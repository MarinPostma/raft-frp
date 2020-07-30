use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use crate::message::{Message, Proposal, RaftResponse};
use crate::raft_service::raft_service_server::{RaftServiceServer, RaftService};
use crate::raft_service::{self, ConfigChange};

use log::{error, info, warn};
use raft::eraftpb::{ConfChange, ConfChangeType, Message as RaftMessage};
use tokio::sync::oneshot;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tonic::transport::Server;
use tonic::{Status, Response, Request};
use bincode::serialize;
use protobuf::Message as _;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct RaftServer {
    snd: mpsc::Sender<Message>,
    addr: SocketAddr,
    seq: AtomicU64,
}

impl RaftServer {
    pub fn new<A: ToSocketAddrs>(snd: mpsc::Sender<Message>, addr: A) -> Self {
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        let seq = AtomicU64::new(0);
        RaftServer { snd, addr, seq }
    }

    pub async fn run(self) {
        let addr = self.addr.clone();
        info!("listening gRPC requests on: {}", addr);
        let svc = RaftServiceServer::new(self);
        Server::builder()
            .add_service(svc)
            .serve(addr)
            .await
            .expect("error running server");
        warn!("got here");
    }
}

#[tonic::async_trait]
impl RaftService for RaftServer {
    async fn change_config(&self, req: Request<ConfigChange>) -> Result<Response<raft_service::RaftResponse>, Status> {
        let mut change = ConfChange::default();
        change.merge_from_bytes(&req.into_inner().inner);
        // intially, a new peer address is 0, a real peer adress will be assigned later by the
        // leader
        change.set_id(0);
        change.set_context(serialize(&addr).unwrap());
        change.set_node_id(0);
        change.set_change_type(ConfChangeType::AddNode);
        info!("Request add: {:?}", change);

        let mut sender = self.snd.clone();

        let (tx, rx) = oneshot::channel();

        let seq = self.seq.fetch_add(1, Ordering::Relaxed);

        let message = Message::ConfigChange {
            seq,
            change,
            chan: tx,
        };

        match sender.send(message).await {
            Ok(_) => (),
            Err(_) => error!("send error"),
        }

        let mut reply = raft_service::RaftResponse::default();

        // if we don't receive a response after 2secs, we timeout
        match timeout(Duration::from_secs(2), rx).await {
            Ok(Ok(raft_response)) => {
                reply.inner = serialize(&raft_response).expect("serialize error");
            },
            Ok(_) => (),
            Err(_e) => {
                reply.inner = serialize(&RaftResponse::Error).unwrap();
                error!("timeout waiting for reply");
            },
        }

        Ok(Response::new(reply))
    }

    async fn send_message(&self, request: Request<raft_service::Message>) -> Result<Response<raft_service::RaftResponse>, Status> {
        let request = request.into_inner();
        // again this ugly shit to serialize the message
        let mut message = RaftMessage::default();
        message.merge_from_bytes(&request.inner).unwrap();
        let mut sender = self.snd.clone();
        match sender.send(Message::Raft(message)).await {
            Ok(_) => (),
            Err(_) => error!("send error"),
        }

        let response = RaftResponse::Ok;
        Ok(Response::new(raft_service::RaftResponse { inner: serialize(&response).unwrap() }))
    }

    async fn put(&self, request: Request<raft_service::Entry>) -> Result<Response<raft_service::RaftResponse>, Status> {
        let raft_service::Entry { key, value } = request.into_inner();
        let (tx, rx) = oneshot::channel();
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);

        let message = Message::Propose {
            seq,
            proposal: Proposal::Put {key, value},
            chan: tx,
        };

        match self.snd.clone().send(message).await {
            Ok(_) => (),
            Err(_) => error!("send error"),
        }

        let _ = rx.await;

        let response = RaftResponse::Ok;
        Ok(Response::new(raft_service::RaftResponse { inner: serialize(&response).unwrap() }))
    }
}
