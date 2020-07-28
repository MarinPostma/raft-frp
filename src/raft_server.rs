use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use crate::message::{Message, Proposal};
use crate::raft_service::raft_service_server::{RaftServiceServer, RaftService};
use crate::raft_service::{self, JoinRequest, ResultReply, ResultCode};

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
        info!("running gRPC server on: {}", addr);
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
    async fn join(&self, req: Request<JoinRequest>) -> Result<Response<ResultReply>, Status> {
        let JoinRequest { host, id } = req.into_inner();
        let mut change = ConfChange::default();
        change.set_id(id);
        change.set_context(serialize(&host).unwrap());
        change.set_node_id(id);
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

        let mut reply = ResultReply::default();

        // if we don't receive a response after 2secs, we timeout
        match timeout(Duration::from_secs(2), rx).await {
            Ok(Ok(cluster_info)) => {
                match cluster_info.leader_id {
                    Some(_id) => reply.set_code(ResultCode::WrongLeader),
                    None => reply.set_code(ResultCode::Ok),
                }
                reply.data = serialize(&cluster_info).expect("serialize error");
            },
            Ok(_) => unreachable!(),
            Err(_e) => {
                reply.set_code(ResultCode::Error);
                error!("timeout waiting for reply");
            },
        }

        Ok(Response::new(reply))
    }

    async fn send_message(&self, request: Request<raft_service::Message>) -> Result<Response<ResultReply>, Status> {
        let request = request.into_inner();
        // again this ugly shit to serialize the message
        let mut message = RaftMessage::default();
        message.merge_from_bytes(&request.inner).unwrap();
        let mut sender = self.snd.clone();
        match sender.send(Message::Raft(message)).await {
            Ok(_) => (),
            Err(_) => error!("send error"),
        }
        Ok(Response::new(ResultReply::default()))
    }

    async fn put(&self, request: Request<raft_service::Entry>) -> Result<Response<ResultReply>, Status> {
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

        Ok(Response::new(ResultReply::default()))
    }
}
