use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use crate::message::{Message, RaftResponse};
use crate::raft_service::raft_service_server::{RaftServiceServer, RaftService};
use crate::raft_service::{self, ConfigChange, Empty, Proposal};

use log::{error, info};
use raft::eraftpb::{ConfChange, Message as RaftMessage};
use tokio::sync::oneshot;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tonic::transport::Server;
use tonic::{Status, Response, Request};
use bincode::serialize;
use protobuf::Message as _;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct RaftServer<M: Send + Sync + 'static> {
    snd: mpsc::Sender<Message<M>>,
    addr: SocketAddr,
    seq: AtomicU64,
}

impl<M: Send + Sync> RaftServer<M> {
    pub fn new<A: ToSocketAddrs>(snd: mpsc::Sender<Message<M>>, addr: A) -> Self {
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
    }
}

#[tonic::async_trait]
impl<M: Send + Sync> RaftService for RaftServer<M> {
    async fn request_id(&self, _req: Request<Empty>) -> Result<Response<raft_service::IdRequestReponse>, Status> {
        unimplemented!()
    }

    async fn propose(&self, _req: Request<Proposal>) -> Result<Response<raft_service::RaftResponse>, Status> {
        unimplemented!()
    }

    async fn change_config(&self, req: Request<ConfigChange>) -> Result<Response<raft_service::RaftResponse>, Status> {

        let mut change = ConfChange::default();
        change.merge_from_bytes(&req.into_inner().inner).unwrap();

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
}
