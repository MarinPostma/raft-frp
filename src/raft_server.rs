use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use crate::message::{Message, RaftResponse};
use crate::raft_service::raft_service_server::{RaftServiceServer, RaftService};
use crate::raft_service::{self, ConfigChange, Empty };

use log::{error, info};
use raftrs::eraftpb::{ConfChange, Message as RaftMessage};
use tokio::sync::oneshot;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tonic::transport::Server;
use tonic::{Status, Response, Request};
use bincode::serialize;
use protobuf::Message as _;

pub struct RaftServer {
    snd: mpsc::Sender<Message>,
    addr: SocketAddr,
}

impl RaftServer {
    pub fn new<A: ToSocketAddrs>(snd: mpsc::Sender<Message>, addr: A) -> Self {
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        RaftServer { snd, addr }
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
impl RaftService for RaftServer {
    async fn request_id(&self, _: Request<Empty>) -> Result<Response<raft_service::IdRequestReponse>, Status> {
        let mut sender = self.snd.clone();
        let (tx, rx) = oneshot::channel();
        let _ = sender.send(Message::RequestId { chan: tx }).await;
        let id = rx.await.unwrap();
        Ok(Response::new(raft_service::IdRequestReponse {
            code: raft_service::ResultCode::Ok as i32,
            data: serialize(&(1u64, id)).unwrap(),
        }))
    }

    async fn change_config(&self, req: Request<ConfigChange>) -> Result<Response<raft_service::RaftResponse>, Status> {

        let mut change = ConfChange::default();
        change.merge_from_bytes(&req.into_inner().inner).unwrap();

        let mut sender = self.snd.clone();

        let (tx, rx) = oneshot::channel();

        let message = Message::ConfigChange {
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
