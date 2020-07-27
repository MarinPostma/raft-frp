use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use crate::message::Message;
use crate::raft_service::raft_service_server::{RaftServiceServer, RaftService};
use crate::raft_service::{JoinRequest, ResultReply, ResultCode};

use log::{error, info, warn};
use raft::eraftpb::{ConfChange, ConfChangeType};
use tokio::sync::oneshot;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tonic::transport::Server;
use tonic::{Status, Response, Request};
use bincode::serialize;

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
        change.set_context(host.as_bytes().to_vec());
        change.set_node_id(id);
        change.set_change_type(ConfChangeType::AddNode);
        info!("Request add: {:?}", change);

        let mut sender = self.snd.clone();

        let (tx, rx) = oneshot::channel();

        let message = Message::ConfigChange {
            seq: 0,
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
                reply.set_code(ResultCode::WrongLeader);
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

}
