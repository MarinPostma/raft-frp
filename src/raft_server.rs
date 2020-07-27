use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use crate::message::Message;
use crate::raft_service::raft_service_server::{RaftServiceServer, RaftService};
use crate::raft_service::{JoinRequest, ResultReply};

use log::info;
use raft::eraftpb::{ConfChange, ConfChangeType};
use tokio::sync::mpsc::{Sender, self};
use tokio::time::timeout;
use tonic::transport::Server;
use tonic::{Status, Response, Request};

pub struct RaftServer {
    snd: Sender<Message>,
    addr: SocketAddr,
}

impl RaftServer {
    pub fn new<A: ToSocketAddrs>(snd: Sender<Message>, addr: A) -> Self {
        let addr = addr.to_socket_addrs().unwrap().next().unwrap();
        RaftServer { snd, addr }
    }

    pub fn run(self) {
        tokio::spawn(async {
            let addr = self.addr.clone();
            let svc = RaftServiceServer::new(self);
            Server::builder()
                .add_service(svc)
                .serve(addr)
                .await
                .expect("error running server");
        });
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

        let (tx, mut rx) = mpsc::channel(1);

        let _message = Message::ConfigChange {
            seq: 0,
            change,
            chan: tx,
        };

        match timeout(Duration::from_millis(2000), rx.recv()).await {
            Ok(_cluster_info) => (),
            Err(_e) => (),
        }

        Ok(Response::new(ResultReply::default()))
    }

}
