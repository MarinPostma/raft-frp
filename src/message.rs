use raft::eraftpb::{Message as RaftMessage, ConfChange};
use serde::{Serialize, Deserialize};
use std::net::SocketAddr;
use tokio::sync::oneshot::Sender;

#[derive(Serialize, Deserialize)]
pub struct RaftClusterInfo {
    // if reponse contains Some(leader_id), then the request was made to the wrong leader
    // and it must be redirected to leader_id
    leader_id: Option<u64>,
    addrs: Vec<SocketAddr>,
}

#[allow(dead_code)]
pub enum Message {
    Propose {
        seq: u64,
        msg_type: u64,
        msg: String,
        callback: Box<dyn Fn() + Send + 'static> 
    },
    ConfigChange {
        seq: u64,
        change: ConfChange,
        chan: Sender<RaftClusterInfo>,
    },
    Raft(RaftMessage),
}
