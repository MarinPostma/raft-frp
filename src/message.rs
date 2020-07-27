use raft::eraftpb::{Message as RaftMessage, ConfChange};
use serde::{Serialize, Deserialize};
use std::net::SocketAddr;
use tokio::sync::mpsc::Sender;

#[derive(Serialize, Deserialize)]
pub struct RaftClusterInfo {
    leader_id: u64,
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
