use raft::eraftpb::{Message as RaftMessage, ConfChange};
use serde::{Serialize, Deserialize};
use tokio::sync::oneshot::Sender;

#[derive(Serialize, Deserialize, Debug)]
pub struct RaftClusterInfo {
    // if reponse contains Some(leader_id), then the request was made to the wrong leader
    // and it must be redirected to leader_id
    pub leader_id: Option<u64>,
    pub addrs: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Proposal {
    Put { key: u64, value: String },
    Remove { key: u64 },
}

#[allow(dead_code)]
pub enum Message {
    Propose {
        seq: u64,
        proposal: Proposal,
        chan: Sender<RaftClusterInfo>,
    },
    ConfigChange {
        seq: u64,
        change: ConfChange,
        chan: Sender<RaftClusterInfo>,
    },
    Raft(RaftMessage),
}
