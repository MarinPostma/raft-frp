use std::collections::HashMap;

use raft::eraftpb::{Message as RaftMessage, ConfChange};
use serde::{Serialize, Deserialize};
use tokio::sync::oneshot::Sender;

#[derive(Serialize, Deserialize, Debug)]
pub enum RaftResponse {
    WrongLeader { leader_id: u64, leader_addr: String },
    JoinSuccess { assigned_id: u64, peer_addrs: HashMap<u64, String> },
    Error,
    Ok,
}

#[allow(dead_code)]
pub enum Message<P>
where P: Sync + Send {
    Propose {
        seq: u64,
        proposal: P,
        chan: Sender<RaftResponse>,
    },
    ConfigChange {
        seq: u64,
        change: ConfChange,
        chan: Sender<RaftResponse>,
    },
    Raft(RaftMessage),
}
