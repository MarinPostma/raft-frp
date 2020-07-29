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
        chan: Sender<RaftResponse>,
    },
    ConfigChange {
        seq: u64,
        change: ConfChange,
        chan: Sender<RaftResponse>,
    },
    Raft(RaftMessage),
}
