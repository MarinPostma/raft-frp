use crate::message::{Message, RaftResponse};
use crate::raft_node::RaftNode;
use crate::raft_server::RaftServer;
use crate::raft_service::raft_service_client::RaftServiceClient;
use crate::raft_service::ConfigChange;
use crate::raft_service::{Empty, ResultCode};
use crate::RaftError;

use anyhow::{anyhow, Result};
use bincode::deserialize;
use protobuf::Message as _;
use raftrs::eraftpb::{ConfChange, ConfChangeType};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tonic::Request;
use bincode::serialize;
use log::warn;

use std::fmt;

#[derive(Debug, Serialize, Deserialize)]
pub struct Leader {
    id: u64,
    addr: String,
}

pub trait Store {
    type Error: Sync + Send + std::error::Error;

    fn apply(&mut self, message: &[u8]) -> Result<Vec<u8>, Self::Error>;
    fn snapshot(&self) -> Vec<u8>;
    fn restore(&mut self, snapshot: &[u8]) -> Result<(), Self::Error>;
}

impl fmt::Display for RaftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "mega error")
    }
}

impl std::error::Error for RaftError {}

/// A mailbox to send messages to a ruung raft node.
#[derive(Clone)]
pub struct Mailbox(mpsc::Sender<Message>);

impl Mailbox {
    /// sends a proposal message to commit to the node. This fails if the current node is not the
    /// leader
    pub async fn send(&self, message: Vec<u8>) -> Result<Vec<u8>, RaftError> {
        let (tx, rx) = oneshot::channel();
        let proposal = Message::Propose { proposal: message, chan: tx };
        let mut sender = self.0.clone();
        match sender.send(proposal).await {
            Ok(_) => {
                match rx.await {
                    Ok(RaftResponse::Response { data }) => Ok(data),
                    _ => Err(RaftError),
                }
            }
            _ => Err(RaftError),
        }
    }

    pub async fn leave(&self) -> Result<(), RaftError> {
        let mut change = ConfChange::default();
        // set node id to 0, the node will set it to self when it receives it.
        change.set_node_id(0);
        change.set_change_type(ConfChangeType::RemoveNode);
        let mut sender= self.0.clone();
        let (chan, rx) = oneshot::channel();
        match sender.send(Message::ConfigChange { change, chan }).await {
            Ok(_) => {
                match rx.await {
                    Ok(RaftResponse::Ok) => Ok(()),
                    _ => Err(RaftError),
                }
            }
            _ => Err(RaftError),
        }
    }
}

pub struct Raft<S: Store + 'static> {
    store: S,
    tx: mpsc::Sender<Message>,
    rx: mpsc::Receiver<Message>,
    addr: String,
}

impl<S: Store + Send + Sync + 'static> Raft<S> {
    /// creates a new node with the given address and store.
    pub fn new(addr: String, store: S) -> Self {
        let (tx, rx) = mpsc::channel(100);
        Self { store, tx, rx, addr }
    }

    /// gets the node's `Mailbox`.
    pub fn mailbox(&self) -> Mailbox {
        Mailbox(self.tx.clone())
    }

    /// Create a new leader for the cluster, with id 1. There has to be exactly one node in the
    /// cluster that is initialised that way
    pub async fn lead(self) -> Result<()> {
        let addr = self.addr.clone();
        let node = RaftNode::new_leader(self.rx, self.tx.clone(), self.store);
        let server = RaftServer::new(self.tx, addr);
        let _server_handle = tokio::spawn(server.run());
        let node_handle = tokio::spawn(node.run());
        tokio::try_join!(node_handle)?;
        warn!("leaving leader node");

        Ok(())
    }

    /// Tries to join a new cluster at `addr`, getting an id from the leader, or finding it if
    /// `addr` is not the current leader of the cluster
    pub async fn join(self, addr: String) -> Result<()> {
        // 1. try to discover the leader and obtain an id from it.
        let mut leader_addr = addr.to_string();
        let (leader_id, node_id): (u64, u64) = loop {
            let mut client = RaftServiceClient::connect(format!("http://{}", leader_addr)).await?;
            let response = client
                .request_id(Request::new(Empty::default()))
                .await?
                .into_inner();
            match response.code() {
                ResultCode::WrongLeader => {
                    let leader: Leader = deserialize(&response.data)?;
                    leader_addr = leader.addr;
                    continue;
                }
                ResultCode::Ok => break deserialize(&response.data)?,
                ResultCode::Error => {
                    return Err(anyhow!(
                            "join error: {}",
                            deserialize::<String>(&response.data)?
                    ))
                }
            }
        };

        // 2. run server and node to prepare for joining
        let addr = self.addr.clone();
        let mut node = RaftNode::new_follower(self.rx, self.tx.clone(), node_id, self.store);
        node.add_peer(&leader_addr, leader_id).await?;
        let mut client = node.peer_mut(leader_id).unwrap().clone();
        let server = RaftServer::new(self.tx, addr);
        let _server_handle = tokio::spawn(server.run());
        let node_handle = tokio::spawn(node.run());

        // 3. Join the cluster
        // TODO: handle wrong leader
        let mut change = ConfChange::default();
        change.set_node_id(node_id);
        change.set_change_type(ConfChangeType::AddNode);
        change.set_context(serialize(&self.addr)?);
        client
            .change_config(Request::new(ConfigChange {
                inner: change.write_to_bytes()?,
            }))
        .await?;
        tokio::try_join!(node_handle)?;

        Ok(())
    }
}
