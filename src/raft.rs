use crate::message::Message;
use crate::raft_node::RaftNode;
use crate::raft_server::RaftServer;
use crate::raft_service::raft_service_client::RaftServiceClient;
use crate::raft_service::ConfigChange;
use crate::raft_service::{Empty, ResultCode};
use anyhow::{anyhow, Result};
use bincode::deserialize;
use protobuf::Message as _;
use raft::eraftpb::{ConfChange, ConfChangeType};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tonic::Request;
use bincode::serialize;
use log::info;

use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Serialize, Deserialize)]
pub struct Leader {
    id: u64,
    addr: String,
}

pub trait Store: Send + Sync {
    type Message: Serialize + DeserializeOwned + Send + Sync;
    type Error: std::error::Error;

    fn apply(&mut self, message: Self::Message) -> Result<(), Self::Error>;
}

#[derive(Debug, Serialize, Deserialize)]
pub enum MyMessage {
    Insert { key: u64, value: String },
}

#[derive(Debug)]
pub struct RaftError;

impl fmt::Display for RaftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "mega error")
    }
}

impl std::error::Error for RaftError {}

impl Store for HashMap<u64, String> {
    type Message = MyMessage;
    type Error = RaftError;

    fn apply(&mut self, message: Self::Message) -> Result<(), Self::Error> {
        match message {
            MyMessage::Insert { key, value } => {
                info!("inserting: ({}, {})", key, value);
                self.insert(key, value);
            }
        }
        Ok(())
    }
}

pub async fn run<S: Store + 'static>(
    raft_addr: String,
    store: Arc<RwLock<S>>,
    tx: mpsc::Sender<Message<S::Message>>,
    rx: mpsc::Receiver<Message<S::Message>>,
) -> Result<()> {
    // TODO: arbitrary buffer length, may need to tune this?
    let node = RaftNode::new_leader(rx, tx.clone(), store);
    let server = RaftServer::new(tx, raft_addr);
    let server_handle = tokio::spawn(server.run());
    let node_handle = tokio::spawn(node.run());
    tokio::try_join!(server_handle, node_handle)?;

    Ok(())
}

pub async fn join<S: Store + 'static>(
    raft_addr: &str,
    peer_addr: &str,
    store: Arc<RwLock<S>>,
    tx: mpsc::Sender<Message<S::Message>>,
    rx: mpsc::Receiver<Message<S::Message>>,
) -> Result<()> {
    // 1. try to discover the leader and obtain an id from it.
    let mut leader_addr = peer_addr.to_string();
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
    let mut node = RaftNode::new_follower(rx, tx.clone(), node_id, store);
    node.add_peer(&leader_addr, leader_id).await?;
    let mut client = node.peer_mut(leader_id).unwrap().clone();
    let server = RaftServer::new(tx, raft_addr);
    let server_handle = tokio::spawn(server.run());
    let node_handle = tokio::spawn(node.run());

    // 3. Join the cluster
    // TODO: handle leader change
    let mut change = ConfChange::default();
    change.set_node_id(node_id);
    change.set_change_type(ConfChangeType::AddNode);
    change.set_context(serialize(&raft_addr)?);
    client
        .change_config(Request::new(ConfigChange {
            inner: change.write_to_bytes()?,
        }))
        .await?;
    tokio::try_join!(server_handle, node_handle)?;

    Ok(())
}
