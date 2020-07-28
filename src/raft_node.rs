use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::time::Duration;

use crate::message::{Message, RaftClusterInfo, Proposal};

use protobuf::Message as PMessage;
use bincode::{serialize, deserialize};
use log::{info, debug};
use raft::eraftpb::{Entry, EntryType, ConfChange, ConfChangeType};
use raft::{raw_node::RawNode, storage::MemStorage, Config};
use tokio::sync::mpsc::{error::TryRecvError, Receiver};
use tokio::sync::oneshot;
use tokio::time::interval;
use crate::raft_service::raft_service_client::RaftServiceClient;
use crate::raft_service;
use tonic::transport::channel::Channel;
use tonic::Request;

pub struct Peer {
    addr: String,
    client: RaftServiceClient<Channel>,
}

impl Deref for Peer {
    type Target = RaftServiceClient<Channel>;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl DerefMut for Peer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

impl Peer {
    pub async fn new(addr: &str) -> Result<Peer, tonic::transport::Error> {
        // TODO: clean up this mess
        let client = RaftServiceClient::connect(addr.to_string()).await?;
        info!("NODE: connected to {}", addr);
        let addr = addr.to_string();
        Ok(Peer { addr, client })
    }
}

pub struct RaftNode {
    inner: RawNode<MemStorage>,
    pub peers: HashMap<u64, Peer>,
    pub rcv: Receiver<Message>,
    pub store: HashMap<u64, String>,
}

impl RaftNode {
    pub fn new(rcv: Receiver<Message>, id: u64, store: HashMap<u64, String>) -> Self {
        let config = Config {
            id,
            peers: vec![id],
            ..Default::default()
        };
        let storage = MemStorage::default();

        config.validate().unwrap();

        let inner = RawNode::new(&config, storage, vec![]).unwrap();

        let peers = HashMap::new();

        RaftNode { inner, rcv, peers, store }
    }

    pub fn peer_mut(&mut self, id: u64) -> Option<&mut Peer> {
        self.peers.get_mut(&id)
    }

    pub fn is_leader(&self) -> bool {
        self.inner.raft.leader_id == self.inner.raft.id
    }

    pub fn id(&self) -> u64 {
        self.raft.id
    }

    pub async fn add_peer(&mut self, addr: &str, id: u64) -> Result<(), tonic::transport::Error> {
        let peer = Peer::new(addr).await?;
        self.peers.insert(id, peer);
        Ok(())
    }

    fn leader(&self) -> u64 {
        self.raft.leader_id
    }

    fn peer_addrs(&self) -> Vec<String> {
        self.peers.values().map(|Peer {ref addr, ..}| addr.to_string()).collect()
    }

    #[allow(irrefutable_let_patterns)]
    pub async fn run(mut self) {
        let mut interval = interval(Duration::from_millis(100));

        // A map to contain sender to client responses
        let mut client_send = HashMap::new();

        while let _ = interval.tick().await {
            match self.rcv.try_recv() {
                Ok(Message::ConfigChange { chan, seq, change }) => {
                    info!("NODE: conf change requested");
                    if !self.is_leader() {
                        // wrong leader send client cluster data
                        let leader_id = Some(self.leader());
                        let addrs = self.peer_addrs();
                        let cluster_info = RaftClusterInfo { leader_id, addrs };
                        chan.send(cluster_info).unwrap();
                    } else {
                        client_send.insert(seq, chan);
                        self.propose_conf_change(serialize(&seq).unwrap(), change)
                            .unwrap();
                    }
                }
                Ok(Message::Raft(m)) => {
                    debug!("raft message: to={} from={}", self.raft.id, m.from);
                    if let Ok(_a) = self.step(m) {};
                }
                Ok(Message::Propose {seq, proposal, chan}) => {
                    if !self.is_leader() {
                        // wrong leader send client cluster data
                        let leader_id = Some(self.leader());
                        let addrs = self.peer_addrs();
                        let cluster_info = RaftClusterInfo { leader_id, addrs };
                        chan.send(cluster_info).unwrap();
                    } else {
                        debug!("NODE: received proposal: {:?}", proposal);
                        client_send.insert(seq, chan);
                        let proposal = serialize(&proposal).unwrap();
                        let seq = serialize(&seq).unwrap();
                        self.propose(seq, proposal).unwrap();
                    }
                }
                Err(TryRecvError::Empty) => (),
                Err(TryRecvError::Closed) => break,
            }

            //info!("tick");
            self.tick();

            self.on_ready(&mut client_send).await;
        }
    }

    async fn on_ready(&mut self, client_send: &mut HashMap<u64, oneshot::Sender<RaftClusterInfo>>) {
        if !self.has_ready() {
            return;
        }

        let mut ready = self.ready();

        if self.is_leader() {
            let messages = ready.messages.drain(..);
            for message in messages {
                let mut client = match self.peer_mut(message.get_to()) {
                    Some(ref peer) => peer.client.clone(),
                    None => continue,
                };
                let message_request = Request::new(raft_service::Message { inner: message.write_to_bytes().unwrap() });
                client.send_message(message_request).await.unwrap();
                info!("NODE(leader): sent message");
            }
        }

        if !raft::is_empty_snap(ready.snapshot()) {
            info!("there is snapshot");
        }

        if !ready.entries().is_empty() {
            let entries = ready.entries();
            info!("there are entries: {:?}", entries);
            self.mut_store().wl().append(ready.entries()).unwrap();
        }

        if let Some(hs) = ready.hs() {
            // Raft HardState changed, and we need to persist it.
            self.mut_store().wl().set_hardstate(hs.clone());
        }

        if !self.is_leader() {
            let messages = ready.messages.drain(..);
            for message in messages {
                let mut client = match self.peer_mut(message.get_to()) {
                    Some(ref peer) => peer.client.clone(),
                    None => continue,
                };
                let message_request = Request::new(raft_service::Message { inner: message.write_to_bytes().unwrap() });
                client.send_message(message_request).await.unwrap();
                info!("NODE: sent message");
            }
        }

        if let Some(committed_entries) = ready.committed_entries.take() {
            let mut _last_apply_index = 0;
            for entry in committed_entries {
                // Mostly, you need to save the last apply index to resume applying
                // after restart. Here we just ignore this because we use a Memory storage.
                _last_apply_index = entry.get_index();

                if entry.get_data().is_empty() {
                    // Emtpy entry, when the peer becomes Leader it will send an empty entry.
                    continue;
                }

                match entry.get_entry_type() {
                    EntryType::EntryNormal => self.handle_normal(&entry),
                    EntryType::EntryConfChange => self.handle_config_change(&entry, client_send).await,
                }
            }
        }
        self.advance(ready);
    }

    async fn handle_config_change(&mut self, entry: &Entry, senders: &mut HashMap<u64, oneshot::Sender<RaftClusterInfo>>) {
        info!("handling config change");
        let seq: u64 = deserialize(entry.get_context()).unwrap();
        let mut change = ConfChange::new();
        // we do this to deserialize the conf. Very ugly, gotta find something better
        change.merge_from_bytes(entry.get_data()).unwrap();
        let id = change.get_node_id();

        println!("confchange: {:?}", change);

        match change.get_change_type() {
            ConfChangeType::AddNode => {
                let addr: String = deserialize(change.get_context()).unwrap();
                self.add_peer(&addr, id).await.unwrap();
                info!("NODE: added {} ({}) to peers", addr, id);
            }
            _ => unimplemented!()
        }

        self.apply_conf_change(&change).unwrap();

        match senders.remove(&seq) {
            Some(sender) => {
                let cluster_info = RaftClusterInfo { leader_id: None, addrs: self.peer_addrs() };
                sender.send(cluster_info).unwrap();
            }
            None => (),
        }
    }

    fn handle_normal(&mut self, entry: &Entry) {
        let seq: u64 = deserialize(&entry.get_context()).unwrap(); 
        let proposal: Proposal = deserialize(&entry.get_data()).unwrap();
        debug!("NODE: commited entry ({}): {:?}", seq, proposal);
        match proposal {
            Proposal::Put { key, value } => {
                self.store.insert(key, value);
            }
        }
        println!("current state: {:?}", self.store);
    }
}

impl Deref for RaftNode {
    type Target = RawNode<MemStorage>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for RaftNode {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
