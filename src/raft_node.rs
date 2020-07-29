use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::time::{Duration, Instant};

use crate::message::{Message, RaftClusterInfo, Proposal};

use protobuf::Message as PMessage;
use bincode::{serialize, deserialize};
use log::{info, debug};
use raft::eraftpb::{Entry, EntryType, ConfChange, ConfChangeType};
use raft::{raw_node::RawNode, storage::MemStorage, Config};
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use tokio::time::timeout;
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
    pub fn new(rcv: Receiver<Message>, store: HashMap<u64, String>, id: u64) -> Self {
        let config = Config {
            id,
            peers: vec![id],
            election_tick: 10,
            // Heartbeat tick is for how long the leader needs to send
            // a heartbeat to keep alive.
            heartbeat_tick: 3,
            // The max size limits the max size of each appended message. Mostly, 1 MB is enough.
            max_size_per_msg: 1024 * 1024 * 1024,
            // Max inflight msgs that the leader sends messages to follower without
            // receiving ACKs.
            max_inflight_msgs: 256,
            // The Raft applied index.
            // You need to save your applied index when you apply the committed Raft logs.
            applied: 0,
            // Just for log
            tag: format!("[{}]", 1),
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

    fn peer_addrs(&self) -> HashMap<u64, String> {
        self
            .peers
            .iter()
            .map(|(&id, Peer {ref addr, ..})| (id, addr.to_string()))
            .collect()
    }

    #[allow(irrefutable_let_patterns)]
    pub async fn run(mut self) {
        let mut heartbeat = Duration::from_millis(100);
        let mut now = Instant::now();

        // A map to contain sender to client responses
        let mut client_send = HashMap::new();

        loop {
            match timeout(heartbeat, self.rcv.recv()).await {
                Ok(Some(Message::ConfigChange { chan, seq, change })) => {
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
                Ok(Some(Message::Raft(m))) => {
                    debug!("raft message: to={} from={}", self.raft.id, m.from);
                    if let Ok(_a) = self.step(m) {};
                }
                Ok(Some(Message::Propose {seq, proposal, chan})) => {
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
                Ok(_) => unreachable!(),
                Err(_) => (),
            }

            //info!("tick");
            let elapsed = now.elapsed();
            now = Instant::now();
            if elapsed > heartbeat {
                heartbeat = Duration::from_millis(100);
                self.tick();
            } else {
                heartbeat -= elapsed;
            }

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
                debug!("NODE(leader): sent message");
            }
        }

        if !raft::is_empty_snap(ready.snapshot()) {
            info!("there is snapshot");
        }

        if !ready.entries().is_empty() {
            let entries = ready.entries();
            debug!("there are entries: {:?}", entries);
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
                debug!("NODE: sent message");
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
                    EntryType::EntryNormal => self.handle_normal(&entry, client_send),
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

        let _ = self.apply_conf_change(&change);

        match senders.remove(&seq) {
            Some(sender) => {
                let cluster_info = RaftClusterInfo { leader_id: None, addrs: self.peer_addrs() };
                sender.send(cluster_info).unwrap();
            }
            None => (),
        }
    }

    fn handle_normal(&mut self, entry: &Entry, senders: &mut HashMap<u64, oneshot::Sender<RaftClusterInfo>>) {
        let seq: u64 = deserialize(&entry.get_context()).unwrap(); 
        let proposal: Proposal = deserialize(&entry.get_data()).unwrap();
        debug!("NODE: commited entry ({}): {:?}", seq, proposal);
        match proposal {
            Proposal::Put { key, value } => {
                self.store.insert(key, value);
            }
            Proposal::Remove { ref key } => {
                self.store.remove(key);
            }
        }
        println!("current store: {:?}", self.store);
        if let Some(_sender) = senders.remove(&seq) { /* drop channel for now */ }
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
