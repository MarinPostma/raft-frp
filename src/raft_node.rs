use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::message::{Message, RaftResponse};
use crate::raft::Store;

use crate::storage::HeedStorage;
use crate::raft_service;
use crate::raft_service::raft_service_client::RaftServiceClient;
use bincode::{deserialize, serialize};
use log::{debug, error, info, warn};
use protobuf::Message as PMessage;
use raftrs::eraftpb::{ConfChange, ConfChangeType, Entry, EntryType};
use raftrs::{raw_node::RawNode, Config};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tonic::transport::channel::Channel;
use tonic::Request;

struct MessageSender {
    message: Vec<u8>,
    client: RaftServiceClient<tonic::transport::channel::Channel>,
    client_id: u64,
    chan: mpsc::Sender<Message>,
    max_retries: usize,
    timeout: Duration,
}

impl MessageSender {
    async fn send(mut self) {
        let mut current_retry = 0usize;

        loop {
            let message_request = Request::new(raft_service::Message {
                inner: self.message.clone(),
            });
            match self.client.send_message(message_request).await {
                Ok(_) => return,
                Err(_) => {
                    if current_retry < self.max_retries {
                        current_retry += 1;
                        tokio::time::delay_for(self.timeout).await;
                    } else {
                        let _ = self
                            .chan
                            .send(Message::ReportUnreachable {
                                node_id: self.client_id,
                            })
                            .await;
                        return;
                    }
                }
            }
        }
    }
}

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
        let client = RaftServiceClient::connect(format!("http://{}", addr)).await?;
        info!("NODE: connected to {}", addr);
        let addr = addr.to_string();
        Ok(Peer { addr, client })
    }
}

pub struct RaftNode<S: Store> {
    inner: RawNode<HeedStorage>,
    // the peer is optional, because an id can be reserved and later populated
    pub peers: HashMap<u64, Option<Peer>>,
    pub rcv: mpsc::Receiver<Message>,
    pub snd: mpsc::Sender<Message>,
    store: S,
    should_quit: bool,
    seq: AtomicU64,
    last_snap_time: Instant,
}

impl<S: Store + 'static> RaftNode<S> {
    pub fn new_leader(
        rcv: mpsc::Receiver<Message>,
        snd: mpsc::Sender<Message>,
        store: S,
        ) -> Self {
        let config = Config {
            id: 1,
            election_tick: 10,
            // Heartbeat tick is for how long the leader needs to send
            // a heartbeat to keep alive.
            heartbeat_tick: 3,
            // Just for log
            ..Default::default()
        };

        config.validate().unwrap();

        let storage = HeedStorage::create(".").unwrap();
        let inner = RawNode::new(&config, storage).unwrap();
        let peers = HashMap::new();
        let seq = AtomicU64::new(0);
        let last_snap_time = Instant::now();

        RaftNode {
            inner,
            rcv,
            peers,
            store,
            seq,
            snd,
            should_quit: false,
            last_snap_time,
        }
    }

    pub fn new_follower(
        rcv: mpsc::Receiver<Message>,
        snd: mpsc::Sender<Message>,
        id: u64,
        store: S,
    ) -> Self {
        let config = Config {
            id,
            election_tick: 10,
            // Heartbeat tick is for how long the leader needs to send
            // a heartbeat to keep alive.
            heartbeat_tick: 3,
            // Just for log
            ..Default::default()
        };

        config.validate().unwrap();

        let storage = HeedStorage::create(".").unwrap();
        let inner = RawNode::new(&config, storage).unwrap();
        let peers = HashMap::new();
        let seq = AtomicU64::new(0);
        let last_snap_time = Instant::now()
            .checked_sub(Duration::from_secs(1000))
            .unwrap();

        RaftNode {
            inner,
            rcv,
            peers,
            store,
            seq,
            snd,
            should_quit: false,
            last_snap_time,
        }
    }

    pub fn peer_mut(&mut self, id: u64) -> Option<&mut Peer> {
        match self.peers.get_mut(&id) {
            None => None,
            Some(v) => v.as_mut(),
        }
    }

    pub fn is_leader(&self) -> bool {
        self.inner.raft.leader_id == self.inner.raft.id
    }

    pub fn id(&self) -> u64 {
        self.raft.id
    }

    pub async fn add_peer(&mut self, addr: &str, id: u64) -> Result<(), tonic::transport::Error> {
        let peer = Peer::new(addr).await?;
        self.peers.insert(id, Some(peer));
        Ok(())
    }

    fn leader(&self) -> u64 {
        self.raft.leader_id
    }

    fn peer_addrs(&self) -> HashMap<u64, String> {
        self.peers
            .iter()
            .filter_map(|(&id, peer)| {
                peer.as_ref()
                    .map(|Peer { addr, .. }| (id, addr.to_string()))
            })
            .collect()
    }

    // reserve a slot to insert node on next node addition commit
    fn reserve_next_peer_id(&mut self) -> u64 {
        let next_id = self.peers.keys().max().cloned().unwrap_or(1);
        // if assigned id is ourself, return next one
        let next_id = std::cmp::max(next_id + 1, self.id());
        self.peers.insert(next_id, None);
        info!("reserving id {}", next_id);
        next_id
    }

    pub async fn run(mut self) {
        let mut heartbeat = Duration::from_millis(100);
        let mut now = Instant::now();

        // A map to contain sender to client responses
        let mut client_send = HashMap::new();

        loop {
            if self.should_quit {
                return;
            }
            match timeout(heartbeat, self.rcv.recv()).await {
                Ok(Some(Message::ConfigChange { chan, mut change })) => {
                    // whenever a change id is 0, it's a message to self.
                    if change.get_node_id() == 0 {
                        change.set_node_id(self.id());
                    }

                    if !self.is_leader() {
                        // wrong leader send client cluster data
                        let leader_id = self.leader();
                        // leader can't be an empty node
                        let leader_addr = self.peers[&leader_id].as_ref().unwrap().addr.clone();
                        let raft_response = RaftResponse::WrongLeader {
                            leader_id,
                            leader_addr,
                        };
                        chan.send(raft_response).unwrap();
                    } else {
                        // leader assign new id to peer
                        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
                        client_send.insert(seq, chan);
                        self.propose_conf_change(serialize(&seq).unwrap(), change)
                            .unwrap();
                    }
                }
                Ok(Some(Message::Raft(m))) => {
                    debug!("raft message: to={} from={}", self.raft.id, m.from);
                    if let Ok(_a) = self.step(m) {};
                }
                Ok(Some(Message::Propose { proposal, chan })) => {
                    if !self.is_leader() {
                        // wrong leader send client cluster data
                        let leader_id = self.leader();
                        // leader can't be an empty node
                        let leader_addr = self.peers[&leader_id].as_ref().unwrap().addr.clone();
                        let raft_response = RaftResponse::WrongLeader {
                            leader_id,
                            leader_addr,
                        };
                        chan.send(raft_response).unwrap();
                    } else {
                        info!("leader received proposal");
                        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
                        client_send.insert(seq, chan);
                        let seq = serialize(&seq).unwrap();
                        self.propose(seq, proposal).unwrap();
                    }
                }
                Ok(Some(Message::RequestId { chan })) => {
                    let id = self.reserve_next_peer_id();
                    chan.send(id).unwrap();
                }
                Ok(Some(Message::ReportUnreachable { node_id })) => {
                    self.report_unreachable(node_id);
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

    async fn on_ready(&mut self, client_send: &mut HashMap<u64, oneshot::Sender<RaftResponse>>) {
        if !self.has_ready() {
            return;
        }

        let mut ready = self.ready();

        if !ready.entries().is_empty() {
            let entries = ready.entries();
            let mut store = self.mut_store().wl();
            store.append(entries).unwrap();
        }

        if let Some(hs) = ready.hs() {
            // Raft HardState changed, and we need to persist it.
            let mut store = self.mut_store().wl();
            store.set_hard_state(hs.clone()).unwrap();
        }

        for message in ready.messages.drain(..) {
            let client = match self.peer_mut(message.get_to()) {
                Some(ref peer) => peer.client.clone(),
                None => continue,
            };
            let message_sender = MessageSender {
                client_id: message.get_to(),
                client: client.clone(),
                chan: self.snd.clone(),
                message: message.write_to_bytes().unwrap(),
                timeout: Duration::from_millis(100),
                max_retries: 5,
            };
            tokio::spawn(message_sender.send());
        }

        if !raftrs::raw_node::is_empty_snap(ready.snapshot()) {
            let snapshot = ready.snapshot();
            self.store.restore(snapshot.get_data()).unwrap();
            let mut store = self.mut_store().wl();
            store.apply_snapshot(snapshot.clone()).unwrap();
        }

        if let Some(hs) = ready.hs() {
            // Raft HardState changed, and we need to persist it.
            let mut store = self.mut_store().wl();
            store.set_hard_state(hs.clone()).unwrap();
        }

        if let Some(committed_entries) = ready.committed_entries.take() {
            let mut _last_apply_index = 0;
            for entry in &committed_entries {
                // Mostly, you need to save the last apply index to resume applying
                // after restart. Here we just ignore this because we use a Memory storage.
                _last_apply_index = entry.get_index();

                if entry.get_data().is_empty() {
                    // Emtpy entry, when the peer becomes Leader it will send an empty entry.
                    continue;
                }

                match entry.get_entry_type() {
                    EntryType::EntryNormal => self.handle_normal(&entry, client_send),
                    EntryType::EntryConfChange => {
                        self.handle_config_change(&entry, client_send).await
                    }
                }
            }
        }
        self.advance(ready);
    }

    async fn handle_config_change(
        &mut self,
        entry: &Entry,
        senders: &mut HashMap<u64, oneshot::Sender<RaftResponse>>,
    ) {
        info!("handling config change");
        let seq: u64 = deserialize(entry.get_context()).unwrap();
        let mut change = ConfChange::new();
        // we do this to deserialize the conf. Very ugly, gotta find something better
        change.merge_from_bytes(entry.get_data()).unwrap();
        let id = change.get_node_id();

        let change_type = change.get_change_type();

        match change_type {
            ConfChangeType::AddNode => {
                let addr: String = deserialize(change.get_context()).unwrap();
                self.add_peer(&addr, id).await.unwrap();
                info!("NODE: added {} ({}) to peers", addr, id);
            }
            ConfChangeType::RemoveNode => {
                if change.get_node_id() == self.id() {
                    self.should_quit = true;
                    warn!("quiting the cluster");
                } else {
                    self.peers.remove(&change.get_node_id());
                }
            }
            _ => unimplemented!(),
        }

        if let Ok(cs) = self.apply_conf_change(&change) {
            let last_applied = self.raft.raft_log.applied;
            let snapshot = self.store.snapshot();
            {
                let mut store = self.mut_store().wl();
                store.set_conf_state(cs.clone()).unwrap();
                store.compact(last_applied).unwrap();
                let _ = store.create_snapshot(last_applied, snapshot);
            }
        }

        match senders.remove(&seq) {
            Some(sender) => {
                let response = match change_type {
                    ConfChangeType::AddNode => RaftResponse::JoinSuccess {
                        assigned_id: id,
                        peer_addrs: self.peer_addrs(),
                    },
                    ConfChangeType::RemoveNode => RaftResponse::Ok,
                    _ => unimplemented!(),
                };
                match sender.send(response) {
                    Err(_) => error!("error sending response"),
                    _ => (),
                }
            }
            None => (),
        }
    }

    fn handle_normal(
        &mut self,
        entry: &Entry,
        senders: &mut HashMap<u64, oneshot::Sender<RaftResponse>>,
    ) {
        let seq: u64 = deserialize(&entry.get_context()).unwrap();
        let data = self.store.apply(entry.get_data()).unwrap();
        if let Some(sender) = senders.remove(&seq) {
            sender.send(RaftResponse::Response { data }).unwrap();
        }

        if Instant::now() > self.last_snap_time + Duration::from_secs(15) {
            warn!("creating backup");
            self.last_snap_time = Instant::now();
            let last_applied = self.raft.raft_log.applied;
            let snapshot = self.store.snapshot();
            let mut store = self.mut_store().wl();
            store.compact(last_applied).unwrap();
            let _ = store.create_snapshot(last_applied, snapshot);
        }
    }
}

impl<S: Store> Deref for RaftNode<S> {
    type Target = RawNode<HeedStorage>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S: Store> DerefMut for RaftNode<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
