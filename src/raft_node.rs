use raft::{raw_node::RawNode, storage::MemStorage, Config};
use tokio::sync::mpsc::Receiver;
use crate::message::Message;
use std::ops::{Deref, DerefMut};

pub struct RaftNode {
    inner: RawNode<MemStorage>,
    pub rcv: Receiver<Message>,
}

impl RaftNode {
    pub fn new(rcv: Receiver<Message>, id: u64) -> RaftNode {
        let config = Config {
            id,
            peers: vec![id],
            ..Default::default()
        };
        let storage = MemStorage::default();

        config.validate().unwrap();

        let inner = RawNode::new(&config, storage, vec![]).unwrap();
        RaftNode {
            inner,
            rcv,
        }
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
