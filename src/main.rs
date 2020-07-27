#![feature(async_closure)]

mod raft_service;
mod raft_server;
mod raft_node;
mod message;

use log::info;
use raft::eraftpb::{EntryType, Entry};
use std::collections::HashMap;
use std::time::Duration;
use structopt::StructOpt;
use tokio::sync::mpsc::{self, error::TryRecvError};
use tokio::time::interval;
use raft_server::RaftServer;
use raft_node::RaftNode;

type State = HashMap<u64, String>;

#[derive(Debug, StructOpt)]
struct Options {
    #[structopt(long)]
    raft_addr: Option<String>,
    #[structopt(long)]
    peer_addr: Option<String>,
    #[structopt(long)]
    node_id: u64,
}

#[tokio::main]
#[allow(irrefutable_let_patterns)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // init the logger

    env_logger::init();

    // init state
    let mut state = HashMap::new();

    let options = Options::from_args();
    // Select some defaults, then change what we need.
    let id = options.node_id;

    // if node is started without a peer addr, this is the first node of the cluster and it is
    // started as the leader.
    let (tx, rx) = mpsc::channel(100);


    // create rpc server and run it
    let raft_server = RaftServer::new(tx, "[::]:9000");
    raft_server.run();

    let mut node = RaftNode::new(rx, id);
    if options.peer_addr.is_none() {
        node.raft.become_candidate();
        node.raft.become_leader();
    }

    // this can't be done, othewise we only process one event at a time.
    // need make a timeout based tick
    let mut interval = interval(Duration::from_millis(100));
    while let _ = interval.tick().await {
        match node.rcv.try_recv() {
            Ok(_conf_change) => {
            },
            Err(TryRecvError::Empty) => (),
            Err(TryRecvError::Closed) => break,
        }

        //info!("tick");
        node.tick();

        on_ready(&mut node, &mut state);
    }

    Ok(())
}

fn on_ready(node: &mut RaftNode, state: &mut State) {
    if !node.has_ready() {
        return;
    }

    let mut ready = node.ready();

    if !raft::is_empty_snap(ready.snapshot()) {
        info!("there is snapshot");
    }

    if !ready.entries().is_empty() {
        let entries = ready.entries();
        info!("there are entries: {:?}", entries);
        node.mut_store().wl().append(ready.entries()).unwrap();
    }

    if let Some(hs) = ready.hs() {
        // Raft HardState changed, and we need to persist it.
        node.mut_store().wl().set_hardstate(hs.clone());
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
                EntryType::EntryNormal => handle_normal(&entry, state),
                EntryType::EntryConfChange => (),
            }
        }
    }
    node.advance(ready);
}

fn handle_normal(entry: &Entry, state: &mut State) {
    use std::convert::TryInto;
    let entry_type = u64::from_be_bytes(entry.get_context()[..8].try_into().expect(""));
    match entry_type {
        0 => {
            let k = u64::from_be_bytes(entry.get_data()[..8].try_into().expect(""));
            let v = String::from_utf8(entry.get_data()[8..].to_vec()).unwrap();
            info!("commiting ({}, {}) to state", k, v);
            state.insert(k, v);
        }
        _ => unimplemented!()
    }
}
