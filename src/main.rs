#![feature(async_closure)]

mod raft_service;
mod raft_server;
mod raft_node;
mod message;

use structopt::StructOpt;
use tokio::sync::mpsc;
use raft_server::RaftServer;
use raft_node::RaftNode;

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

    let options = Options::from_args();
    // Select some defaults, then change what we need.
    let id = options.node_id;

    // if node is started without a peer addr, this is the first node of the cluster and it is
    // started as the leader.
    let (tx, rx) = mpsc::channel(100);

    // create rpc server and run it
    let raft_server = RaftServer::new(tx, "[::]:9000");

    // TODO: setup leader election on timeout
    let mut node = RaftNode::new(rx, id);
    if options.peer_addr.is_none() {
        node.raft.become_candidate();
        node.raft.become_leader();
    }

    let server_handle = tokio::spawn(raft_server.run());
    let node_handle = tokio::spawn(node.run());

    tokio::try_join!(node_handle, server_handle)?;

    Ok(())
}
