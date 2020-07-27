mod raft_service;
mod raft_server;
mod raft_node;
mod message;

use log::info;
use raft_node::RaftNode;
use raft_server::RaftServer;
use raft_service::JoinRequest;
use raft_service::ResultCode;
use structopt::StructOpt;
use tokio::sync::mpsc;
use tonic::Request;

#[derive(Debug, StructOpt)]
struct Options {
    #[structopt(long)]
    raft_addr: String,
    #[structopt(long)]
    node_id: u64,
    #[structopt(long)]
    peer_addr: Option<String>,
    #[structopt(long)]
    peer_id: Option<u64>,

}

#[tokio::main]
#[allow(irrefutable_let_patterns)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // init the logger
    env_logger::init();

    let options = Options::from_args();
    // Select some defaults, then change what we need.
    let id = options.node_id;

    let (tx, rx) = mpsc::channel(100);

    // create rpc server and run it
    let raft_server = RaftServer::new(tx, options.raft_addr);

    // TODO: setup leader election on timeout
    let mut node = RaftNode::new(rx, id);

    // if node is started without a peer addr, this is the first node of the cluster and it is
    // started as the leader.
    match options.peer_addr {
        Some(host) => {

            // add peer to node's peers
            let peer_id = options.peer_id.unwrap();
            node.add_peer(&host, peer_id).await?;
            let join_request = JoinRequest {
                id: node.id(),
                host,
            };
            let request = Request::new(join_request);
            info!("created client");

            // get added peer and attempt to join it's cluster
            let client = node.peer_mut(peer_id).unwrap();
            let response = client.join(request).await?.into_inner();
            match response.code() {
                ResultCode::Ok => {
                    info!("joined successfully");
                }
                ResultCode::WrongLeader => {
                    info!("Wrong leader, try again");
                }
                ResultCode::Error => {
                    info!("there was an error joining the cluster");
                }
            }
        }
        None => {
            info!("starting leader node");
            node.raft.become_candidate();
            node.raft.become_leader();
        }
    }

    let server_handle = tokio::spawn(raft_server.run());
    let node_handle = tokio::spawn(node.run());

    tokio::try_join!(node_handle, server_handle)?;

    Ok(())
}
