mod raft_service;
mod raft_server;
mod raft_node;
mod message;

use std::collections::HashMap;

use log::info;
use raft_node::RaftNode;
use raft_server::RaftServer;
use raft_service::JoinRequest;
use raft_service::ResultCode;
use structopt::StructOpt;
use tokio::sync::{oneshot, mpsc};
use tonic::Request;
use actix_web::{get, web, App, HttpServer, Responder};
use crate::message::{Message, Proposal};

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
    #[structopt(long)]
    web_server: Option<String>,
}

#[get("/put/{id}/{name}")]
async fn put(sender: web::Data<mpsc::Sender<Message>>, path: web::Path<(u64, String)>) -> impl Responder {
    let proposal = Proposal::Put { key: path.0, value: path.1.clone() };
    let (tx, rx) = oneshot::channel();
    let message = Message::Propose {
        seq: 100,
        proposal,
        chan: tx,
    };

    let _ = sender
        .as_ref()
        .clone()
        .send(message)
        .await;
    let _ = rx.await;
    format!("OK")
}

#[tokio::main]
#[allow(irrefutable_let_patterns)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let (tx, rx) = mpsc::channel(100);

    // init the logger
    env_logger::init();

    let options = Options::from_args();
    // Select some defaults, then change what we need.
    let id = options.node_id;

    // create rpc server and run it
    let raft_server = RaftServer::new(tx.clone(), options.raft_addr.clone());

    let store: HashMap<u64, String> = HashMap::new();

    // TODO: setup leader election on timeout
    let mut node = RaftNode::new(rx, store, id);

    let server_handle = tokio::spawn(raft_server.run());

    let local = tokio::task::LocalSet::new();
    let sys = actix_rt::System::run_in_tokio("server", &local);
    // if node is started without a peer addr, this is the first node of the cluster and it is
    // started as the leader.
    match options.peer_addr {
        Some(host) => {
            // add peer to node's peers
            println!("nodeid : {}", node.id());
            let peer_id = options.peer_id.unwrap();
            node.add_peer(&host, peer_id).await?;
            let join_request = JoinRequest {
                id: node.id(),
                host: format!("http://{}", options.raft_addr),
            };
            let request = Request::new(join_request);

            // get added peer and attempt to join it's cluster
            let client = node.peer_mut(peer_id).unwrap();
            let response = client.join(request).await?.into_inner();
            info!("created client");
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
        None => ()
    }

    let node_handle = tokio::spawn(node.run());

    if let Some(addr) = options.web_server {
        let http_handle = tokio::spawn(
            HttpServer::new(move || {
                App::new()
                    .app_data(web::Data::new(tx.clone()))
                    .service(put)
            })
            .bind(addr)?
            .run());
            let _ = tokio::try_join!(server_handle, node_handle, http_handle)?;
    } else {
            let _ = tokio::try_join!(server_handle, node_handle)?;
    }

    sys.await?;

    Ok(())
}
