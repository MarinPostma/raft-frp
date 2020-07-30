mod raft_service;
mod raft_server;
mod raft_node;
mod message;

use std::collections::HashMap;
use std::time::Duration;

use actix_web::{get, web, App, HttpServer, Responder};
use bincode::deserialize;
use crate::message::{Message, Proposal, RaftResponse};
use log::info;
use raft_node::RaftNode;
use raft_server::RaftServer;
use raft_service::ConfigChange;
use raft_service::raft_service_client::RaftServiceClient;
use structopt::StructOpt;
use tokio::sync::{oneshot, mpsc};
use tokio::time::delay_for;
use tonic::Request;

#[derive(Debug, StructOpt)]
struct Options {
    #[structopt(long)]
    raft_addr: String,
    #[structopt(long)]
    peer_addr: Option<String>,
    #[structopt(long)]
    web_server: Option<String>,
}

#[get("/put/{id}/{name}")]
async fn put(sender: web::Data<mpsc::Sender<Message>>, path: web::Path<(u64, String)>) -> impl Responder {
    info!("received put request ({}, {})", path.0, path.1);
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

#[get("/remove/{id}")]
async fn remove(sender: web::Data<mpsc::Sender<Message>>, path: web::Path<u64>) -> impl Responder {
    let proposal = Proposal::Remove { key: path.into_inner() };
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

    // create rpc server and run it
    let raft_server = RaftServer::new(tx.clone(), options.raft_addr.clone());

    let store: HashMap<u64, String> = HashMap::new();

    let server_handle = tokio::spawn(raft_server.run());

    let local = tokio::task::LocalSet::new();
    let sys = actix_rt::System::run_in_tokio("server", &local);
    // if node is started without a peer addr, this is the first node of the cluster and it is
    // started as the leader.
    let mut node = match options.peer_addr {
        Some(host) => {
            // add peer to node's peers
            let mut host = host.to_string();
            loop {
                let mut client = RaftServiceClient::connect(host.clone()).await?;
                let join_request = JoinRequest {
                    addr: format!("http://{}", options.raft_addr),
                };
                let request = Request::new(join_request);

                // get added peer and attempt to join it's cluster
                let response = client.join(request).await?.into_inner();
                let raft_response: RaftResponse  = deserialize(&response.inner).unwrap();
                match raft_response {
                    RaftResponse::JoinSuccess { assigned_id, peer_addrs } => {
                        info!("joined successfully, got id {}", assigned_id);
                        let mut node = RaftNode::new(rx, store, assigned_id);
                        for (id, addr) in peer_addrs {
                            let _ = node.add_peer(&addr, id).await;
                        }
                        break node;
                    }
                    RaftResponse::WrongLeader { leader_addr, .. } => {
                        host = leader_addr;
                        info!("Wrong leader, try again with leader at {}", host);
                    }
                    RaftResponse::Error => {
                        info!("there was an error joining the cluster");
                        delay_for(Duration::from_millis(1000)).await;
                    }
                    _ => unreachable!()
                }
            }
        }
        None => RaftNode::new(rx, store, 1),
    };

    // add current node to it's peers, needed to comunicate it to other nodes when they join
    let _ = node.add_peer(&format!("http://{}", options.raft_addr), node.id()).await.unwrap();

    let node_handle = tokio::spawn(node.run());

    // server for testing purpose
    if let Some(addr) = options.web_server {
        let http_handle = tokio::spawn(
            HttpServer::new(move || {
                App::new()
                    .app_data(web::Data::new(tx.clone()))
                    .service(put)
                    .service(remove)
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
