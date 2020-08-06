mod message;
mod raft;
mod raft_node;
mod raft_server;
mod raft_service;

use crate::raft::{Mailbox, Raft, RaftError, Store};
use actix_web::{get, web, App, HttpServer, Responder};
use log::info;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use structopt::StructOpt;
use bincode::{serialize, deserialize};

#[derive(Debug, StructOpt)]
struct Options {
    #[structopt(long)]
    raft_addr: String,
    #[structopt(long)]
    peer_addr: Option<String>,
    #[structopt(long)]
    web_server: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub enum Message {
    Insert { key: u64, value: String },
}

impl Store for HashMap<u64, String> {
    type Message = Message;
    type Error = RaftError;

    fn apply(&mut self, message: Self::Message) -> Result<(), Self::Error> {
        match message {
            Message::Insert { key, value } => {
                info!("inserting: ({}, {})", key, value);
                self.insert(key, value);
            }
        }
        Ok(())
    }

    fn snapshot(&self) -> Vec<u8> {
        serialize(self).unwrap()
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), Self::Error> {
        let new: Self = deserialize(snapshot).unwrap();
        let _ = std::mem::replace(self, new);
        Ok(())
    }
}

#[get("/put/{id}/{name}")]
async fn put(
    mailbox: web::Data<Arc<Mailbox<Message>>>,
    path: web::Path<(u64, String)>,
) -> impl Responder {
    let message = Message::Insert {
        key: path.0,
        value: path.1.clone(),
    };
    mailbox.send(message).await.unwrap();
    "OK".to_string()
}

#[get("/leave")]
async fn leave(
    mailbox: web::Data<Arc<Mailbox<Message>>>,
) -> impl Responder {
    mailbox.leave().await.unwrap();
    "OK".to_string()
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let options = Options::from_args();
    let store = HashMap::new();

    // setup runtime for actix
    let local = tokio::task::LocalSet::new();
    let _sys = actix_rt::System::run_in_tokio("server", &local);

    match options.peer_addr {
        Some(addr) => {
            Raft::new(options.raft_addr, store).join(&addr).await?;
        }
        None => {
            let raft = Raft::new(options.raft_addr, store);
            let mailbox = Arc::new(raft.mailbox());
            let raft = tokio::spawn(raft.lead());
            let http_addr = options.web_server.clone().unwrap();
            let _server = tokio::spawn(
                HttpServer::new(move || {
                    App::new()
                        .app_data(web::Data::new(mailbox.clone()))
                        .service(put)
                        .service(leave)
                })
                .bind(http_addr)
                .unwrap()
                .run()
            );
            let _ = tokio::try_join!(raft)?;
        }
    }

    Ok(())
}
