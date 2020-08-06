mod message;
mod raft;
mod raft_node;
mod raft_server;
mod raft_service;

use crate::raft::{Mailbox, Raft, RaftError, Store};
use actix_web::{get, web, App, HttpServer, Responder};
use log::info;
use std::collections::HashMap;
use std::sync::Arc;
use structopt::StructOpt;
use bincode::{serialize, deserialize};
use serde::{Serialize, Deserialize};

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
    Get { key: u64 },
}

impl Store for HashMap<u64, String> {
    type Error = RaftError;

    fn apply(&mut self, message: &[u8]) -> Result<Vec<u8>, Self::Error> {
        let message: Message = deserialize(message).unwrap();
        let message: Vec<u8> = match message {
            Message::Insert { key, value } => {
                info!("inserting: ({}, {})", key, value);
                self.insert(key, value.clone());
                serialize(&value).unwrap()
            }
            Message::Get { ref key } => {
                let value = self.get(key);
                serialize(&value).unwrap()
            }
        };
        Ok(message)
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
    mailbox: web::Data<Arc<Mailbox>>,
    path: web::Path<(u64, String)>,
) -> impl Responder {
    let message = Message::Insert {
        key: path.0,
        value: path.1.clone(),
    };
    let message = serialize(&message).unwrap();
    let result = mailbox.send(message).await.unwrap();
    let result: String = deserialize(&result).unwrap();
    format!("{:?}", result)
}

#[get("/get/{id}")]
async fn get(
    mailbox: web::Data<Arc<Mailbox>>,
    path: web::Path<u64>,
) -> impl Responder {
    let message = Message::Get { key: path.into_inner(), };
    let message = serialize(&message).unwrap();
    let result = mailbox.send(message).await.unwrap();
    let result: Option<String> = deserialize(&result).unwrap();
    format!("{:?}", result)
}

#[get("/leave")]
async fn leave(
    mailbox: web::Data<Arc<Mailbox>>,
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
                        .service(get)
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
