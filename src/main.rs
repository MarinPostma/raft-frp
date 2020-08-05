mod raft_service;
mod raft_server;
mod raft_node;
mod message;
mod raft;

use std::sync::Arc;
use std::collections::HashMap;
use structopt::StructOpt;
use actix_web::{get, web, App, HttpServer, Responder};
use crate::raft::{Raft, Mailbox, Store, RaftError};
use serde::{Serialize, Deserialize};
use log::info;

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
    Insert { key: u64, value: String }
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
}

#[get("/put/{id}/{name}")]
async fn put(mailbox: web::Data<Mailbox<Message>>, path: web::Path<(u64, String)>) -> impl Responder {
    let message = Message::Insert { key: path.0, value: path.1.clone() };
    mailbox.send(message).await.unwrap();
    "OK".to_string()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let options = Options::from_args();
    let store = HashMap::new();

    // setup runtime for actix
    let local = tokio::task::LocalSet::new();
    let sys = actix_rt::System::run_in_tokio("server", &local);

    match options.peer_addr {
        Some(addr) => {
            Raft::new(options.raft_addr, store)
                .join(&addr)
                .await?;
        }
        None => {
            let raft = Raft::new(options.raft_addr, store);
            let mailbox = Arc::new(raft.mailbox());
            let raft = tokio::spawn(raft.lead());
            let http_addr = options.web_server.clone().unwrap();
            let server = tokio::spawn(async move {
                HttpServer::new(move || {
                    App::new()
                        .app_data(web::Data::new(mailbox.clone()))
                        .service(put)
                })
                .bind(http_addr)
                    .unwrap()
                    .run()
            });
            let (res1, _res2) = tokio::try_join!(raft, server)?;
            res1?;
        }
    }

    sys.await?;
    Ok(())
}
