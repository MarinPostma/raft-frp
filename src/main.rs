use raft::{Mailbox, Raft, RaftError, Store};
use actix_web::{get, web, App, HttpServer, Responder};
use log::info;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
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
}

#[derive(Clone)]
struct HashStore(Arc<RwLock<HashMap<u64, String>>>);

impl HashStore {
    fn new() -> Self { Self(Arc::new(RwLock::new(HashMap::new()))) }
    fn get(&self, id: u64) -> Option<String> {
        self.0.read().unwrap().get(&id).cloned()
    }
}

impl Store for HashStore {
    type Error = RaftError;

    fn apply(&mut self, message: &[u8]) -> Result<Vec<u8>, Self::Error> {
        let message: Message = deserialize(message).unwrap();
        let message: Vec<u8> = match message {
            Message::Insert { key, value } => {

                let mut db = self.0.write().unwrap();
                db.insert(key, value.clone());
                info!("inserted: ({}, {})", key, value);
                serialize(&value).unwrap()
            }
        };
        Ok(message)
    }

    fn snapshot(&self) -> Vec<u8> {
        serialize(&self.0.read().unwrap().clone()).unwrap()
    }

    fn restore(&mut self, snapshot: &[u8]) -> Result<(), Self::Error> {
        let new: HashMap<u64, String> = deserialize(snapshot).unwrap();
        let mut db = self.0.write().unwrap();
        let _ = std::mem::replace(&mut *db, new);
        Ok(())
    }
}

#[get("/put/{id}/{name}")]
async fn put(
    data: web::Data<(Arc<Mailbox>, HashStore)>,
    path: web::Path<(u64, String)>,
) -> impl Responder {
    let message = Message::Insert {
        key: path.0,
        value: path.1.clone(),
    };
    let message = serialize(&message).unwrap();
    let result = data.0.send(message).await.unwrap();
    let result: String = deserialize(&result).unwrap();
    format!("{:?}", result)
}

#[get("/get/{id}")]
async fn get(
    data: web::Data<(Arc<Mailbox>, HashStore)>,
    path: web::Path<u64>,
) -> impl Responder {
    let id = path.into_inner();
    
    let response = data.1.get(id);
    format!("{:?}", response)
}

#[get("/leave")]
async fn leave(
    data: web::Data<(Arc<Mailbox>, Arc<RwLock<HashMap<u64, String>>>)>,
) -> impl Responder {
    data.0.leave().await.unwrap();
    "OK".to_string()
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let options = Options::from_args();
    let store = HashStore::new();

    // setup runtime for actix
    let local = tokio::task::LocalSet::new();
    let _sys = actix_rt::System::run_in_tokio("server", &local);

    let raft = Raft::new(options.raft_addr, store.clone());
    let mailbox = Arc::new(raft.mailbox());
    let (raft_handle, mailbox) = match options.peer_addr {
        Some(addr) => {
            info!("running in follower mode");
            let handle = tokio::spawn(raft.join(addr));
            (handle, mailbox)
        }
        None => {
            info!("running in leader mode");
            let handle =  tokio::spawn(raft.lead());
            (handle, mailbox)
        }
    };

    if let Some(addr) = options.web_server {
        let _server = tokio::spawn(
            HttpServer::new(move || {
                App::new()
                    .app_data(web::Data::new((mailbox.clone(), store.clone())))
                    .service(put)
                    .service(get)
                    .service(leave)
            })
            .bind(addr)
            .unwrap()
            .run()
        );
    }

    let result = tokio::try_join!(raft_handle)?;
    result.0?;
    Ok(())
}
