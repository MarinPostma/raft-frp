use std::sync::{RwLock, Arc, RwLockWriteGuard, RwLockReadGuard};
use std::fs;
use std::path::Path;
use heed::{PolyDatabase, Env, Database};
use heed::types::*;
use raftrs::prelude::*;
use heed_traits::{BytesDecode, BytesEncode};
use std::borrow::Cow;
use protobuf::Message;
use log::{warn, info};


type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Sync + Send>>;

const SNAPSHOT_KEY: &str = "snapshot";
const LAST_INDEX_KEY: &str = "last_index";
const HARD_STATE_KEY: &str = "hard_state";
const CONF_STATE_KEY: &str = "conf_state";

struct HeedSnapshot;

impl<'a> BytesEncode<'a> for HeedSnapshot{
    type EItem = Snapshot;
    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        let bytes = item.write_to_bytes().ok()?;
        Some(Cow::Owned(bytes))
    }
}

impl<'a> BytesDecode<'a> for HeedSnapshot{
    type DItem = Snapshot;
    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let mut snapshot= Snapshot::default();
        snapshot.merge_from_bytes(bytes).ok()?;
        Some(snapshot)
    }
}

struct HeedEntry;

impl<'a> BytesEncode<'a> for HeedEntry {
    type EItem = Entry;
    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        let bytes = item.write_to_bytes().ok()?;
        Some(Cow::Owned(bytes))
    }
}

impl<'a> BytesDecode<'a> for HeedEntry {
    type DItem = Entry;
    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let mut entry = Entry::default();
        entry.merge_from_bytes(bytes).ok()?;
        Some(entry)
    }
}

struct HeedHardState;

impl<'a> BytesEncode<'a> for HeedHardState {
    type EItem = HardState;
    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        Some(Cow::Owned(item.write_to_bytes().ok()?))
    }
}

impl<'a> BytesDecode<'a> for HeedHardState {
    type DItem = HardState;
    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let mut hard_state = HardState::default();
        hard_state.merge_from_bytes(bytes).ok();
        Some(hard_state)
    }
}

struct HeedConfState;

impl<'a> BytesEncode<'a> for HeedConfState {
    type EItem = ConfState;
    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        Some(Cow::Owned(item.write_to_bytes().ok()?))
    }
}

impl<'a> BytesDecode<'a> for HeedConfState {
    type DItem = ConfState;
    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let mut conf_state = ConfState::default();
        conf_state.merge_from_bytes(bytes).ok();
        Some(conf_state)
    }
}

pub struct HeedStorageCore {
    env: Env,
    entries_db: Database<OwnedType<u64>, HeedEntry>,
    metadata_db: PolyDatabase,
}

impl HeedStorageCore {
    pub fn create(path: impl AsRef<Path>, id: u64) -> Result<Self> {

        let path = path.as_ref();
        let name = format!("raft-{}.mdb", id);

        fs::create_dir_all(Path::new(&path).join(&name))?;

        let path = path.join(&name);

        let env = heed::EnvOpenOptions::new()
            .map_size(100 * 4096)
            .max_dbs(3000)
            .open(path)?;
        let entries_db: Database<OwnedType<u64>, HeedEntry> = env.create_database(Some("entries"))?;
        println!("hereeee");
        let metadata_db = env.create_poly_database(Some("meta"))?;

        let hard_state = HardState::new();
        let conf_state = ConfState::new();

        let mut storage = Self {
            metadata_db,
            entries_db,
            env,
        };

        storage.set_hard_state(&hard_state)?;
        storage.set_conf_state(&conf_state)?;
        storage.append(&[Entry::new()])?;

        Ok(storage)
    }

    pub fn append(&mut self, entries: &[Entry]) -> Result<()> {
        info!("adding entries");
        let mut writer = self.env.write_txn()?;
        let mut last_index = self.last_index(&writer)?;
        for entry in entries {
            //assert_eq!(entry.get_index(), last_index + 1);
            let index = entry.index;
            last_index = std::cmp::max(index, last_index);
            self.entries_db.put(&mut writer, &index, entry)?;
        }
        self.set_last_index(&mut writer, last_index)?;
        writer.commit()?;
        let reader = self.env.read_txn()?;
        self.last_index(&reader)?;
        Ok(())
    }

    pub fn set_hard_state(&mut self, hard_state: &HardState) -> Result<()> {
        let mut writer = self.env.write_txn()?;
        self.metadata_db.put::<_, Str, HeedHardState>(&mut writer, HARD_STATE_KEY, hard_state)?;
        writer.commit()?;
        Ok(())
    }

    pub fn hard_state(&self) -> Result<HardState> {
        let reader = self.env.read_txn()?;
        let hard_state = self.metadata_db.get::<_, Str, HeedHardState>(&reader, HARD_STATE_KEY)?;
        Ok(hard_state.expect("missing hard_state"))
    }

    pub fn set_conf_state(&mut self, conf_state: &ConfState) -> Result<()> {
        let mut writer = self.env.write_txn()?;
        self.metadata_db.put::<_, Str, HeedConfState>(&mut writer, CONF_STATE_KEY, conf_state)?;
        writer.commit()?;
        Ok(())
    }

    pub fn conf_state(&self) -> Result<ConfState> {
        let reader = self.env.read_txn()?;
        let conf_state = self.metadata_db.get::<_, Str, HeedConfState>(&reader, CONF_STATE_KEY)?;
        Ok(conf_state.expect("there should be a conf state"))
    }

    /// attempts to create a snapshot with the lastest commited entry
    pub fn create_snapshot(&mut self, data: Vec<u8>) -> Result<()> {
        let hard_state = self.hard_state()?;
        let conf_state = self.conf_state()?;
        let mut snapshot = Snapshot::new();
        snapshot.set_data(data);
        let meta = snapshot.mut_metadata();
        meta.set_conf_state(conf_state);
        meta.index = hard_state.commit;
        meta.term = hard_state.term;
        self.set_snapshot(&snapshot)?;
        Ok(())
    }

    fn set_snapshot(&mut self, snapshot: &Snapshot) -> Result<()> {
        let mut writer = self.env.write_txn()?;
        self.metadata_db.put::<_, Str, HeedSnapshot>(&mut writer, SNAPSHOT_KEY, snapshot)?;
        writer.commit()?;
        Ok(())
    }

    pub fn snapshot(&self) -> Result<Option<Snapshot>> {
        let reader = self.env.read_txn()?;
        let snapshot = self.metadata_db.get::<_, Str, HeedSnapshot>(&reader, SNAPSHOT_KEY)?;
        Ok(snapshot)
    }

    pub fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<()> {
        let metadata = snapshot.get_metadata();
        let conf_state = metadata.get_conf_state();
        let mut hard_state = self.hard_state()?;
        hard_state.set_term(metadata.term);
        hard_state.set_commit(metadata.index);
        // TODO: make this operation atomic
        self.set_hard_state(&hard_state)?;
        self.set_conf_state(conf_state)?;
        let mut writer = self.env.write_txn()?;
        self.set_last_index(&mut writer, metadata.index)?;
        writer.commit()?;
        Ok(())
    }

    pub fn compact(&mut self, index: u64) -> Result<()> {
        let mut writer = self.env.write_txn()?;
        //let last_index = self.last_index(&writer)?;
        // there should always be at least one entry in the log
        //assert!(last_index > index + 1);
        self.entries_db.delete_range(&mut writer, &(..index))?;
        writer.commit()?;
        Ok(())
    }

    fn last_index(&self, r: &heed::RoTxn) -> Result<u64> {
        let last_index = self
            .metadata_db.get::<_, Str, OwnedType<u64>>(r, LAST_INDEX_KEY)?
            .unwrap_or(0);
        println!("last index requested: {}", last_index);
        Ok(last_index)
    }

    fn set_last_index(&self, w: &mut heed::RwTxn, index: u64) -> Result<()> {
        println!("setting last index: {}", index);
        self.metadata_db.put::<_, Str, OwnedType<u64>>(w, LAST_INDEX_KEY, &index)?;
        Ok(())
    }

    fn first_index(&self, r: &heed::RoTxn) -> Result<u64> {
        let first_entry = self.entries_db.first(r)?.expect("There should always be at least one entry in the db");
        Ok(first_entry.0 + 1)
    }

    fn entry(&self, index: u64) -> Result<Option<Entry>> {
        let reader = self.env.read_txn()?;
        let entry = self.entries_db.get(&reader, &index)?;
        Ok(entry)
    }

    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>> {
        info!("entries requested: {}->{}", low, high);
        println!("entries requested: {}->{}", low, high);
        let reader = self.env.read_txn()?;
        let iter = self.entries_db.range(&reader, &(low..high))?;
        let max_size: Option<u64> = max_size.into();
        let mut size_count = 0;
        let entries = iter
            .filter_map(|e| {
                println!("blablabla: {:?}", e);
                match e {
                Ok((_, e)) => Some(e),
                _ => None,
            }})
            .take_while(|entry| {
                match max_size {
                    Some(max_size) => {
                        size_count += entry.compute_size() as u64;
                        if size_count < max_size {
                            true
                        } else {
                            false
                        }
                    }
                    None => true,
                }
            })
            .collect();
        Ok(entries)
    }
}

pub struct HeedStorage(Arc<RwLock<HeedStorageCore>>);

impl HeedStorage {
    pub fn create(path:  impl AsRef<Path>, id: u64) -> Result<Self> {
        let core = HeedStorageCore::create(path, id)?;
        Ok(Self(Arc::new(RwLock::new(core))))
    }

    pub fn wl(&mut self) -> RwLockWriteGuard<HeedStorageCore> {
        self.0.write().unwrap()
    }

    pub fn rl(&self) -> RwLockReadGuard<HeedStorageCore> {
        self.0.read().unwrap()
    }
}

impl Storage for HeedStorage {
    fn initial_state(&self) -> raftrs::Result<RaftState> {
        let store = self.rl();
        let mut raft_state = RaftState::default();
        raft_state.hard_state = store.hard_state()
            .map_err(|e| raftrs::Error::Store(raftrs::StorageError::Other(e)))?;
        raft_state.conf_state = store.conf_state()
            .map_err(|e| raftrs::Error::Store(raftrs::StorageError::Other(e)))?;
        warn!("raft_state: {:#?}", raft_state);
        Ok(raft_state)
    }

    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> raftrs::Result<Vec<Entry>> {
        let store = self.rl();
        let entries = store.entries(low, high, max_size)
            .map_err(|e| raftrs::Error::Store(raftrs::StorageError::Other(e)))?;
        Ok(entries)
    }

    fn term(&self, idx: u64) -> raftrs::Result<u64> {
        let store = self.rl();
        let first_index = self.first_index()?;
        let last_index = self.last_index()?;
        let hard_state = store.hard_state()
            .map_err(|e| raftrs::Error::Store(raftrs::StorageError::Other(e)))?;
        if idx == hard_state.commit {
            return Ok(hard_state.term)
        }

        if idx < first_index {
            return Err(raftrs::Error::Store(raftrs::StorageError::Compacted));
        }

        if idx > last_index {
            return Err(raftrs::Error::Store(raftrs::StorageError::Unavailable));
        }

        let entry = store.entry(idx)
            .map_err(|e| raftrs::Error::Store(raftrs::StorageError::Other(e)))?;
        Ok(entry.map(|e| e.term).unwrap_or(0))
    }

    fn first_index(&self) -> raftrs::Result<u64> {
        let store = self.rl();
        let reader = store.env.read_txn().unwrap();
        store.first_index(&reader)
            .map_err(|e| raftrs::Error::Store(raftrs::StorageError::Other(e)))
    }

    fn last_index(&self) -> raftrs::Result<u64> {
        let store = self.rl();
        let reader = store.env.read_txn().unwrap();
        let last_index = store.last_index(&reader)
            .map_err(|e| raftrs::Error::Store(raftrs::StorageError::Other(e)))?;
        println!("last index asked: {}", last_index);
        Ok(last_index)
    }

    fn snapshot(&self, index: u64) -> raftrs::Result<Snapshot> {
        println!("requested index {}", index);
        let store = self.rl();
        match store.snapshot() {
            Ok(Some(snapshot)) => Ok(snapshot),
            _ => Err(raftrs::Error::Store(raftrs::StorageError::SnapshotTemporarilyUnavailable))
        }
    }
}
