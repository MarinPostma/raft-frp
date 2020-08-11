use std::sync::{RwLock, Arc, RwLockWriteGuard, RwLockReadGuard};
use std::fs;
use std::path::Path;
use heed::{PolyDatabase, Env, Database};
use heed::types::*;
use raftrs::prelude::*;
use heed_traits::{BytesDecode, BytesEncode};
use std::borrow::Cow;
use protobuf::Message;

struct HeedSnapshotMetadata;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Sync + Send>>;

const SNAPSHOT_KEY: &str = "snapshot_metadata";
const LAST_INDEX_KEY: &str = "last_index";
const HARD_STATE_KEY: &str = "hard_state";
const CONF_STATE_KEY: &str = "conf_state";

impl<'a> BytesEncode<'a> for HeedSnapshotMetadata {
    type EItem = SnapshotMetadata;
    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        let bytes = item.write_to_bytes().ok()?;
        Some(Cow::Owned(bytes))
    }
}

impl<'a> BytesDecode<'a> for HeedSnapshotMetadata {
    type DItem = SnapshotMetadata;
    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        let mut snapshot_meta = SnapshotMetadata::default();
        snapshot_meta.merge_from_bytes(bytes).ok()?;
        Some(snapshot_meta)
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

#[allow(dead_code)]
pub struct HeedStorageCore {
    env: Env,
    entries_db: Database<OwnedType<u64>, HeedEntry>,
    metadata_db: PolyDatabase,
    snapshot: Option<Snapshot>,
}

impl HeedStorageCore {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {

        let path = path.as_ref();

        fs::create_dir_all(Path::new(&path).join("raft.mdb"))?;

        let path = path.join("raft.mdb");

        let env = heed::EnvOpenOptions::new()
            .map_size(10 * 1096 * 1096)
            .max_dbs(3000)
            .open(path)?;
        let entries_db: Database<OwnedType<u64>, HeedEntry> = env.create_database(None)?;
        let metadata_db = env.create_poly_database(None)?;

        let hard_state = HardState::new();
        let conf_state = ConfState::new();
        let snapshot_metadata = SnapshotMetadata::default();


        let mut wtxn = env.write_txn()?;
        metadata_db.put::<_, Str, HeedSnapshotMetadata>(&mut wtxn, SNAPSHOT_KEY, &snapshot_metadata)?;
        metadata_db.put::<_, Str, HeedHardState>(&mut wtxn, HARD_STATE_KEY, &hard_state)?;
        metadata_db.put::<_, Str, HeedConfState>(&mut wtxn, CONF_STATE_KEY, &conf_state)?;
        metadata_db.put::<_, Str, OwnedType<u64>>(&mut wtxn, LAST_INDEX_KEY, &0)?;

        wtxn.commit()?;

        let mut storage = Self {
            metadata_db,
            entries_db,
            env,
            snapshot: None,
        };

        storage.append(&[Entry::new()])?;

        Ok(storage)
    }

    pub fn append(&mut self, entries: &[Entry]) -> Result<()> {
        let mut writer = self.env.write_txn()?;
        let mut last_index = self.last_index(&writer)?;
        for entry in entries {
            assert_eq!(entry.get_index(), last_index + 1);
            last_index += 1;
            self.entries_db.put(&mut writer, &last_index, entry)?;
        }
        self.set_last_index(&mut writer, last_index)?;
        writer.commit()?;
        Ok(())
    }

    pub fn set_hard_state(&mut self, _hard_state: HardState) -> Result<()> {
        todo!()
    }

    pub fn set_conf_state(&mut self, _conf_state: ConfState) -> Result<()> {
        todo!()
    }

    pub fn create_snapshot(&mut self, _last_applied: u64,_data: Vec<u8>) -> Result<()> {
        todo!()
    }

    pub fn apply_snapshot(&mut self, _snapshot: Snapshot) -> Result<()> {
        todo!()
    }

    pub fn compact(&mut self, _index: u64) -> Result<()> {
        todo!()
    }

    fn last_index(&self, r: &heed::RoTxn) -> Result<u64> {
        let last_index = self
            .metadata_db.get::<_, Str, OwnedType<u64>>(r, LAST_INDEX_KEY)?
            .expect("Last index should always exist.");
        Ok(last_index)
    }

    fn set_last_index(&self, w: &mut heed::RwTxn, index: u64) -> Result<()> {
        self.metadata_db.put::<_, Str, OwnedType<u64>>(w, LAST_INDEX_KEY, &index)?;
        Ok(())
    }

    fn first_index(&self, r: &heed::RoTxn) -> Result<u64> {
        let first_entry = self.entries_db.first(r)?.expect("There should always be at least one entry in the db");
        Ok(first_entry.0)
    }
}

pub struct HeedStorage(Arc<RwLock<HeedStorageCore>>);

impl HeedStorage {
    pub fn create(path:  impl AsRef<Path>) -> Result<Self> {
        let core = HeedStorageCore::create(path)?;
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
        todo!()
    }

    fn entries(&self, _low: u64, _high: u64, _max_size: impl Into<Option<u64>>) -> raftrs::Result<Vec<Entry>> {
        todo!()
    }

    fn term(&self, _idx: u64) -> raftrs::Result<u64> {
        todo!()
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
        store.last_index(&reader)
            .map_err(|e| raftrs::Error::Store(raftrs::StorageError::Other(e)))
    }

    fn snapshot(&self, index: u64) -> raftrs::Result<Snapshot> {
        println!("requested index: {}", index);
        todo!()
    }
}
