use std::sync::{RwLock, Arc, RwLockReadGuard, RwLockWriteGuard};
use std::fs;
use std::path::Path;
use heed::{PolyDatabase, Env, Database};
use heed::types::*;
use raftrs::prelude::*;
use heed_traits::{BytesDecode, BytesEncode};
use std::borrow::Cow;
use protobuf::Message;

struct HeedSnapshotMetadata;

const SNAPSHOT_KEY: &str = "snapshot_metadata";
const LAST_INDEX_KEY: &str = "last_index";
const RAFT_STATE_KEY: &str = "raft_state";

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

struct HeedRaftState;

impl<'a> BytesEncode<'a> for HeedRaftState {
    type EItem = RaftState;
    fn bytes_encode(item: &'a Self::EItem) -> Option<Cow<'a, [u8]>> {
        let hard_state_bytes = item.hard_state.write_to_bytes().ok()?;
        let conf_state_bytes= item.conf_state.write_to_bytes().ok()?;
        let mut bytes = Vec::with_capacity(hard_state_bytes.len() + conf_state_bytes.len() + std::mem::size_of::<u64>());
        bytes.extend_from_slice(&hard_state_bytes.len().to_be_bytes());
        bytes.extend_from_slice(&hard_state_bytes);
        bytes.extend_from_slice(&conf_state_bytes);
        Some(Cow::Owned(bytes))
    }
}

impl<'a> BytesDecode<'a> for HeedRaftState {
    type DItem = RaftState;
    fn bytes_decode(bytes: &'a [u8]) -> Option<Self::DItem> {
        use std::convert::TryInto;
        let header_size = std::mem::size_of::<u64>();
        let mut hard_state = HardState::default();
        let mut conf_state = ConfState::default();
        let hard_state_size = u64::from_be_bytes(bytes[0..header_size].try_into().ok()?);
        hard_state.merge_from_bytes(&bytes[header_size..header_size + hard_state_size as usize]).ok()?;
        conf_state.merge_from_bytes(&bytes[header_size + hard_state_size as usize..]).ok()?;
        let raft_state = RaftState::new(hard_state, conf_state);
        Some(raft_state)
    }
}

pub struct HeedStorage(Arc<RwLock<HeedStorageCore>>);

pub struct HeedStorageCore {
    env: Env,
    entries_db: Database<OwnedType<u64>, HeedEntry>,
    metadata_db: PolyDatabase,
}

impl HeedStorageCore {
    pub fn create(path: impl AsRef<Path>) -> Result<Self, Box<dyn std::error::Error>> {

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
        let raft_state = RaftState::new(hard_state, conf_state);
        let snapshot_metadata = SnapshotMetadata::default();


        let mut wtxn = env.write_txn()?;
        metadata_db.put::<_, Str, HeedSnapshotMetadata>(&mut wtxn, SNAPSHOT_KEY, &snapshot_metadata)?;
        metadata_db.put::<_, Str, HeedRaftState>(&mut wtxn, RAFT_STATE_KEY, &raft_state)?;
        metadata_db.put::<_, Str, OwnedType<u64>>(&mut wtxn, LAST_INDEX_KEY, &0)?;

        wtxn.commit()?;

        let mut storage = Self {
            metadata_db,
            entries_db,
            env,
        };


        storage.append(&[Entry::new()])?;

        Ok(storage)
    }

    pub fn set_raft_state(&mut self, hs: RaftState) -> Result<(), Box<dyn std::error::Error>> {
        let mut wtxn = self.env.write_txn()?;
        self.metadata_db.put::<_, Str, HeedRaftState>(&mut wtxn, RAFT_STATE_KEY, &hs)?;
        wtxn.commit()?;
        Ok(())
    }

    fn raft_state(&self, rtxn: &heed::RoTxn) -> Result<RaftState, Box<dyn std::error::Error>> {
        let hard_state = self
            .metadata_db
            .get::<_, Str, HeedRaftState>(&rtxn, RAFT_STATE_KEY)?
            .expect("expected hard_state");
        Ok(hard_state)
    }

    fn set_snapshot_metadata(&mut self, snapshot: SnapshotMetadata) -> Result<(), Box<dyn std::error::Error>> {
        let mut wtxn = self.env.write_txn()?;
        self.metadata_db.put::<_, Str, HeedSnapshotMetadata>(&mut wtxn, SNAPSHOT_KEY, &snapshot)?;
        wtxn.commit()?;
        Ok(())
    }

    fn snapshot_metadata(&self) -> Result<SnapshotMetadata, Box<dyn std::error::Error>> {
        let rtxn = self
            .env
            .read_txn()
            .map_err(|_| raftrs::Error::Store(raftrs::StorageError::SnapshotTemporarilyUnavailable))?;
        let snapshot_metadata = self
            .metadata_db
            .get::<_, Str, HeedSnapshotMetadata>(&rtxn, SNAPSHOT_KEY)
            .map_err(|_| raftrs::Error::Store(raftrs::StorageError::SnapshotTemporarilyUnavailable))?
            .expect("there should be a snapshot");
        Ok(snapshot_metadata)
    }

    pub fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<(), Box<dyn std::error::Error>> {
        let mut meta = snapshot.take_metadata();
        let term = meta.term;
        let index = meta.index;

        let mut wtxn = self.env.write_txn()?;
        if self.first_index(&wtxn)? > index {
            return Err(Box::new(raftrs::Error::Store(raftrs::StorageError::SnapshotOutOfDate)));
        }

        self.set_snapshot_metadata(meta.clone())?;

        let mut raft_state = self.raft_state(&wtxn)?;
        raft_state.hard_state.term = term;
        raft_state.hard_state.commit = index;
        raft_state.conf_state = meta.take_conf_state();
        self.set_raft_state(raft_state)?;

        self.entries_db.clear(&mut wtxn)?;

        self.set_last_index(index, &mut wtxn)?;

        wtxn.commit()?;

        // Update conf states.
        Ok(())
    }

    fn set_last_index(&mut self, index: u64, txn: &mut heed::RwTxn) -> Result<(), Box<dyn std::error::Error>> {
        let mut wtxn = self.env.write_txn()?;
        self.metadata_db.put::<_, Str, OwnedType<u64>>(&mut wtxn, LAST_INDEX_KEY, &index)?;
        Ok(())
    }

    fn last_index(&self, rtxn: &heed::RoTxn) -> Result<u64, Box<dyn std::error::Error>> {
        let last_index = self
            .metadata_db
            .get::<_, Str, OwnedType<u64>>(&rtxn, LAST_INDEX_KEY)
            .map_err(|_| raftrs::Error::Store(raftrs::StorageError::SnapshotTemporarilyUnavailable))?
            .expect("expect last index");
        Ok(last_index)
    }

    fn set_entries(&mut self, entries: impl Iterator<Item = Entry>) -> Result<(), Box<dyn std::error::Error>> {
        let mut wtxn = self.env.write_txn()?;
        for entry in entries {
            self.entries_db.put(&mut wtxn, &entry.get_index(), &entry)?;
        }
        wtxn.commit()?;
        Ok(())
    }

    pub fn create_snapshot(
        &mut self,
        _idx: u64,
        _cs: Option<ConfState>,
        _pending_membership_change: Option<ConfChange>,
        _data: Vec<u8>
    ) -> Result<(), Box<dyn std::error::Error>> {
        unimplemented!()
    }

    pub fn compact(&mut self, compact_index: u64) -> Result<(), Box<dyn std::error::Error>> {
        let mut wtxn = self.env.write_txn()?;
        let range = 0..compact_index;
        self.entries_db.delete_range(&mut wtxn, &range)?;
        wtxn.commit()?;
        Ok(())
    }

    pub fn append(&mut self, entries: &[Entry]) -> Result<(), Box<dyn std::error::Error>> {
        let mut wtxn = self.env.write_txn()?;
        let mut last_index = self.last_index(&wtxn)?;
        for entry in entries {
            let index = entry.get_index();
            if index != last_index + 1 {
                panic!("indices should follow")
            }
            last_index = index;
            self.entries_db.put(&mut wtxn, &index, &entry)?;
        }
        self.set_last_index(last_index, &mut wtxn)?;
        wtxn.commit()?;
        Ok(())
    }

    fn first_index(&self, txn: &heed::RoTxn) -> Result<u64, Box<dyn std::error::Error>> {
        let first_entry = self
            .entries_db
            .iter(&txn)
            .map_err(|_| raftrs::Error::Store(raftrs::StorageError::SnapshotTemporarilyUnavailable))?
            .next()
            .transpose()
            .map_err(|_| raftrs::Error::Store(raftrs::StorageError::SnapshotTemporarilyUnavailable))?;
        match first_entry {
            Some((index, _)) => Ok(index),
            None => Ok(0),
        }
    }

    fn get_entry(&self, index: u64, rtxn: &heed::RoTxn) -> Result<Option<Entry>, Box<dyn std::error::Error>> {
        let entry = self.entries_db.get(rtxn, &index)?;
        Ok(entry)
    }

    pub fn read<R, F: FnOnce(&heed::RoTxn) -> R>(&self, f: F) -> Result<R, Box<dyn std::error::Error>> {
        let rtxn = self.env.read_txn()?;
        Ok(f(&rtxn))
    }

    pub fn snapshot(&self) -> Result<Snapshot, Box<dyn std::error::Error>> {
        let mut snapshot = Snapshot::default();
        let rtxn = self.env.read_txn()?;

        // Use the latest applied_idx to construct the snapshot.
        let raft_state = self.raft_state(&rtxn)?;
        let applied_idx = raft_state.hard_state.commit;
        let term = raft_state.hard_state.term;
        let meta = snapshot.mut_metadata();
        meta.index = applied_idx;
        meta.term = term;

        meta.set_conf_state(raft_state.conf_state.clone());
        Ok(snapshot)
    }
}

impl HeedStorage {
    fn wl(&mut self) -> RwLockWriteGuard<HeedStorageCore> {
        self.0.write().unwrap()
    }

    fn rl(&mut self) -> RwLockReadGuard<HeedStorageCore> {
        self.0.read().unwrap()
    }
}

impl Storage for HeedStorage{
    fn initial_state(&self) -> raftrs::Result<RaftState> {
        let core = self.rl();
        core.read(|r| {
            let state = core.raft_state(r)
                .map_err(|_| raftrs::Error::Store(raftrs::StorageError::SnapshotTemporarilyUnavailable))?;
            Ok(state)
        })
        .map_err(|_| raftrs::Error::Store(raftrs::StorageError::SnapshotTemporarilyUnavailable))?
    }

    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> raftrs::Result<Vec<Entry>> {
        let core = self.rl();
        let rtxn = core.env.read_txn()
            .map_err(|_| raftrs::Error::Store(raftrs::StorageError::SnapshotTemporarilyUnavailable))?;
        let mut total_size = 0;
        let entries = core
            .entries_db
            .range(&rtxn, &(low..high))
            .map_err(|_| raftrs::Error::Store(raftrs::StorageError::SnapshotTemporarilyUnavailable))?
            .filter_map(|e| e.ok())
            .take_while(|(_, entry)| {
                if let Some(max_size) = max_size.into() {
                    let size = entry.compute_size();
                    total_size += size;
                    if total_size as u64 > max_size {
                        false
                    } else {
                        true
                    }
                } else {
                    true
                }
            })
        .map(|(_, e)| e)
        .collect();
        Ok(entries)
    }

    fn term(&self, idx: u64) -> raftrs::Result<u64> {
        let core = self.rl();
        core.read(|r| {
            let last_index = core.last_index(r)
                .map_err(|_| raftrs::Error::Store(raftrs::StorageError::SnapshotTemporarilyUnavailable))?;
            let last_entry = core.get_entry(last_index, r)
                .map_err(|_| raftrs::Error::Store(raftrs::StorageError::SnapshotTemporarilyUnavailable))?
                .expect("expect last entry");
            Ok(last_entry.get_term())
        })
        .map_err(|_| raftrs::Error::Store(raftrs::StorageError::SnapshotTemporarilyUnavailable))?
    }

    fn first_index(&self) -> raftrs::Result<u64> {
        let core = self.rl();
        core.read(|r| {
            core
                .first_index(r)
                .map_err(|_| raftrs::Error::Store(raftrs::StorageError::SnapshotTemporarilyUnavailable))
        })
        .map_err(|_| raftrs::Error::Store(raftrs::StorageError::SnapshotTemporarilyUnavailable))?
    }

    fn last_index(&self) -> raftrs::Result<u64> {
        let core = self.rl();
        core.read(|r| {
            core.last_index(r)
                .map_err(|_| raftrs::Error::Store(raftrs::StorageError::SnapshotTemporarilyUnavailable))
        })
        .map_err(|_| raftrs::Error::Store(raftrs::StorageError::SnapshotTemporarilyUnavailable))?
    }

    fn snapshot(&self, index: u64) -> raftrs::Result<Snapshot> {
        let mut core = self.wl();
        let snap = core.snapshot()
            .map_err(|_| raftrs::Error::Store(raftrs::StorageError::SnapshotTemporarilyUnavailable))?;
        Ok(snap)
    }
}
