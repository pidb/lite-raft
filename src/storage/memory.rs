use std::cmp;
use std::collections::hash_map::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;

use futures::Future;
use tokio::sync::RwLock as AsyncRwLock;

use crate::proto::limit_entry_size;
use crate::proto::ConfState;
use crate::proto::Entry;
use crate::proto::HardState;
use crate::proto::ReplicaMetadata;
use crate::proto::Snapshot;
use crate::proto::SnapshotMetadata;

use crate::storage::MultiRaftStorage;
use crate::storage::RaftSnapshotBuilder;
use crate::storage::RaftState;
use crate::storage::RaftStorage;
use crate::storage::RaftStorageImpl;
use crate::storage::Result;
use crate::storage::StorageError;

/// The Memory Storage Core instance holds the actual state of the storage struct. To access this
/// value, use the `rl` and `wl` functions on the main MemStorage implementation.
#[allow(unused)]
#[derive(Default)]
pub struct MemStorageCore {
    raft_state: RaftState,
    // entries[i] has raft log position i+snapshot.get_metadata().index
    entries: Vec<Entry>,
    // Metadata of the last snapshot received.
    snapshot_metadata: SnapshotMetadata,
    // If it is true, the next snapshot will return a
    // SnapshotTemporarilyUnavailable error.
    trigger_snap_unavailable: bool,
    // Peers that are fetching entries asynchronously.
    trigger_log_unavailable: bool,
    // Stores get entries context.
}

impl MemStorageCore {
    /// Saves the current HardState.
    pub fn set_hardstate(&mut self, hs: HardState) {
        self.raft_state.hard_state = hs;
    }

    /// Get the hard state.
    pub fn hard_state(&self) -> &HardState {
        &self.raft_state.hard_state
    }

    /// Get the mut hard state.
    pub fn mut_hard_state(&mut self) -> &mut HardState {
        &mut self.raft_state.hard_state
    }

    /// Commit to an index.
    ///
    /// # Panics
    ///
    /// Panics if there is no such entry in raft logs.
    pub fn commit_to(&mut self, index: u64) -> Result<()> {
        assert!(
            self.has_entry_at(index),
            "commit_to {} but the entry does not exist",
            index
        );

        let diff = (index - self.entries[0].index) as usize;
        self.raft_state.hard_state.commit = index;
        self.raft_state.hard_state.term = self.entries[diff].term;
        Ok(())
    }

    /// Saves the current conf state.
    pub fn set_conf_state(&mut self, cs: ConfState) {
        self.raft_state.conf_state = cs;
    }

    #[inline]
    fn has_entry_at(&self, index: u64) -> bool {
        !self.entries.is_empty() && index >= self.first_index() && index <= self.last_index()
    }

    fn first_index(&self) -> u64 {
        match self.entries.first() {
            Some(e) => e.index,
            None => self.snapshot_metadata.index + 1,
        }
    }

    fn last_index(&self) -> u64 {
        match self.entries.last() {
            Some(e) => e.index,
            None => self.snapshot_metadata.index,
        }
    }

    /// Overwrites the contents of this Storage object with those of the given snapshot.
    ///
    /// # Panics
    ///
    /// Panics if the snapshot index is less than the storage's first index.
    pub fn apply_snapshot(&mut self, mut snapshot: Snapshot) -> Result<()> {
        let mut meta = snapshot.take_metadata();
        let index = meta.index;

        if self.first_index() > index {
            return Err(StorageError::SnapshotOutOfDate);
        }

        self.snapshot_metadata = meta.clone();

        self.raft_state.hard_state.term = cmp::max(self.raft_state.hard_state.term, meta.term);
        self.raft_state.hard_state.commit = index;
        self.entries.clear();

        // Update conf states.
        self.raft_state.conf_state = meta.take_conf_state();
        Ok(())
    }

    fn snapshot(&self) -> Snapshot {
        let mut snapshot = Snapshot::default();

        // We assume all entries whose indexes are less than `hard_state.commit`
        // have been applied, so use the latest commit index to construct the snapshot.
        // TODO: This is not true for async ready.
        let meta = snapshot.mut_metadata();
        meta.index = self.raft_state.hard_state.commit;
        meta.term = match meta.index.cmp(&self.snapshot_metadata.index) {
            cmp::Ordering::Equal => self.snapshot_metadata.term,
            cmp::Ordering::Greater => {
                let offset = self.entries[0].index;
                self.entries[(meta.index - offset) as usize].term
            }
            cmp::Ordering::Less => {
                panic!(
                    "commit {} < snapshot_metadata.index {}",
                    meta.index, self.snapshot_metadata.index
                );
            }
        };

        meta.set_conf_state(self.raft_state.conf_state.clone());
        snapshot
    }

    /// Discards all log entries prior to compact_index.
    /// It is the application's responsibility to not attempt to compact an index
    /// greater than RaftLog.applied.
    ///
    /// # Panics
    ///
    /// Panics if `compact_index` is higher than `Storage::last_index(&self) + 1`.
    pub fn compact(&mut self, compact_index: u64) -> Result<()> {
        if compact_index <= self.first_index() {
            // Don't need to treat this case as an error.
            return Ok(());
        }

        if compact_index > self.last_index() + 1 {
            panic!(
                "compact not received raft logs: {}, last index: {}",
                compact_index,
                self.last_index()
            );
        }

        if let Some(entry) = self.entries.first() {
            let offset = compact_index - entry.index;
            self.entries.drain(..offset as usize);
        }
        Ok(())
    }

    /// Append the new entries to storage.
    ///
    /// # Panics
    ///
    /// Panics if `ents` contains compacted entries, or there's a gap between `ents` and the last
    /// received entry in the storage.
    pub fn append(&mut self, ents: &[Entry]) -> Result<()> {
        if ents.is_empty() {
            return Ok(());
        }
        if self.first_index() > ents[0].index {
            panic!(
                "overwrite compacted raft logs, compacted: {}, append: {}",
                self.first_index() - 1,
                ents[0].index,
            );
        }
        if self.last_index() + 1 < ents[0].index {
            panic!(
                "raft logs should be continuous, last index: {}, new appended: {}",
                self.last_index(),
                ents[0].index,
            );
        }

        // Remove all entries overwritten by `ents`.
        let diff = ents[0].index - self.first_index();
        self.entries.drain(diff as usize..);
        self.entries.extend_from_slice(ents);
        Ok(())
    }

    /// Commit to `idx` and set configuration to the given states. Only used for tests.
    pub fn commit_to_and_set_conf_states(&mut self, idx: u64, cs: Option<ConfState>) -> Result<()> {
        self.commit_to(idx)?;
        if let Some(cs) = cs {
            self.raft_state.conf_state = cs;
        }
        Ok(())
    }

    /// Trigger a SnapshotTemporarilyUnavailable error.
    pub fn trigger_snap_unavailable(&mut self) {
        self.trigger_snap_unavailable = true;
    }

    /// Set a LogTemporarilyUnavailable error.
    pub fn trigger_log_unavailable(&mut self, v: bool) {
        self.trigger_log_unavailable = v;
    }
}

/// `MemStorage` is a thread-safe but incomplete implementation of `Storage`, mainly for tests.
///
/// A real `Storage` should save both raft logs and applied data. However `MemStorage` only
/// contains raft logs. So you can call `MemStorage::append` to persist new received unstable raft
/// logs and then access them with `Storage` APIs. The only exception is `Storage::snapshot`. There
/// is no data in `Snapshot` returned by `MemStorage::snapshot` because applied data is not stored
/// in `MemStorage`.
#[derive(Clone, Default)]
pub struct MemStorage {
    core: Arc<RwLock<MemStorageCore>>,
}

impl MemStorage {
    /// Returns a new memory storage value.
    pub fn new() -> MemStorage {
        MemStorage {
            ..Default::default()
        }
    }

    /// Create a new `MemStorage` with a given `Config`. The given `Config` will be used to
    /// initialize the storage.
    ///
    /// You should use the same input to initialize all nodes.
    pub fn new_with_conf_state<T>(conf_state: T) -> MemStorage
    where
        ConfState: From<T>,
    {
        let store = MemStorage::new();
        store.initialize_with_conf_state(conf_state);
        store
    }

    /// Initialize a `MemStorage` with a given `Config`.
    ///
    /// You should use the same input to initialize all nodes.
    pub fn initialize_with_conf_state<T>(&self, conf_state: T)
    where
        ConfState: From<T>,
    {
        assert!(!self.initial_state().unwrap().initialized());
        let mut core = self.wl();
        // Setting initial state is very important to build a correct raft, as raft algorithm
        // itself only guarantees logs consistency. Typically, you need to ensure either all start
        // states are the same on all nodes, or new nodes always catch up logs by snapshot first.
        //
        // In practice, we choose the second way by assigning non-zero index to first index. Here
        // we choose the first way for historical reason and easier to write tests.
        core.raft_state.conf_state = ConfState::from(conf_state);
    }

    /// Opens up a read lock on the storage and returns a guard handle. Use this
    /// with functions that don't require mutation.
    pub fn rl(&self) -> RwLockReadGuard<'_, MemStorageCore> {
        self.core.read().unwrap()
    }

    /// Opens up a write lock on the storage and returns guard handle. Use this
    /// with functions that take a mutable reference to self.
    pub fn wl(&self) -> RwLockWriteGuard<'_, MemStorageCore> {
        self.core.write().unwrap()
    }
}

impl RaftStorage for MemStorage {
    /// Implements the Storage trait.
    fn initial_state(&self) -> Result<RaftState> {
        Ok(self.rl().raft_state.clone())
    }

    /// Implements the Storage trait.
    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>> {
        let max_size = max_size.into();
        let core = self.wl();
        if low < core.first_index() {
            return Err(StorageError::Compacted);
        }

        if high > core.last_index() + 1 {
            panic!(
                "index out of bound (last: {}, high: {})",
                core.last_index() + 1,
                high
            );
        }

        // if core.trigger_log_unavailable && context.can_async() {
        //     core.get_entries_context = Some(context);
        //     return Err(StorageError::LogTemporarilyUnavailable);
        // }

        let offset = core.entries[0].index;
        let lo = (low - offset) as usize;
        let hi = (high - offset) as usize;
        let mut ents = core.entries[lo..hi].to_vec();
        limit_entry_size(&mut ents, max_size);
        Ok(ents)
    }

    /// Implements the Storage trait.
    fn term(&self, idx: u64) -> Result<u64> {
        let core = self.rl();
        if idx == core.snapshot_metadata.index {
            return Ok(core.snapshot_metadata.term);
        }

        let offset = core.first_index();
        if idx < offset {
            return Err(StorageError::Compacted);
        }

        if idx > core.last_index() {
            return Err(StorageError::Unavailable);
        }
        Ok(core.entries[(idx - offset) as usize].term)
    }

    /// Implements the Storage trait.
    fn first_index(&self) -> Result<u64> {
        Ok(self.rl().first_index())
    }

    /// Implements the Storage trait.
    fn last_index(&self) -> Result<u64> {
        Ok(self.rl().last_index())
    }

    /// Implements the Storage trait.
    fn snapshot(&self, request_index: u64) -> Result<Snapshot> {
        let mut core = self.wl();
        if core.trigger_snap_unavailable {
            core.trigger_snap_unavailable = false;
            Err(StorageError::SnapshotTemporarilyUnavailable)
        } else {
            let mut snap = core.snapshot();
            if snap.get_metadata().index < request_index {
                snap.mut_metadata().index = request_index;
            }
            Ok(snap)
        }
    }

    fn append_entries(&self, entries: &Vec<Entry>) -> Result<()> {
        let mut wl = self.wl();
        wl.append(&entries)
    }

    fn get_hard_state(&self) -> Result<HardState> {
        Ok(self.rl().hard_state().clone())
    }

    fn set_hardstate(&self, hs: HardState) -> Result<()> {
        Ok(self.wl().set_hardstate(hs))
    }

    fn set_confstate(&self, cs: ConfState) -> Result<()> {
        Ok(self.wl().set_conf_state(cs))
    }

    fn get_confstate(&self) -> Result<ConfState> {
        let rl = self.rl();
        Ok(rl.raft_state.conf_state.clone())
    }

    fn set_commit(&self, commit: u64) {
        self.wl().mut_hard_state().commit = commit
    }

    fn apply_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        self.wl().apply_snapshot(snapshot)
    }
}

impl RaftSnapshotBuilder for MemStorage {
    fn build_snapshot(&self, applied: u64) -> Result<Snapshot> {
        unimplemented!()
    }
}

#[derive(Clone)]
pub struct MultiRaftGroupMemoryStorage {
    node_id: u64,
    store_id: u64,
    groups: Arc<AsyncRwLock<HashMap<u64, MemStorage>>>,
    // group_id map to multiple replica_metadata.
    replicas: Arc<AsyncRwLock<HashMap<u64, Vec<ReplicaMetadata>>>>,
}

impl MultiRaftGroupMemoryStorage {
    pub fn new(node_id: u64, store_id: u64) -> Self {
        Self {
            node_id,
            store_id,
            groups: Default::default(),
            replicas: Default::default(),
        }
    }

    pub async fn insert_memory_storage(&self, group_id: u64) {
        let mut wl = self.groups.write().await;
        if wl.contains_key(&group_id) {
            panic!("raft group ({}) already exists", group_id)
        }
        wl.insert(group_id, MemStorage::new());
    }

    pub async fn insert_replica_memory_storage_with_conf_state<T>(
        &self,
        group_id: u64,
        replica_id: u64,
        conf_state: T,
    ) where
        ConfState: From<T>,
    {
        let mut wl = self.groups.write().await;
        if wl.contains_key(&group_id) {
            panic!("raft group ({}) already exists", group_id)
        }

        wl.insert(group_id, MemStorage::new_with_conf_state(conf_state));
    }
}

impl MultiRaftStorage<MemStorage> for MultiRaftGroupMemoryStorage {
    type GroupStorageFuture<'life0> = impl Future<Output = super::storage::Result<Option<super::RaftStorageImpl<MemStorage>>>>
        where
            Self: 'life0;

    type ReplicaMetadataFuture<'life0> = impl Future<Output =  super::storage::Result<crate::proto::ReplicaMetadata>>
        where
            Self: 'life0;

    type CreateGroupStorageFuture<'life0> = impl Future<Output = super::storage::Result<super::RaftStorageImpl<MemStorage>>>
        where
            Self: 'life0;

    type CreateGroupStorageWithConfStateFuture<'life0, T> = impl Future<Output = super::storage::Result<super::RaftStorageImpl<MemStorage>>>
        where
            Self: 'life0,
            ConfState: From<T>,
            T: Send;

    type ReplicaInStoreFuture<'life0> = impl Future<Output = super::storage::Result<Option<u64>>>
        where
            Self: 'life0;

    #[allow(unused)]
    fn create_group_storage(
        &self,
        group_id: u64,
        replica_id: u64,
    ) -> Self::CreateGroupStorageFuture<'_> {
        async move {
            let mut wl = self.groups.write().await;
            assert_ne!(wl.contains_key(&group_id), true);
            let storage = MemStorage::new();
            wl.insert(group_id, storage.clone());
            Ok(RaftStorageImpl::new(storage))
        }
    }

    #[allow(unused)]
    fn create_group_storage_with_conf_state<T>(
        &self,
        group_id: u64,
        replica_id: u64,
        conf_state: T,
    ) -> Self::CreateGroupStorageWithConfStateFuture<'_, T>
    where
        ConfState: From<T>,
        T: Send,
    {
        async move {
            let mut wl = self.groups.write().await;
            assert_ne!(wl.contains_key(&group_id), true);
            let storage = MemStorage::new_with_conf_state(conf_state);
            wl.insert(group_id, storage.clone());
            Ok(RaftStorageImpl::new(storage))
        }
    }

    #[allow(unused)]
    fn group_storage(&self, group_id: u64, replica_id: u64) -> Self::GroupStorageFuture<'_> {
        async move {
            let mut wl = self.groups.write().await;
            match wl.get_mut(&group_id) {
                None => Ok(None),
                Some(store) => Ok(Some(RaftStorageImpl::new(store.clone()))),
            }
        }
    }

    fn replica_metadata(&self, group_id: u64, replica_id: u64) -> Self::ReplicaMetadataFuture<'_> {
        async move {
            let mut wl = self.replicas.write().await;
            return match wl.get_mut(&group_id) {
                Some(replicas) => {
                    match replicas
                        .iter()
                        .find(|replica_metadata| replica_metadata.replica_id == replica_id)
                    {
                        Some(replica) => Ok(replica.clone()),
                        None => {
                            let replica_metadata = ReplicaMetadata {
                                node_id: self.node_id,
                                store_id: self.store_id,
                                replica_id,
                            };
                            replicas.push(replica_metadata.clone());
                            Ok(replica_metadata.clone())
                        }
                    }
                }
                None => {
                    let replica_metadata = ReplicaMetadata {
                        node_id: self.node_id,
                        store_id: self.store_id,
                        replica_id,
                    };
                    let replicas = vec![replica_metadata.clone()];
                    wl.insert(group_id, replicas);
                    Ok(replica_metadata)
                }
            };
        }
    }

    fn replica_in_node(&self, group_id: u64, store_id: u64) -> Self::ReplicaInStoreFuture<'_> {
        async move {
            let rl = self.replicas.read().await;
            if !rl.contains_key(&group_id) {
                return Ok(None);
            }

            let replicas = rl.get(&group_id).unwrap();
            for replica_metadata in replicas.iter() {
                if store_id == replica_metadata.store_id {
                    return Ok(Some(replica_metadata.replica_id));
                }
            }

            Ok(None)
        }
    }
}

#[cfg(test)]
mod test {
    use crate::proto::ConfState;
    use crate::proto::Entry;
    use crate::proto::Snapshot;
    use std::panic::{self, AssertUnwindSafe};

    use super::MemStorage;
    use super::RaftStorage;
    use super::StorageError;

    fn new_entry(index: u64, term: u64) -> Entry {
        let mut e = Entry::default();
        e.term = term;
        e.index = index;
        e
    }

    fn size_of(m: &Entry) -> u32 {
        m.compute_size() as u32
    }

    fn new_snapshot(index: u64, term: u64, voters: Vec<u64>) -> Snapshot {
        let mut s = Snapshot::default();
        s.mut_metadata().index = index;
        s.mut_metadata().term = term;
        s.mut_metadata().mut_conf_state().voters = voters;
        s
    }

    #[test]
    fn test_storage_term() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut tests = vec![
            (2, Err(StorageError::Compacted)),
            (3, Ok(3)),
            (4, Ok(4)),
            (5, Ok(5)),
            (6, Err(StorageError::Unavailable)),
        ];

        for (i, (idx, wterm)) in tests.drain(..).enumerate() {
            let storage = MemStorage::new();
            storage.wl().entries = ents.clone();

            let t = storage.term(idx);
            if t != wterm {
                panic!("#{}: expect res {:?}, got {:?}", i, wterm, t);
            }
        }
    }

    #[test]
    fn test_storage_entries() {
        let ents = vec![
            new_entry(3, 3),
            new_entry(4, 4),
            new_entry(5, 5),
            new_entry(6, 6),
        ];
        let max_u64 = u64::max_value();
        let mut tests = vec![
            (2, 6, max_u64, Err(StorageError::Compacted)),
            (3, 4, max_u64, Ok(vec![new_entry(3, 3)])),
            (4, 5, max_u64, Ok(vec![new_entry(4, 4)])),
            (4, 6, max_u64, Ok(vec![new_entry(4, 4), new_entry(5, 5)])),
            (
                4,
                7,
                max_u64,
                Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)]),
            ),
            // even if maxsize is zero, the first entry should be returned
            (4, 7, 0, Ok(vec![new_entry(4, 4)])),
            // limit to 2
            (
                4,
                7,
                u64::from(size_of(&ents[1]) + size_of(&ents[2])),
                Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            (
                4,
                7,
                u64::from(size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3]) / 2),
                Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            (
                4,
                7,
                u64::from(size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3]) - 1),
                Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            // all
            (
                4,
                7,
                u64::from(size_of(&ents[1]) + size_of(&ents[2]) + size_of(&ents[3])),
                Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)]),
            ),
        ];
        for (i, (lo, hi, maxsize, wentries)) in tests.drain(..).enumerate() {
            let storage = MemStorage::new();
            storage.wl().entries = ents.clone();
            let e = storage.entries(lo, hi, maxsize);
            if e != wentries {
                panic!("#{}: expect entries {:?}, got {:?}", i, wentries, e);
            }
        }
    }

    #[test]
    fn test_storage_last_index() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let storage = MemStorage::new();
        storage.wl().entries = ents;

        let wresult = Ok(5);
        let result = storage.last_index();
        if result != wresult {
            panic!("want {:?}, got {:?}", wresult, result);
        }

        storage.wl().append(&[new_entry(6, 5)]).unwrap();
        let wresult = Ok(6);
        let result = storage.last_index();
        if result != wresult {
            panic!("want {:?}, got {:?}", wresult, result);
        }
    }

    #[test]
    fn test_storage_first_index() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let storage = MemStorage::new();
        storage.wl().entries = ents;

        assert_eq!(storage.first_index(), Ok(3));
        storage.wl().compact(4).unwrap();
        assert_eq!(storage.first_index(), Ok(4));
    }

    #[test]
    fn test_storage_compact() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut tests = vec![(2, 3, 3, 3), (3, 3, 3, 3), (4, 4, 4, 2), (5, 5, 5, 1)];
        for (i, (idx, windex, wterm, wlen)) in tests.drain(..).enumerate() {
            let storage = MemStorage::new();
            storage.wl().entries = ents.clone();

            storage.wl().compact(idx).unwrap();
            let index = storage.first_index().unwrap();
            if index != windex {
                panic!("#{}: want {}, index {}", i, windex, index);
            }
            let term = if let Ok(v) = storage.entries(index, index + 1, 1) {
                v.first().map_or(0, |e| e.term)
            } else {
                0
            };
            if term != wterm {
                panic!("#{}: want {}, term {}", i, wterm, term);
            }
            let last = storage.last_index().unwrap();
            let len = storage.entries(index, last + 1, 100).unwrap().len();
            if len != wlen {
                panic!("#{}: want {}, term {}", i, wlen, len);
            }
        }
    }

    #[test]
    fn test_storage_create_snapshot() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let nodes = vec![1, 2, 3];
        let mut conf_state = ConfState::default();
        conf_state.voters = nodes.clone();

        let unavailable = Err(StorageError::SnapshotTemporarilyUnavailable);
        let mut tests = vec![
            (4, Ok(new_snapshot(4, 4, nodes.clone())), 0),
            (5, Ok(new_snapshot(5, 5, nodes.clone())), 5),
            (5, Ok(new_snapshot(6, 5, nodes)), 6),
            (5, unavailable, 6),
        ];
        for (i, (idx, wresult, windex)) in tests.drain(..).enumerate() {
            let storage = MemStorage::new();
            storage.wl().entries = ents.clone();
            storage.wl().raft_state.hard_state.commit = idx;
            storage.wl().raft_state.hard_state.term = idx;
            storage.wl().raft_state.conf_state = conf_state.clone();

            if wresult.is_err() {
                storage.wl().trigger_snap_unavailable();
            }

            let result = storage.snapshot(windex);
            if result != wresult {
                panic!("#{}: want {:?}, got {:?}", i, wresult, result);
            }
        }
    }

    #[test]
    fn test_storage_append() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut tests = vec![
            (
                vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)],
                Some(vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)]),
            ),
            (
                vec![new_entry(3, 3), new_entry(4, 6), new_entry(5, 6)],
                Some(vec![new_entry(3, 3), new_entry(4, 6), new_entry(5, 6)]),
            ),
            (
                vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 5),
                ],
                Some(vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 5),
                ]),
            ),
            // overwrite compacted raft logs is not allowed
            (
                vec![new_entry(2, 3), new_entry(3, 3), new_entry(4, 5)],
                None,
            ),
            // truncate the existing entries and append
            (
                vec![new_entry(4, 5)],
                Some(vec![new_entry(3, 3), new_entry(4, 5)]),
            ),
            // direct append
            (
                vec![new_entry(6, 6)],
                Some(vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 6),
                ]),
            ),
        ];
        for (i, (entries, wentries)) in tests.drain(..).enumerate() {
            let storage = MemStorage::new();
            storage.wl().entries = ents.clone();
            let res = panic::catch_unwind(AssertUnwindSafe(|| storage.wl().append(&entries)));
            if let Some(wentries) = wentries {
                let _ = res.unwrap();
                let e = &storage.wl().entries;
                if *e != wentries {
                    panic!("#{}: want {:?}, entries {:?}", i, wentries, e);
                }
            } else {
                res.unwrap_err();
            }
        }
    }

    #[test]
    fn test_storage_apply_snapshot() {
        let nodes = vec![1, 2, 3];
        let storage = MemStorage::new();

        // Apply snapshot successfully
        let snap = new_snapshot(4, 4, nodes.clone());
        storage.wl().apply_snapshot(snap).unwrap();

        // Apply snapshot fails due to StorageError::SnapshotOutOfDate
        let snap = new_snapshot(3, 3, nodes);
        storage.wl().apply_snapshot(snap).unwrap_err();
    }
}
