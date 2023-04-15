use std::cmp;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;
use std::thread::sleep;
use std::time::Duration;

use futures::Future;
// use raft::storage::MemStorage;
use raft::Error as RaftError;
use raft::GetEntriesContext;
use raft::Result as RaftResult;
use raft::StorageError;
use tokio::sync::RwLock as AsyncRwLock;

use crate::prelude::ConfState;
use crate::prelude::Entry;
use crate::prelude::HardState;
use crate::prelude::RaftState;
use crate::prelude::ReplicaDesc;
use crate::prelude::Snapshot;
use crate::prelude::SnapshotMetadata;

use super::Error;
use super::MultiRaftStorage;
use super::RaftSnapshotReader;
use super::RaftSnapshotWriter;
use super::RaftStorage;
use super::RaftStorageReader;
use super::RaftStorageWriter;
use super::Result;

#[derive(Default)]
struct TriggerSlow {
    enable: bool,
    block: Duration,
}

/// The Memory Storage Core instance holds the actual state of the storage struct. To access this
/// value, use the `rl` and `wl` functions on the main MemStorage implementation.
#[derive(Default)]
pub struct MemStorageCore {
    raft_state: RaftState,
    // entries[i] has raft log position i+snapshot.get_metadata().index
    entries: Vec<Entry>,
    // Metadata of the last snapshot received.
    snapshot_metadata: SnapshotMetadata,
    // Maintenance application applied
    applied_index: u64,
    applied_term: u64,
    // If it is true, the next snapshot will return a
    // SnapshotTemporarilyUnavailable error.
    trigger_snap_temp_unavailable: bool,
    // If it is ture, the storage is unavailable for a
    // long time.
    trigger_log_unavailable: bool,
    // Peers that are fetching entries asynchronously.
    trigger_log_temp_unavailable: bool,
    // If it is true, the next log related write will
    // be delayed.
    trigger_log_write_slow: TriggerSlow,
    // If it is true, the next log related read will
    // be delayed.
    trigger_log_read_slow: TriggerSlow,
    // Stores get entries context.
    get_entries_context: Option<GetEntriesContext>,
}

impl MemStorageCore {
    /// Saves the current HardState.
    pub fn set_hardstate(&mut self, hs: HardState) -> Result<()> {
        if self.trigger_log_unavailable {
            return Err(super::Error::LogUnavailable);
        }

        if self.trigger_log_temp_unavailable {
            return Err(Error::LogTemporarilyUnavailable);
        }

        if self.trigger_log_write_slow.enable {
            sleep(self.trigger_log_write_slow.block)
        }

        self.raft_state.hard_state = hs;
        Ok(())
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
    pub fn set_conf_state(&mut self, cs: ConfState) -> Result<()> {
        if self.trigger_log_unavailable {
            return Err(super::Error::LogUnavailable);
        }

        if self.trigger_log_temp_unavailable {
            return Err(Error::LogTemporarilyUnavailable);
        }

        if self.trigger_log_write_slow.enable {
            sleep(self.trigger_log_write_slow.block)
        }

        self.raft_state.conf_state = cs;
        Ok(())
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
    pub fn apply_snapshot(&mut self, mut snapshot: Snapshot) -> RaftResult<()> {
        let mut meta = snapshot.take_metadata();
        let index = meta.index;

        if self.first_index() > index {
            return Err(RaftError::Store(StorageError::SnapshotOutOfDate));
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

        if self.trigger_log_unavailable {
            return Err(super::Error::LogUnavailable);
        }

        if self.trigger_log_temp_unavailable {
            return Err(super::Error::LogTemporarilyUnavailable);
        }

        if self.trigger_log_write_slow.enable {
            sleep(self.trigger_log_write_slow.block)
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

    /// Set applied index and term.
    pub fn set_applied(&mut self, index: u64, term: u64) {
        self.applied_index = index;
        self.applied_term = term;
    }

    pub fn set_commit(&mut self, commit: u64) {
        self.mut_hard_state().commit = commit;
    }

    /// Trigger a SnapshotTemporarilyUnavailable error.
    pub fn trigger_snap_unavailable(&mut self) {
        self.trigger_snap_temp_unavailable = true;
    }

    /// Set a LogTemporarilyUnavailable error.
    pub fn trigger_log_unavailable(&mut self, enable: bool) {
        self.trigger_log_temp_unavailable = enable;
    }

    /// Enable log to write slowly.
    pub fn enable_log_write_slow(&mut self, block: Duration) {
        self.trigger_log_write_slow.enable = true;
        self.trigger_log_write_slow.block = block;
    }

    /// Disable log to write slowly
    pub fn disable_log_write_slow(&mut self) {
        self.trigger_log_write_slow.enable = false;
    }

    /// Enable log to read slowly.
    pub fn enable_log_read_slow(&mut self, block: Duration) {
        self.trigger_log_read_slow.enable = true;
        self.trigger_log_read_slow.block = block;
    }

    /// Disable log to read slowly
    pub fn disable_log_read_slow(&mut self) {
        self.trigger_log_read_slow.enable = false;
    }

    /// Take get entries context.
    pub fn take_get_entries_context(&mut self) -> Option<GetEntriesContext> {
        self.get_entries_context.take()
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

impl RaftStorageReader for MemStorage {
    /// Implements the Storage trait.
    fn initial_state(&self) -> RaftResult<RaftState> {
        let core = self.rl();
        if core.trigger_log_read_slow.enable {
            sleep(core.trigger_log_read_slow.block);
        }
        Ok(core.raft_state.clone())
    }

    /// Implements the Storage trait.
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: GetEntriesContext,
    ) -> RaftResult<Vec<Entry>> {
        let max_size = max_size.into();
        let mut core = self.wl();
        if low < core.first_index() {
            return Err(RaftError::Store(StorageError::Compacted));
        }

        if high > core.last_index() + 1 {
            panic!(
                "index out of bound (last: {}, high: {})",
                core.last_index() + 1,
                high
            );
        }

        // Note: not use for entires, because if we return
        // this error that raft-rs will be panic.
        // if core.trigger_log_unavailable {
        //     return Err(RaftError::Store(StorageError::Unavailable))
        // }

        if core.trigger_log_temp_unavailable && context.can_async() {
            core.get_entries_context = Some(context);
            return Err(RaftError::Store(StorageError::LogTemporarilyUnavailable));
        }

        if core.trigger_log_read_slow.enable {
            sleep(core.trigger_log_read_slow.block)
        }

        let offset = core.entries[0].index;
        let lo = (low - offset) as usize;
        let hi = (high - offset) as usize;
        let mut ents = core.entries[lo..hi].to_vec();
        raft::util::limit_size(&mut ents, max_size);
        Ok(ents)
    }

    /// Implements the Storage trait.
    fn term(&self, idx: u64) -> RaftResult<u64> {
        let core = self.rl();
        if idx == core.snapshot_metadata.index {
            return Ok(core.snapshot_metadata.term);
        }

        let offset = core.first_index();
        if idx < offset {
            return Err(RaftError::Store(StorageError::Compacted));
        }

        if idx > core.last_index() {
            return Err(RaftError::Store(StorageError::Unavailable));
        }

        let core = self.rl();
        if core.trigger_log_read_slow.enable {
            sleep(core.trigger_log_read_slow.block);
        }

        Ok(core.entries[(idx - offset) as usize].term)
    }

    /// Implements the Storage trait.
    fn first_index(&self) -> RaftResult<u64> {
        Ok(self.rl().first_index())
    }

    /// Implements the Storage trait.
    fn last_index(&self) -> RaftResult<u64> {
        Ok(self.rl().last_index())
    }

    /// Implements the Storage trait.
    fn snapshot(&self, request_index: u64, _to: u64) -> RaftResult<Snapshot> {
        let mut core = self.wl();
        if core.trigger_snap_temp_unavailable {
            core.trigger_snap_temp_unavailable = false;
            Err(RaftError::Store(
                StorageError::SnapshotTemporarilyUnavailable,
            ))
        } else {
            let mut snap = core.snapshot();
            if snap.get_metadata().index < request_index {
                snap.mut_metadata().index = request_index;
            }
            Ok(snap)
        }
    }
}

impl RaftStorageWriter for MemStorage {
    fn append(&self, ents: &[Entry]) -> Result<()> {
        self.wl().append(ents).map_err(|err| err.into())
    }

    fn install_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        self.wl().apply_snapshot(snapshot).map_err(|err| err.into())
    }

    fn set_hardstate(&self, hs: HardState) -> Result<()> {
        self.wl().set_hardstate(hs)
    }

    fn set_confstate(&self, cs: ConfState) -> Result<()> {
        self.wl().set_conf_state(cs)
    }

    fn set_applied(&self, applied_index: u64, appied_term: u64) -> Result<()> {
        self.wl().set_applied(applied_index, appied_term);
        Ok(())
    }

    fn set_commit(&self, commit: u64) -> Result<()> {
        self.wl().set_commit(commit);
        Ok(())
    }

    fn get_applied(&self) -> Result<(u64, u64)> {
        let rl = self.rl();
        Ok((rl.applied_index, rl.applied_term))
    }
}

impl RaftSnapshotWriter for MemStorage {
    fn build_snapshot(
        &self,
        group_id: u64,
        replica_id: u64,
        applied_index: u64,
        applied_term: u64,
        last_conf_state: ConfState,
    ) -> Result<()> {
        unimplemented!()
    }

    fn install_snapshot(&self, group_id: u64, replica_id: u64, data: Vec<u8>) -> Result<()> {
        unimplemented!()
    }

    // fn save_snapshot(&self, _group_id: u64, _replica_id: u64, snapshot: Snapshot) -> Result<()> {
    //     self.core
    //         .wl()
    //         .apply_snapshot(snapshot)
    //         .map_err(|err| err.into())
    // }
}

impl RaftSnapshotReader for MemStorage {
    fn load_snapshot(&self, group_id: u64, replica_id: u64) -> Result<Vec<u8>> {
        unimplemented!()
    }
}

impl RaftStorage for MemStorage {
    type SnapshotReader = Self;
    type SnapshotWriter = Self;
}

#[derive(Clone)]
pub struct MultiRaftMemoryStorage {
    node_id: u64,
    trigger_storage_temp_unavailable: Arc<AsyncRwLock<bool>>,
    groups: Arc<AsyncRwLock<HashMap<u64, MemStorage>>>,
    replicas: Arc<AsyncRwLock<HashMap<u64, Vec<ReplicaDesc>>>>,
}

impl MultiRaftMemoryStorage {
    pub fn new(node_id: u64) -> Self {
        Self {
            node_id,
            trigger_storage_temp_unavailable: Default::default(),
            groups: Default::default(),
            replicas: Default::default(),
        }
    }

    /// Trigger storage temporarily unavailable if enable is true.
    pub async fn trigger_storage_temp_unavailable(&self, enable: bool) {
        let mut wl = self.trigger_storage_temp_unavailable.write().await;
        *wl = enable;
    }
}

impl MultiRaftStorage<MemStorage> for MultiRaftMemoryStorage {
    type GroupStorageFuture<'life0> = impl Future<Output = Result<MemStorage>> + 'life0
        where
            Self: 'life0;
    #[allow(unused)]
    fn group_storage(&self, group_id: u64, replica_id: u64) -> Self::GroupStorageFuture<'_> {
        async move {
            let trigger_storage_temp_unavailable =
                self.trigger_storage_temp_unavailable.read().await;
            if *trigger_storage_temp_unavailable {
                return Err(Error::StorageTemporarilyUnavailable);
            }

            let mut wl = self.groups.write().await;
            match wl.get_mut(&group_id) {
                None => {
                    let storage = MemStorage::new();
                    wl.insert(group_id, storage.clone());
                    Ok(storage)
                }
                Some(store) => Ok(store.clone()),
            }
        }
    }

    type ReplicaDescFuture<'life0> = impl Future<Output = Result<Option<ReplicaDesc>>> + 'life0
    where
        Self: 'life0;
    fn get_replica_desc(&self, group_id: u64, replica_id: u64) -> Self::ReplicaDescFuture<'_> {
        async move {
            let trigger_storage_temp_unavailable =
                self.trigger_storage_temp_unavailable.read().await;
            if *trigger_storage_temp_unavailable {
                return Err(Error::StorageTemporarilyUnavailable);
            }

            let rl = self.replicas.read().await;
            return match rl.get(&group_id) {
                Some(replicas) => {
                    if let Some(replica) = replicas.iter().find(|r| r.replica_id == replica_id) {
                        return Ok(Some(replica.clone()));
                    }
                    Ok(None)
                }
                None => Ok(None),
            };
        }
    }

    type SetReplicaDescFuture<'life0> = impl Future<Output = Result<()>> + 'life0
    where
        Self: 'life0;
    fn set_replica_desc(
        &self,
        group_id: u64,
        replica_desc: ReplicaDesc,
    ) -> Self::SetReplicaDescFuture<'_> {
        async move {
            let trigger_storage_temp_unavailable =
                self.trigger_storage_temp_unavailable.read().await;
            if *trigger_storage_temp_unavailable {
                return Err(Error::StorageTemporarilyUnavailable);
            }

            let mut wl = self.replicas.write().await;
            return match wl.get_mut(&group_id) {
                Some(replicas) => {
                    if replicas.iter().find(|r| **r == replica_desc).is_some() {
                        return Ok(());
                    }

                    replicas.push(replica_desc);
                    Ok(())
                }
                None => {
                    wl.insert(group_id, vec![replica_desc]);
                    Ok(())
                }
            };
        }
    }

    type RemoveReplicaDescFuture<'life0> = impl Future<Output = Result<()>> + 'life0
    where
        Self: 'life0;
    fn remove_replica_desc(
        &self,
        group_id: u64,
        replica_id: u64,
    ) -> Self::RemoveReplicaDescFuture<'_> {
        async move {
            let trigger_storage_temp_unavailable =
                self.trigger_storage_temp_unavailable.read().await;
            if *trigger_storage_temp_unavailable {
                return Err(Error::StorageTemporarilyUnavailable);
            }

            let mut wl = self.replicas.write().await;
            return match wl.get_mut(&group_id) {
                Some(replicas) => {
                    if let Some(idx) = replicas.iter().position(|r| r.replica_id == replica_id) {
                        replicas.remove(idx);
                    }

                    // TODO: if replicas is empty, we need remove it from map
                    Ok(())
                }
                None => Ok(()),
            };
        }
    }

    type ReplicaForNodeFuture<'life0> = impl Future<Output = Result<Option<ReplicaDesc>>> + 'life0
    where
        Self: 'life0;

    fn replica_for_node(&self, group_id: u64, node_id: u64) -> Self::ReplicaForNodeFuture<'_> {
        async move {
            let trigger_storage_temp_unavailable =
                self.trigger_storage_temp_unavailable.read().await;
            if *trigger_storage_temp_unavailable {
                return Err(Error::StorageTemporarilyUnavailable);
            }

            let rl = self.replicas.read().await;
            return match rl.get(&group_id) {
                Some(replicas) => {
                    if let Some(replica) = replicas.iter().find(|r| r.node_id == node_id) {
                        return Ok(Some(replica.clone()));
                    }
                    Ok(None)
                }
                None => Ok(None),
            };
        }
    }
}

#[cfg(test)]
mod test {
    use std::panic::{self, AssertUnwindSafe};

    use protobuf::Message as PbMessage;
    use raft::Error as RaftError;
    use raft::Storage;
    use raft::StorageError;

    use crate::prelude::ConfState;
    use crate::prelude::Entry;
    use crate::prelude::Snapshot;

    use super::GetEntriesContext;
    use super::MemStorage;

    fn new_entry(index: u64, term: u64) -> Entry {
        let mut e = Entry::default();
        e.term = term;
        e.index = index;
        e
    }

    fn size_of<T: PbMessage>(m: &T) -> u32 {
        m.compute_size()
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
            (2, Err(RaftError::Store(StorageError::Compacted))),
            (3, Ok(3)),
            (4, Ok(4)),
            (5, Ok(5)),
            (6, Err(RaftError::Store(StorageError::Unavailable))),
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
            (
                2,
                6,
                max_u64,
                Err(RaftError::Store(StorageError::Compacted)),
            ),
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
            let e = storage.entries(lo, hi, maxsize, GetEntriesContext::empty(false));
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
            let term = if let Ok(v) =
                storage.entries(index, index + 1, 1, GetEntriesContext::empty(false))
            {
                v.first().map_or(0, |e| e.term)
            } else {
                0
            };
            if term != wterm {
                panic!("#{}: want {}, term {}", i, wterm, term);
            }
            let last = storage.last_index().unwrap();
            let len = storage
                .entries(index, last + 1, 100, GetEntriesContext::empty(false))
                .unwrap()
                .len();
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

        let unavailable = Err(RaftError::Store(
            StorageError::SnapshotTemporarilyUnavailable,
        ));
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

            let result = storage.snapshot(windex, 0);
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
