use std::mem::transmute;

use crate::proto::ConfState;
use crate::proto::Entry;
use crate::proto::HardState;
use crate::proto::Message;
use crate::proto::ReplicaMetadata;
use crate::proto::Snapshot;
use crate::proto::SnapshotMetadata;
use crate::proto::transmute_entries;

use futures::Future;

/// An error with the storage.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// The storage was compacted and not accessible
    #[error("log compacted")]
    Compacted,
    /// The log is not available.
    #[error("log unavailable")]
    Unavailable,
    /// The log is being fetched.
    #[error("log is temporarily unavailable")]
    LogTemporarilyUnavailable,
    /// The snapshot is out of date.
    #[error("snapshot out of date")]
    SnapshotOutOfDate,
    /// The snapshot is being created.
    #[error("snapshot is temporarily unavailable")]
    SnapshotTemporarilyUnavailable,
    /// Some other error occurred.
    #[error("unknown error {0}")]
    Other(#[from] Box<dyn std::error::Error + Sync + Send>),
}

impl PartialEq for StorageError {
    // #[cfg_attr(feature = "cargo-clippy", allow(clippy::match_same_arms))]
    fn eq(&self, other: &StorageError) -> bool {
        matches!(
            (self, other),
            (StorageError::Compacted, StorageError::Compacted)
                | (StorageError::Unavailable, StorageError::Unavailable)
                | (
                    StorageError::LogTemporarilyUnavailable,
                    StorageError::LogTemporarilyUnavailable
                )
                | (
                    StorageError::SnapshotOutOfDate,
                    StorageError::SnapshotOutOfDate
                )
                | (
                    StorageError::SnapshotTemporarilyUnavailable,
                    StorageError::SnapshotTemporarilyUnavailable,
                )
        )
    }
}

pub type Result<T> = std::result::Result<T, StorageError>;

#[inline]
pub fn transmute_error(error: StorageError) -> raft::StorageError {
    unsafe { transmute::<StorageError, raft::StorageError>(error) }
}


#[inline]
pub fn transmute_message(msg: Message) -> raft::prelude::Message {
    unsafe { transmute::<Message, raft::prelude::Message>(msg) }
}

#[inline]
pub fn transmute_raft_state(rs: RaftState) -> raft::RaftState {
    unsafe { transmute(rs) }
}

/// Reinterprets the `&Vec<RaftEntry>` to `&Vec<RaftEntry>`.
///
/// N.B. The reason why this unsafe method provided is because entries need to be persisted,
/// but the storeage interface we provide about persisting entires requires type `&Vec<RaftEntry>`,
/// because that is a reference and providing into requires clone whole `&Vec<Entry>` which is expensive.
#[inline]
pub fn transmute_entries_ref<'a>(entries: &'a Vec<Entry>) -> &'a Vec<raft::prelude::Entry> {
    unsafe { std::mem::transmute(entries) }
}


#[inline]
pub fn transmute_entry(entry: Entry) -> raft::prelude::Entry {
    unsafe { transmute(entry) }
}


#[inline]
pub fn transmute_snapshot_metadata(
    snapshot_metadata: SnapshotMetadata,
) -> raft::prelude::SnapshotMetadata {
    unsafe { transmute(snapshot_metadata) }
}

#[inline]
pub fn transmute_raft_entries(entries: Vec<raft::prelude::Entry>) -> Vec<Entry> {
    unsafe { transmute(entries) }
}





#[inline]
pub fn transmute_snapshot(snapshot: Snapshot) -> raft::prelude::Snapshot {
    unsafe { transmute(snapshot) }
}

/// # RaftPersistentState
///
/// Holds both the hard state (commit index, vote leader, term) and the configuration state
/// (Current node IDs)
#[derive(Debug, Clone, Default)]
pub struct RaftState {
    /// Contains the last meta information including commit index, the vote leader, and the vote term.
    pub hard_state: HardState,

    /// Records the current node IDs like `[1, 2, 3]` in the cluster. Every Raft node must have a
    /// unique ID in the cluster;
    pub conf_state: ConfState,
}

impl RaftState {
    /// Create a new RaftState.
    pub fn new(hard_state: HardState, conf_state: ConfState) -> RaftState {
        RaftState {
            hard_state,
            conf_state,
        }
    }
    /// Indicates the `RaftState` is initialized or not.
    pub fn initialized(&self) -> bool {
        self.conf_state != ConfState::default()
    }
}

pub trait RaftSnapshotBuilder: Clone + Send + Sync + 'static {
    fn build_snapshot(&self, applied: u64) -> Result<Snapshot>;
}

//----------------------------------------------------------------------
// MultiRaft storage trait
//----------------------------------------------------------------------

/// RaftStorage per replica
pub trait RaftStorage: RaftSnapshotBuilder + Clone + Send + Sync + 'static {
    /// `initial_state` is called when Raft is initialized. This interface will return a `RaftState`
    /// which contains `HardState` and `ConfState`.
    ///
    /// `RaftState` could be initialized or not. If it's initialized it means the `Storage` is
    /// created with a configuration, and its last index and term should be greater than 0.
    fn initial_state(&self) -> Result<RaftState>;

    /// Returns a slice of log entries in the range `[low, high)`.
    /// max_size limits the total size of the log entries returned if not `None`, however
    /// the slice of entries returned will always have length at least 1 if entries are
    /// found in the range.
    ///
    /// # Panics
    ///
    /// Panics if `high` is higher than `Storage::last_index(&self) + 1`.
    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>>;

    /// Append the new entries to storage.
    ///
    /// # Panics
    ///
    /// Panics if `ents` contains compacted entries, or there's a gap between `ents` and the last
    /// received entry in the storage.
    fn append_entries(&self, entries: &Vec<Entry>) -> Result<()>;

    /// Get the current HardState.
    fn get_hard_state(&self) -> Result<HardState>;

    /// Saves the current HardState.
    fn set_hardstate(&self, hs: HardState) -> Result<()>;

    /// Get the current HardState.
    fn get_confstate(&self) -> Result<ConfState>;

    /// Saves the current HardState.
    fn set_confstate(&self, cs: ConfState) -> Result<()>;

    /// Returns the term of entry idx, which must be in the range
    /// [first_index()-1, last_index()]. The term of the entry before
    /// first_index is retained for matching purpose even though the
    /// rest of that entry may not be available.
    fn term(&self, idx: u64) -> Result<u64>;

    fn set_commit(&self, commit: u64);

    /// Returns the index of the first log entry that is possible available via entries, which will
    /// always equal to `truncated index` plus 1.
    ///
    /// New created (but not initialized) `Storage` can be considered as truncated at 0 so that 1
    /// will be returned in this case.
    fn first_index(&self) -> Result<u64>;

    /// The index of the last entry replicated in the `Storage`.
    fn last_index(&self) -> Result<u64>;

    /// Returns the most recent snapshot.
    ///
    /// If snapshot is temporarily unavailable, it should return SnapshotTemporarilyUnavailable,
    /// so raft state machine could know that Storage needs some time to prepare
    /// snapshot and call snapshot later.
    /// A snapshot's index must not less than the `request_index`.
    fn snapshot(&self, request_index: u64) -> Result<Snapshot>;

    /// install snapshot
    fn apply_snapshot(&self, snapshot: Snapshot) -> Result<()>;
}

#[derive(Clone)]
pub struct RaftStorageImpl<S: RaftStorage> {
    storage_impl: S,
}

impl<S: RaftStorage> RaftStorageImpl<S> {
    pub fn new(storage_impl: S) -> Self {
        Self { storage_impl }
    }
}

impl<S: RaftStorage> RaftStorage for RaftStorageImpl<S> {
    #[inline]
    fn append_entries(&self, entries: &Vec<Entry>) -> Result<()> {
        self.storage_impl.append_entries(entries)
    }

    #[inline]
    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>) -> Result<Vec<Entry>> {
        self.storage_impl.entries(low, high, max_size)
    }

    #[inline]
    fn first_index(&self) -> Result<u64> {
        self.storage_impl.first_index()
    }

    #[inline]
    fn get_hard_state(&self) -> Result<HardState> {
        self.storage_impl.get_hard_state()
    }

    #[inline]
    fn initial_state(&self) -> Result<RaftState> {
        self.storage_impl.initial_state()
    }

    #[inline]
    fn last_index(&self) -> Result<u64> {
        self.storage_impl.last_index()
    }

    #[inline]
    fn set_commit(&self, commit: u64) {
        self.storage_impl.set_commit(commit)
    }

    #[inline]
    fn set_hardstate(&self, hs: HardState) -> Result<()> {
        self.storage_impl.set_hardstate(hs)
    }

    #[inline]
    fn get_confstate(&self) -> Result<ConfState> {
        self.storage_impl.get_confstate()
    }

    #[inline]
    fn set_confstate(&self, cs: ConfState) -> Result<()> {
        self.storage_impl.set_confstate(cs)
    }

    #[inline]
    fn snapshot(&self, request_index: u64) -> Result<Snapshot> {
        self.storage_impl.snapshot(request_index)
    }

    #[inline]
    fn term(&self, idx: u64) -> Result<u64> {
        self.storage_impl.term(idx)
    }

    #[inline]
    fn apply_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        self.storage_impl.apply_snapshot(snapshot)
    }
}

impl<S: RaftStorage> RaftSnapshotBuilder for RaftStorageImpl<S> {
    fn build_snapshot(&self, _applied: u64) -> Result<Snapshot> {
        unimplemented!()
    }
}

impl<S: RaftStorage> raft::storage::Storage for RaftStorageImpl<S> {
    fn initial_state(&self) -> raft::Result<raft::RaftState> {
        match self.storage_impl.initial_state() {
            Err(error) => Err(raft::Error::Store(transmute_error(error))),
            Ok(rs) => Ok(transmute_raft_state(rs)),
        }
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> raft::Result<Vec<raft::prelude::Entry>> {
        match self.storage_impl.entries(low, high, max_size) {
            Err(error) => Err(raft::Error::Store(transmute_error(error))),
            Ok(entries) => Ok(transmute_entries(entries)),
        }
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        self.storage_impl
            .term(idx)
            .map_err(|error| raft::Error::Store(transmute_error(error)))
    }

    fn first_index(&self) -> raft::Result<u64> {
        self.storage_impl
            .first_index()
            .map_err(|error| raft::Error::Store(transmute_error(error)))
    }

    fn last_index(&self) -> raft::Result<u64> {
        self.storage_impl
            .last_index()
            .map_err(|error| raft::Error::Store(transmute_error(error)))
    }

    fn snapshot(&self, request_index: u64) -> raft::Result<raft::prelude::Snapshot> {
        match self.storage_impl.snapshot(request_index) {
            Err(error) => Err(raft::Error::Store(transmute_error(error))),
            Ok(snapshot) => Ok(transmute_snapshot(snapshot)),
        }
    }
}

//----------------------------------------------------------------------
// MultiRaft storage trait
//----------------------------------------------------------------------

/// MultiRaftStorage per group
pub trait MultiRaftStorage<S: RaftStorage>: Clone + Send + Sync + 'static {
    // GAT trait for group_storage
    type GroupStorageFuture<'life0>: Send + Future<Output = Result<RaftStorageImpl<S>>>
    where
        Self: 'life0;

    // GAT trait for replica_metadata
    type ReplicaMetadataFuture<'life0>: Send + Future<Output = Result<ReplicaMetadata>>
    where
        Self: 'life0;

    type ReplicaInStoreFuture<'life0>: Send + Future<Output = Result<Option<u64>>>
    where
        Self: 'life0;

    type CreateGroupStorageFuture<'life0>: Send + Future<Output = Result<RaftStorageImpl<S>>>
    where
        Self: 'life0;

    type CreateGroupStorageWithConfStateFuture<'life0, T>: Send
        + Future<Output = Result<RaftStorageImpl<S>>>
    where
        Self: 'life0,
        ConfState: From<T>,
        T: Send;

    fn create_group_storage(
        &self,
        group_id: u64,
        replica_id: u64,
    ) -> Self::CreateGroupStorageFuture<'_>;

    fn create_group_storage_with_conf_state<T>(
        &self,
        group_id: u64,
        replica_id: u64,
        conf_state: T,
    ) -> Self::CreateGroupStorageWithConfStateFuture<'_, T>
    where
        ConfState: From<T>,
        T: Send;

    fn group_storage(&self, group_id: u64, replica_id: u64) -> Self::GroupStorageFuture<'_>;

    /// Get the metadata of `replica_id` and create if it does not exist.
    fn replica_metadata(&self, group_id: u64, replica_id: u64) -> Self::ReplicaMetadataFuture<'_>;

    fn replica_in_node(&self, group_id: u64, store_id: u64) -> Self::ReplicaInStoreFuture<'_>;
}
