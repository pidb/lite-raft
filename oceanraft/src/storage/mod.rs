use futures::Future;
use raft::Error as RaftError;
use raft::StorageError as RaftStorageError;
use raft::StorageError;

use crate::prelude::ConfState;
use crate::prelude::Entry;
use crate::prelude::HardState;
use crate::prelude::ReplicaDesc;
use crate::prelude::Snapshot;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// The storage was temporarily unavailable.
    #[error("storage unavailable")]
    StorageUnavailable,

    /// The storage was temporarily unavailable.
    #[error("storage temporarily unavailable")]
    StorageTemporarilyUnavailable,

    /// The storage was compacted and not accessible
    #[error("log compacted")]
    LogCompacted,

    /// The log is not available.
    #[error("log unavailable")]
    LogUnavailable,

    /// The log is being fetched.
    #[error("log is temporarily unavailable")]
    LogTemporarilyUnavailable,

    /// The snapshot is out of date.
    #[error("snapshot out of date")]
    SnapshotOutOfDate,

    /// The snapshot is not available.
    #[error("snapshot unavailable")]
    SnapshotUnavailable,

    /// The snapshot is being created.
    #[error("snapshot is temporarily unavailable")]
    SnapshotTemporarilyUnavailable,

    /// Some other error occurred.
    #[error("unknown error {0}")]
    Other(#[from] Box<dyn std::error::Error + Sync + Send>),
}

impl PartialEq for Error {
    #[cfg_attr(feature = "cargo-clippy", allow(clippy::match_same_arms))]
    fn eq(&self, other: &Error) -> bool {
        matches!(
            (self, other),
            (Error::LogCompacted, Error::LogCompacted)
                | (Error::LogUnavailable, Error::LogUnavailable)
                | (
                    Error::LogTemporarilyUnavailable,
                    Error::LogTemporarilyUnavailable
                )
                | (Error::SnapshotOutOfDate, Error::SnapshotOutOfDate)
                | (
                    Error::SnapshotTemporarilyUnavailable,
                    Error::SnapshotTemporarilyUnavailable,
                )
        )
    }
}

impl From<StorageError> for Error {
    fn from(that: raft::StorageError) -> Self {
        match that {
            StorageError::Compacted => Self::LogCompacted,
            StorageError::Unavailable => Self::LogUnavailable,
            StorageError::LogTemporarilyUnavailable => Self::LogTemporarilyUnavailable,
            StorageError::SnapshotOutOfDate => Self::SnapshotOutOfDate,
            StorageError::SnapshotTemporarilyUnavailable => Self::SnapshotTemporarilyUnavailable,
            StorageError::Other(err) => Self::Other(err),
        }
    }
}

impl From<RaftError> for Error {
    fn from(that: RaftError) -> Self {
        match that {
            RaftError::Store(err) => err.into(),
            _ => unreachable!(),
        }
    }
}

impl From<Error> for RaftStorageError {
    fn from(that: Error) -> Self {
        match that {
            // just to make the compiler happy, should never reach here.
            Error::StorageUnavailable => Self::Unavailable,
            // just to make the compiler happy, should never reach here.
            Error::StorageTemporarilyUnavailable => Self::LogTemporarilyUnavailable,
            // just to make the compiler happy, should never reach here.
            Error::SnapshotUnavailable => Self::Unavailable,
            Error::LogCompacted => Self::Compacted,
            Error::LogUnavailable => Self::Unavailable,
            Error::LogTemporarilyUnavailable => Self::LogTemporarilyUnavailable,
            Error::SnapshotOutOfDate => Self::SnapshotOutOfDate,
            Error::SnapshotTemporarilyUnavailable => Self::SnapshotTemporarilyUnavailable,
            Error::Other(err) => Self::Other(err),
        }
    }
}

impl From<Error> for RaftError {
    fn from(that: Error) -> Self {
        match that {
            // just to make the compiler happy, should never reach here.
            Error::StorageUnavailable => RaftError::Store(RaftStorageError::Unavailable),
            // just to make the compiler happy, should never reach here.
            Error::StorageTemporarilyUnavailable => {
                RaftError::Store(RaftStorageError::LogTemporarilyUnavailable)
            }
            // just to make the compiler happy, should never reach here.
            Error::SnapshotUnavailable => RaftError::Store(RaftStorageError::Unavailable),
            Error::LogCompacted => RaftError::Store(RaftStorageError::Compacted),
            Error::LogUnavailable => RaftError::Store(RaftStorageError::Unavailable),
            Error::LogTemporarilyUnavailable => {
                RaftError::Store(RaftStorageError::LogTemporarilyUnavailable)
            }
            Error::SnapshotOutOfDate => RaftError::Store(RaftStorageError::SnapshotOutOfDate),
            Error::SnapshotTemporarilyUnavailable => {
                RaftError::Store(RaftStorageError::SnapshotTemporarilyUnavailable)
            }
            Error::Other(err) => RaftError::Store(RaftStorageError::Other(err)),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// RaftStorageReader comes from a re-export of `raft-rs`, and provides an
/// interface for `raft-rs` to read storage
pub use raft::Storage as RaftStorageReader;

/// RaftStorageWriter provides writes all the information about the current Raft implementation,
/// including Raft Log, commit index, the leader to vote for, etc.
///
/// If any Storage method returns an error, the raft instance will become inoperable and refuse to participate in elections; the application is responsible for cleanup and recovery in this case.
pub trait RaftStorageWriter {
    /// Append the new entries to storage.
    ///
    /// # Panics
    ///
    /// Panics if `ents` contains compacted entries, or there's a gap between `ents` and the last
    /// received entry in the storage.
    fn append(&self, ents: &[Entry]) -> Result<()>;

    /// Saves the current HardState.
    fn set_hardstate(&self, hs: HardState) -> Result<()>;

    /// Saves the current ConfState
    fn set_confstate(&self, cs: ConfState) -> Result<()>;

    /// Overwrites the contents of this Storage object with those of the given snapshot.
    ///
    /// # Panics
    ///
    /// Panics if the snapshot index is less than the storage’s first index.
    fn install_snapshot(&self, snapshot: Snapshot) -> Result<()>;
}

pub trait RaftSnapshotReader: Clone + Send + Sync + 'static {
    // TODO: using serializer trait for adta
    fn load_snapshot(&self, group_id: u64, replica_id: u64) -> Result<Vec<u8>>;
}

pub trait RaftSnapshotWriter: Clone + Send + Sync + 'static {
    // TODO: using serializer trait for adta
    fn install_snapshot(&self, group_id: u64, replica_id: u64, data: Vec<u8>) -> Result<()>;

    fn build_snapshot(
        &self,
        group_id: u64,
        replica_id: u64,
        applied_index: u64,
        applied_term: u64,
        last_conf_state: ConfState,
    ) -> Result<()>;
}

/// RaftStorage provides read and writes all the information about the current Raft implementation,
/// including Raft Log, commit index, the leader to vote for, etc.
///
/// If any Storage method returns an error, the raft instance will become inoperable and refuse
/// to participate in elections; the application is responsible for cleanup and recovery in this case.
pub trait RaftStorage:
    RaftStorageReader + RaftStorageWriter + Clone + Send + Sync + 'static
{
    type SnapshotWriter: RaftSnapshotWriter;
    type SnapshotReader: RaftSnapshotReader;
}
//----------------------------------------------------------------------
// MultiRaft storage trait
//----------------------------------------------------------------------

/// MultiRaftStorage per raft group.
pub trait MultiRaftStorage<S: RaftStorage>: Clone + Send + Sync + 'static {
    /// GAT trait for `group_storage`.
    type GroupStorageFuture<'life0>: Send + Future<Output = Result<S>>
    where
        Self: 'life0;
    /// Get the `RaftStorage` impl by `group_id` and `replica_id`. if not exists create a
    /// new one.
    fn group_storage(&self, group_id: u64, replica_id: u64) -> Self::GroupStorageFuture<'_>;

    /// GAT trait for `replica_desc`.
    type ReplicaDescFuture<'life0>: Send + Future<Output = Result<Option<ReplicaDesc>>>
    where
        Self: 'life0;
    /// Get the replica description of `group_id` and `replica_id`.
    fn get_replica_desc(&self, group_id: u64, replica_id: u64) -> Self::ReplicaDescFuture<'_>;

    /// GAT trait for `set_replica_desc`.
    type SetReplicaDescFuture<'life0>: Send + Future<Output = Result<()>> + Send + 'life0
    where
        Self: 'life0;
    /// Set the `ReplicaDesc` by `group_id`.
    fn set_replica_desc(
        &self,
        group_id: u64,
        replica_desc: ReplicaDesc,
    ) -> Self::SetReplicaDescFuture<'_>;

    /// GAT trait for `set_replica_desc`.
    type RemoveReplicaDescFuture<'life0>: Send + Future<Output = Result<()>> + Send + 'life0
    where
        Self: 'life0;
    /// Set the `ReplicaDesc` by `group_id`.
    fn remove_replica_desc(
        &self,
        group_id: u64,
        replica_id: u64,
    ) -> Self::RemoveReplicaDescFuture<'_>;

    /// GAT trait for `replica_for_node`.
    type ReplicaForNodeFuture<'life0>: Send + Future<Output = Result<Option<ReplicaDesc>>>
    where
        Self: 'life0;
    // Get the `ReplicaDesc` by `group_id` and `node_id`.
    fn replica_for_node(&self, group_id: u64, node_id: u64) -> Self::ReplicaForNodeFuture<'_>;
}

mod mem;

#[cfg(feature = "store-rocksdb")]
mod rocks;
pub use mem::{MemStorage, MultiRaftMemoryStorage};
pub use rocks::{ApplyWriteBatch, RockStore, RockStoreCore, StateMachineStore};
