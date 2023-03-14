use raft::prelude::Entry;
use raft::prelude::HardState;

use crate::prelude::ConfState;
use crate::prelude::RaftGroupDesc;
use crate::prelude::ReplicaDesc;
use crate::prelude::Snapshot;

use futures::Future;

use super::error::Result;

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

    /// Overwrites the contents of this Storage object with those of the given snapshot.
    ///
    /// # Panics
    ///
    /// Panics if the snapshot index is less than the storage’s first index.
    fn apply_snapshot(&self, snapshot: Snapshot) -> Result<()>;

    /// Saves the current HardState.
    fn set_hardstate(&self, hs: HardState) -> Result<()>;

    /// Saves the current ConfState
    fn set_confstate(&self, cs: ConfState) -> Result<()>;
}

/// RaftStorage provides read and writes all the information about the current Raft implementation,
/// including Raft Log, commit index, the leader to vote for, etc.
///
/// If any Storage method returns an error, the raft instance will become inoperable and refuse
/// to participate in elections; the application is responsible for cleanup and recovery in this case.
pub trait RaftStorage:
    RaftStorageReader + RaftStorageWriter + Clone + Send + Sync + 'static
{
}

pub trait RaftSnapshotBuilder: Clone + Send + Sync + 'static {
    fn build_snapshot(&self, applied: u64) -> Result<Snapshot>;
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

    /// GAT trait for `create_group_storage_with_conf_state`.
    // type CreateGroupStorageWithConfStateFuture<'life0, T>: Send + Future<Output = Result<S>>
    // where
    //     Self: 'life0,
    //     ConfState: From<T>,
    //     T: Send + 'life0;
    // /// Create a new `RaftStorage` with `ConfState`.
    // fn create_group_storage_with_conf_state<T>(
    //     &self,
    //     group_id: u64,
    //     replica_id: u64,
    //     conf_state: T,
    // ) -> Self::CreateGroupStorageWithConfStateFuture<'_, T>
    // where
    //     ConfState: From<T>,
    //     T: Send;

    /// GAT trait for `group_desc`.
    type GroupDescFuture<'life0>: Send + Future<Output = Result<RaftGroupDesc>> + Send + 'life0
    where
        Self: 'life0;
    /// Get `RaftGroupDesc` by `group_id`. if not exists create a new one.
    fn group_desc(&self, group_id: u64) -> Self::GroupDescFuture<'_>;

    /// GAT trait for `set_group_desc`.
    type SetGroupDescFuture<'life0>: Send + Future<Output = Result<()>> + Send + 'life0
    where
        Self: 'life0;
    /// Set `RaftGroupDesc` by `group_id`.
    fn set_group_desc(
        &self,
        group_id: u64,
        group_desc: RaftGroupDesc,
    ) -> Self::SetGroupDescFuture<'_>;

    /// GAT trait for `replica_desc`.
    type ReplicaDescFuture<'life0>: Send + Future<Output = Result<Option<ReplicaDesc>>>
    where
        Self: 'life0;
    /// Get the replica description of `group_id` and `replica_id`.
    fn replica_desc(&self, group_id: u64, replica_id: u64) -> Self::ReplicaDescFuture<'_>;

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

    /// GAT trait for `replica_for_node`.
    type ReplicaForNodeFuture<'life0>: Send + Future<Output = Result<Option<ReplicaDesc>>>
    where
        Self: 'life0;
    // Get the `ReplicaDesc` by `group_id` and `node_id`.
    fn replica_for_node(&self, group_id: u64, node_id: u64) -> Self::ReplicaForNodeFuture<'_>;
}

mod mem;

pub use mem::{MultiRaftMemoryStorage, RaftMemStorage};