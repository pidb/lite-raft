use raft::Storage;
use raft::StorageError;
use raft_proto::prelude::ConfState;
use raft_proto::prelude::RaftGroupDesc;
use raft_proto::prelude::ReplicaDesc;
use raft_proto::prelude::Snapshot;

use futures::Future;

use super::error::Result;

pub trait RaftSnapshotBuilder: Clone + Send + Sync + 'static {
    fn build_snapshot(&self, applied: u64) -> Result<Snapshot>;
}

//----------------------------------------------------------------------
// MultiRaft storage trait
//----------------------------------------------------------------------

/// MultiRaftStorage per raft group.
pub trait MultiRaftStorage<S: Storage>: Clone + Send + Sync + 'static {
    /// GAT trait for `group_storage`.
    type GroupStorageFuture<'life0>: Send + Future<Output = Result<S>>
    where
        Self: 'life0;
    /// Get the `RaftStorage` impl by `group_id` and `replica_id`. if not exists create a
    /// new one.
    fn group_storage(&self, group_id: u64, replica_id: u64) -> Self::GroupStorageFuture<'_>;

    /// GAT trait for `create_group_storage_with_conf_state`.
    type CreateGroupStorageWithConfStateFuture<'life0, T>: Send + Future<Output = Result<S>>
    where
        Self: 'life0,
        ConfState: From<T>,
        T: Send + 'life0;
    /// Create a new `RaftStorage` with `ConfState`.
    fn create_group_storage_with_conf_state<T>(
        &self,
        group_id: u64,
        replica_id: u64,
        conf_state: T,
    ) -> Self::CreateGroupStorageWithConfStateFuture<'_, T>
    where
        ConfState: From<T>,
        T: Send;

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
