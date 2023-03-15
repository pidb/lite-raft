use std::collections::HashMap;
use std::sync::Arc;

use futures::Future;
use raft::storage::MemStorage;
use raft::Result as RaftResult;
use tokio::sync::RwLock as AsyncRwLock;

use crate::prelude::ConfState;
use crate::prelude::Entry;
use crate::prelude::HardState;
use crate::prelude::RaftState;
use crate::prelude::ReplicaDesc;
use crate::prelude::Snapshot;
use crate::prelude::SnapshotMetadata;


use super::MultiRaftStorage;
use super::RaftSnapshotReader;
use super::RaftSnapshotWriter;
use super::RaftStorage;
use super::RaftStorageReader;
use super::RaftStorageWriter;
use super::Result;

#[derive(Clone)]
pub struct RaftMemStorage {
    core: Arc<MemStorage>,
}

impl RaftMemStorage {
    pub fn new() -> Self {
        Self {
            core: Arc::new(MemStorage::new()),
        }
    }

    pub fn new_with_conf_state(cs: ConfState) -> Self {
        Self {
            core: Arc::new(MemStorage::new_with_conf_state(cs)),
        }
    }
}

impl RaftStorageReader for RaftMemStorage {
    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: raft::GetEntriesContext,
    ) -> RaftResult<Vec<Entry>> {
        self.core.entries(low, high, max_size, context)
    }

    fn first_index(&self) -> RaftResult<u64> {
        self.core.first_index()
    }

    fn initial_state(&self) -> RaftResult<RaftState> {
        self.core.initial_state()
    }

    fn last_index(&self) -> RaftResult<u64> {
        self.core.last_index()
    }

    fn snapshot(&self, request_index: u64, to: u64) -> RaftResult<Snapshot> {
        self.core.snapshot(request_index, to)
    }

    fn term(&self, idx: u64) -> RaftResult<u64> {
        self.core.term(idx)
    }
}

impl RaftStorageWriter for RaftMemStorage {
    fn append(&self, ents: &[Entry]) -> Result<()> {
        self.core.wl().append(ents).map_err(|err| err.into())
    }

    fn install_snapshot(&self, snapshot: Snapshot) -> Result<()> {
        self.core
            .wl()
            .apply_snapshot(snapshot)
            .map_err(|err| err.into())
    }

    fn set_hardstate(&self, hs: HardState) -> Result<()> {
        self.core.wl().set_hardstate(hs);
        Ok(())
    }

    fn set_confstate(&self, cs: ConfState) -> Result<()> {
        self.core.wl().set_conf_state(cs);
        Ok(())
    }
}

impl RaftSnapshotWriter for RaftMemStorage {
    fn build_snapshot(&self, _group_id: u64, _replica_id: u64, meta: SnapshotMetadata) -> Result<()> {
        unimplemented!()
    }

    fn save_snapshot(&self, _group_id: u64, _replica_id: u64, snapshot: Snapshot) -> Result<()> {
        self.core
            .wl()
            .apply_snapshot(snapshot)
            .map_err(|err| err.into())
    }
}

impl RaftSnapshotReader for RaftMemStorage {
    fn load_snapshot(
        &self,
        _group_id: u64,
        _replica_id: u64,
        request_index: u64,
        to: u64,
    ) -> Result<Snapshot> {
        self.core
            .snapshot(request_index, to)
            .map_err(|err| err.into())
    }

    fn snapshot_metadata(
        &self,
        _group_id: u64,
        _replica_id: u64,
    ) -> Result<raft::prelude::SnapshotMetadata> {
        Ok(self.core.snapshot(0, 0).unwrap().metadata.unwrap())
    }
}

impl RaftStorage for RaftMemStorage {
    type SnapshotReader = Self;
    type SnapshotWriter = Self;
}

#[derive(Clone)]
pub struct MultiRaftMemoryStorage {
    node_id: u64,
    groups: Arc<AsyncRwLock<HashMap<u64, RaftMemStorage>>>,
    replicas: Arc<AsyncRwLock<HashMap<u64, Vec<ReplicaDesc>>>>,
}

impl MultiRaftMemoryStorage {
    pub fn new(node_id: u64) -> Self {
        Self {
            node_id,
            groups: Default::default(),
            replicas: Default::default(),
        }
    }
}

impl MultiRaftStorage<RaftMemStorage> for MultiRaftMemoryStorage {
    type GroupStorageFuture<'life0> = impl Future<Output = Result<RaftMemStorage>> + 'life0
        where
            Self: 'life0;
    #[allow(unused)]
    fn group_storage(&self, group_id: u64, replica_id: u64) -> Self::GroupStorageFuture<'_> {
        async move {
            let mut wl = self.groups.write().await;
            match wl.get_mut(&group_id) {
                None => {
                    let storage = RaftMemStorage::new();
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
