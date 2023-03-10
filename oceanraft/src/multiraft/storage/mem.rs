use std::collections::HashMap;
use std::sync::Arc;

use futures::Future;
use raft::storage::MemStorage;
use raft::Result as RaftResult;
use tokio::sync::RwLock as AsyncRwLock;

use crate::multiraft::error::Error;
use crate::multiraft::error::Result;
use crate::prelude::ConfState;
use crate::prelude::Entry;
use crate::prelude::HardState;
use crate::prelude::RaftGroupDesc;
use crate::prelude::RaftState;
use crate::prelude::ReplicaDesc;
use crate::prelude::Snapshot;

use super::MultiRaftStorage;
use super::RaftStorage;
use super::RaftStorageReader;
use super::RaftStorageWriter;

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
    fn append(&self, ents: &[Entry]) -> crate::multiraft::error::Result<()> {
        self.core.wl().append(ents).map_err(|err| Error::Raft(err))
    }

    fn apply_snapshot(&self, snapshot: Snapshot) -> crate::multiraft::error::Result<()> {
        self.core
            .wl()
            .apply_snapshot(snapshot)
            .map_err(|err| Error::Raft(err))
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

impl RaftStorage for RaftMemStorage {}

#[derive(Clone)]
pub struct MultiRaftMemoryStorage {
    node_id: u64,
    groups: Arc<AsyncRwLock<HashMap<u64, RaftMemStorage>>>,
    group_desc_map: Arc<AsyncRwLock<HashMap<u64, RaftGroupDesc>>>,
}

impl MultiRaftMemoryStorage {
    pub fn new(node_id: u64) -> Self {
        Self {
            node_id,
            groups: Default::default(),
            group_desc_map: Default::default(),
        }
    }
}

impl MultiRaftStorage<RaftMemStorage> for MultiRaftMemoryStorage {
    // type CreateGroupStorageWithConfStateFuture<'life0, T> = impl Future<Output = Result<RaftMemStorage>> + 'life0
    //     where
    //         Self: 'life0,
    //         ConfState: From<T>,
    //         T: Send + 'life0;
    // #[allow(unused)]
    // fn create_group_storage_with_conf_state<'life0, T>(
    //     &'life0 self,
    //     group_id: u64,
    //     replica_id: u64,
    //     conf_state: T,
    // ) -> Self::CreateGroupStorageWithConfStateFuture<'life0, T>
    // where
    //     ConfState: From<T>,
    //     T: Send,
    // {
    //     async move {
    //         let mut wl = self.groups.write().await;
    //         assert_ne!(wl.contains_key(&group_id), true);
    //         let storage = RaftMemStorage::new_with_conf_state(conf_state);
    //         wl.insert(group_id, storage.clone());
    //         Ok(storage)
    //     }
    // }

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

    type SetGroupDescFuture<'life0> = impl Future<Output = Result<()>> + 'life0
    where
        Self: 'life0;
    fn set_group_desc(
        &self,
        group_id: u64,
        group_desc: RaftGroupDesc,
    ) -> Self::SetGroupDescFuture<'_> {
        async move {
            let mut wl = self.group_desc_map.write().await;
            wl.insert(group_id, group_desc);
            return Ok(());
        }
    }

    type GroupDescFuture<'life0> = impl Future<Output = Result<RaftGroupDesc>> + 'life0
    where
        Self: 'life0;
    fn group_desc(&self, group_id: u64) -> Self::GroupDescFuture<'_> {
        async move {
            let mut wl = self.group_desc_map.write().await;
            return match wl.get_mut(&group_id) {
                Some(desc) => Ok(desc.clone()),
                None => {
                    let mut desc = RaftGroupDesc::default();
                    desc.group_id = group_id;
                    wl.insert(group_id, desc.clone());
                    Ok(desc)
                }
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
            let mut wl = self.group_desc_map.write().await;
            return match wl.get_mut(&group_id) {
                Some(desc) => {
                    if desc.replicas.iter().find(|r| **r == replica_desc).is_some() {
                        return Ok(());
                    }
                    // invariant: if replica_desc are not present in the raft group desc,
                    // then replica_desc.node_id not present in the nodes
                    desc.nodes.push(replica_desc.node_id);
                    desc.replicas.push(replica_desc);
                    Ok(())
                }
                None => {
                    // if raft group desc is none, a new one is created in storage.
                    let mut desc = RaftGroupDesc::default();
                    desc.group_id = group_id;
                    desc.nodes.push(replica_desc.node_id);
                    desc.replicas.push(replica_desc);
                    wl.insert(group_id, desc.clone());
                    Ok(())
                }
            };
        }
    }

    type ReplicaDescFuture<'life0> = impl Future<Output = Result<Option<ReplicaDesc>>> + 'life0
    where
        Self: 'life0;
    fn replica_desc(&self, group_id: u64, replica_id: u64) -> Self::ReplicaDescFuture<'_> {
        async move {
            let mut wl = self.group_desc_map.write().await;
            return match wl.get_mut(&group_id) {
                Some(desc) => {
                    if let Some(replica) = desc.replicas.iter().find(|r| r.replica_id == replica_id)
                    {
                        return Ok(Some(replica.clone()));
                    }
                    Ok(None)
                }
                None => Ok(None),
            };
        }
    }

    type ReplicaForNodeFuture<'life0> = impl Future<Output = Result<Option<ReplicaDesc>>> + 'life0
    where
        Self: 'life0;

    fn replica_for_node(&self, group_id: u64, node_id: u64) -> Self::ReplicaForNodeFuture<'_> {
        async move {
            let mut wl = self.group_desc_map.write().await;
            return match wl.get_mut(&group_id) {
                Some(desc) => {
                    if let Some(replica) = desc.replicas.iter().find(|r| r.node_id == node_id) {
                        return Ok(Some(replica.clone()));
                    }
                    Ok(None)
                }
                None => Ok(None),
            };
        }
    }
}
