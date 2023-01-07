use std::collections::hash_map::HashMap;
use std::sync::Arc;

use crate::storage::MultiRaftStorage;
use futures::Future;
use raft::storage::MemStorage;
use raft_proto::prelude::ConfState;
use raft_proto::prelude::RaftGroupDesc;
use raft_proto::prelude::ReplicaDesc;
use tokio::sync::RwLock as AsyncRwLock;

use super::storage::Result;

#[derive(Clone)]
pub struct MultiRaftMemoryStorage {
    node_id: u64,
    store_id: u64,
    groups: Arc<AsyncRwLock<HashMap<u64, MemStorage>>>,
    group_desc_map: Arc<AsyncRwLock<HashMap<u64, RaftGroupDesc>>>,
}

impl MultiRaftMemoryStorage {
    pub fn new(node_id: u64, store_id: u64) -> Self {
        Self {
            node_id,
            store_id,
            groups: Default::default(),
            group_desc_map: Default::default(),
        }
    }
}

impl MultiRaftStorage<MemStorage> for MultiRaftMemoryStorage {
    type CreateGroupStorageWithConfStateFuture<'life0, T> = impl Future<Output = super::storage::Result<MemStorage>> + 'life0
        where
            Self: 'life0,
            ConfState: From<T>,
            T: Send + 'life0;
    #[allow(unused)]
    fn create_group_storage_with_conf_state<'life0, T>(
        &'life0 self,
        group_id: u64,
        replica_id: u64,
        conf_state: T,
    ) -> Self::CreateGroupStorageWithConfStateFuture<'life0, T>
    where
        ConfState: From<T>,
        T: Send,
    {
        async move {
            let mut wl = self.groups.write().await;
            assert_ne!(wl.contains_key(&group_id), true);
            let storage = MemStorage::new_with_conf_state(conf_state);
            wl.insert(group_id, storage.clone());
            Ok(storage)
        }
    }

    type GroupStorageFuture<'life0> = impl Future<Output = super::storage::Result<MemStorage>> + 'life0
        where
            Self: 'life0;
    #[allow(unused)]
    fn group_storage(&self, group_id: u64, replica_id: u64) -> Self::GroupStorageFuture<'_> {
        async move {
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
