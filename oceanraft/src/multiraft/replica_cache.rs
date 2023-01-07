use std::collections::HashMap;
use std::marker::PhantomData;

use super::storage::MultiRaftStorage;
use raft::Storage;
use raft_proto::prelude::RaftGroupDesc;
use raft_proto::prelude::ReplicaDesc;

use super::error::Error;

/// ReplicaCache cache replica metadatas
/// from read storage and messages and write the replica metadata the storage
/// when cache miss.
pub struct ReplicaCache<RS, MRS>
where
    RS: Storage,
    MRS: MultiRaftStorage<RS>,
{
    storage: MRS,
    groups: HashMap<u64, RaftGroupDesc>,
    _m: PhantomData<RS>,
}

impl<RS, MRS> ReplicaCache<RS, MRS>
where
    RS: Storage,
    MRS: MultiRaftStorage<RS>,
{
    pub fn new(storage: MRS) -> Self {
        Self {
            storage,
            groups: Default::default(),
            _m: PhantomData,
        }
    }

    /// Get replica description from this cache when replica_id equals,
    /// return `Error` if storage error occurs. if group is not in the
    /// cache, it is read from storage and if storage is not found too,
    /// `None` is returned.
    pub async fn replica_desc(
        &mut self,
        group_id: u64,
        replica_id: u64,
    ) -> Result<Option<ReplicaDesc>, Error> {
        let _ = self.ensure_cache_group(group_id).await?;
        let group_desc = self.groups.get(&group_id).unwrap();
        Ok(
            ReplicaCache::<RS, MRS>::find(group_desc, |replica| replica.replica_id == replica_id)
                .await,
        )
    }

    /// Get replica description from this cache when node_id equals,
    /// return `Error` if storage error occurs. if group is not in the
    /// cache, it is read from storage and if storage is not found too,
    /// `None` is returned.
    pub async fn replica_for_node(
        &mut self,
        group_id: u64,
        node_id: u64,
    ) -> Result<Option<ReplicaDesc>, Error> {
        let _ = self.ensure_cache_group(group_id).await?;
        let group_desc = self.groups.get(&group_id).unwrap();
        Ok(ReplicaCache::<RS, MRS>::find(group_desc, |replica| replica.node_id == node_id).await)
    }

    /// Cache given replica and `sync` indicates whether syn to storage.
    pub async fn cache_replica_desc(
        &mut self,
        group_id: u64,
        replica_desc: ReplicaDesc,
        sync: bool,
    ) -> Result<(), Error> {
        if let Some(group_desc) = self.groups.get_mut(&group_id) {
            if group_desc
                .replicas
                .iter()
                .find(|replica| **replica == replica_desc)
                .is_some()
            {
                return Ok(());
            }
            // update cache
            group_desc.nodes.push(replica_desc.node_id);
            group_desc.replicas.push(replica_desc);
            if sync {
                let _ = self
                    .storage
                    .set_group_desc(group_id, group_desc.clone())
                    .await?;
            }
            return Ok(());
        }

        let mut group_desc = RaftGroupDesc::default();
        group_desc.nodes.push(replica_desc.node_id);
        group_desc.replicas.push(replica_desc);
        if sync {
            // set new group_desc in storage
            let _ = self
                .storage
                .set_group_desc(group_id, group_desc.clone())
                .await?;
        }
        self.groups.insert(group_id, group_desc);
        return Ok(());
    }

    #[inline]
    async fn ensure_cache_group(&mut self, group_id: u64) -> Result<(), Error> {
        if self.groups.get(&group_id).is_none() {
            let group_desc = self
                .storage
                .group_desc(group_id)
                .await?;
            self.groups.insert(group_id, group_desc);
        }

        Ok(())
    }

    #[inline]
    async fn find<P>(group_desc: &RaftGroupDesc, predicate: P) -> Option<ReplicaDesc>
    where
        P: Fn(&&ReplicaDesc) -> bool,
    {
        if group_desc.replicas.is_empty() {
            return None;
        }

        if let Some(replica) = group_desc.replicas.iter().find(predicate) {
            return Some(replica.clone());
        }

        None
    }
}
