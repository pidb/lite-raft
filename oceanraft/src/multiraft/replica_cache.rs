use std::collections::HashMap;
use std::marker::PhantomData;

use crate::prelude::ReplicaDesc;

use super::error::Error;
use super::storage::MultiRaftStorage;
use super::storage::RaftStorage;

/// ReplicaCache cache replica metadatas
/// from read storage and messages and write the replica metadata the storage
/// when cache miss.
pub struct ReplicaCache<RS, MRS>
where
    RS: RaftStorage,
    MRS: MultiRaftStorage<RS>,
{
    storage: MRS,
    // pub groups: HashMap<u64, RaftGroupDesc>,
    cache: HashMap<u64, Vec<ReplicaDesc>>,
    _m: PhantomData<RS>,
}

impl<RS, MRS> ReplicaCache<RS, MRS>
where
    RS: RaftStorage,
    MRS: MultiRaftStorage<RS>,
{
    pub fn new(storage: MRS) -> Self {
        Self {
            storage,
            // groups: Default::default(),
            cache: HashMap::new(),
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
        match self.cache.get_mut(&group_id) {
            None => {
                let mut rds = vec![];
                if let Some(rd) = self.storage.get_replica_desc(group_id, replica_id).await? {
                    rds.push(rd.clone());
                    self.cache.insert(group_id, rds);
                    return Ok(Some(rd));
                }
                self.cache.insert(group_id, rds);
                return Ok(None);
            }

            Some(rds) => {
                if let Some(find) = ReplicaCache::<RS, MRS>::find_in_cache(rds, |replica| {
                    replica.replica_id == replica_id
                })
                .await
                {
                    return Ok(Some(find));
                }

                if let Some(rd) = self.storage.get_replica_desc(group_id, replica_id).await? {
                    rds.push(rd.clone());
                    return Ok(Some(rd));
                }

                return Ok(None);
            }
        }
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
        match self.cache.get_mut(&group_id) {
            None => {
                let mut rds = vec![];
                if let Some(rd) = self.storage.replica_for_node(group_id, node_id).await? {
                    rds.push(rd.clone());
                    self.cache.insert(group_id, rds);
                    return Ok(Some(rd));
                }
                self.cache.insert(group_id, rds);
                return Ok(None);
            }

            Some(rds) => {
                if let Some(find) = ReplicaCache::<RS, MRS>::find_in_cache(rds, |replica| {
                    replica.node_id == node_id
                })
                .await
                {
                    return Ok(Some(find));
                }

                if let Some(rd) = self.storage.replica_for_node(group_id, node_id).await? {
                    rds.push(rd.clone());
                    return Ok(Some(rd));
                }

                return Ok(None);
            }
        }
    }

    #[inline]
    async fn find_in_cache<P>(replicas: &Vec<ReplicaDesc>, predicate: P) -> Option<ReplicaDesc>
    where
        P: Fn(&&ReplicaDesc) -> bool,
    {
        if replicas.is_empty() {
            return None;
        }

        if let Some(replica) = replicas.iter().find(predicate) {
            return Some(replica.clone());
        }

        None
    }

    /// Cache given replica and `sync` indicates whether syn to storage.
    pub async fn cache_replica_desc(
        &mut self,
        group_id: u64,
        replica_desc: ReplicaDesc,
        sync: bool,
    ) -> Result<(), Error> {
        if let Some(rds) = self.cache.get_mut(&group_id) {
            if rds
                .iter()
                .find(|replica| **replica == replica_desc)
                .is_some()
            {
                return Ok(());
            }

            if sync {
                let _ = self
                    .storage
                    .set_replica_desc(group_id, replica_desc.clone())
                    .await?;
            }

            rds.push(replica_desc);
            return Ok(());
        }

        if sync {
            let _ = self
                .storage
                .set_replica_desc(group_id, replica_desc.clone())
                .await?;
        }
        self.cache.insert(group_id, vec![replica_desc]);
        return Ok(());
    }

    pub async fn remove_replica_desc(
        &mut self,
        group_id: u64,
        replica_desc: ReplicaDesc,
        sync: bool,
    ) -> Result<(), Error> {
        if let Some(rds) = self.cache.get_mut(&group_id) {
            if let Some(index) = rds.iter().position(|replica| *replica == replica_desc) {
                let _ = rds.remove(index);
            }

            if sync {
                let _ = self
                    .storage
                    .remove_replica_desc(group_id, replica_desc.replica_id)
                    .await?;
            }
            return Ok(());
        }

        return Ok(());
    }
}
