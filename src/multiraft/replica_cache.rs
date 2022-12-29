use std::collections::HashMap;
use std::marker::PhantomData;

use tokio::sync::RwLock;

use crate::proto::ReplicaMetadata;
use crate::storage::MultiRaftStorage;
use crate::storage::RaftStorage;

use super::error::Error;

/// ReplicaMetadataCache is a thread safe cache. it cache replica metadatas
/// from read storage and messages and write the replica metadata the storage
/// when cache miss.
pub struct ReplicaMetadataCache<RS, MRS>
where
    RS: RaftStorage,
    MRS: MultiRaftStorage<RS>,
{
    storage: MRS,
    inner: RwLock<HashMap<(u64, u64), ReplicaMetadata>>,
    _m: PhantomData<RS>,
}

impl<RS, MRS> ReplicaMetadataCache<RS, MRS>
where
    RS: RaftStorage,
    MRS: MultiRaftStorage<RS>,
{
    pub fn new(storage: MRS) -> Self {
        Self {
            storage,
            inner: Default::default(),
            _m: PhantomData,
        }
    }

    pub async fn get(
        &self,
        group_id: u64,
        replica_id: u64,
    ) -> Result<Option<ReplicaMetadata>, Error> {
        {
            let rl = self.inner.read().await;
            if let Some(replica_metadata) = rl.get(&(group_id, replica_id)) {
                return Ok(Some(replica_metadata.clone()));
            }
        }

        match self.storage.replica_metadata(group_id, replica_id).await {
            Err(err) => return Err(Error::Store(err)),
            Ok(rm) => match rm {
                Some(rm) => {
                    self.cache(group_id, rm.clone()).await;
                    Ok(Some(rm))
                }
                None => Ok(None),
            },
        }
    }

    pub async fn cache(&self, group_id: u64, replica_metadata: ReplicaMetadata) {
        assert_ne!(replica_metadata.replica_id, 0);
        let mut wl = self.inner.write().await;
        let key = (group_id, replica_metadata.replica_id);
        match wl.get_mut(&key) {
            None => {
                wl.insert(key, replica_metadata);
            }
            Some(old) => {
                if *old != replica_metadata {
                    std::mem::replace(old, replica_metadata);
                }
            }
        }
    }
}
