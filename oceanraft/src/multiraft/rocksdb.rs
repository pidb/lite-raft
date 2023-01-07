use std::sync::Arc;

use futures::Future;
use prost::Message;
use rocksdb::ColumnFamily;
use rocksdb::ColumnFamilyDescriptor;
use rocksdb::Options;
use rocksdb::ReadOptions;
use rocksdb::WriteOptions;
use rocksdb::DB;

use crate::proto::ConfState;
use crate::proto::HardState;
use crate::proto::ReplicaMetadata;
use crate::storage::MultiRaftStorage;
use crate::storage::RaftSnapshotBuilder;
use crate::storage::RaftStorage;
use crate::storage::RaftStorageImpl;
use crate::storage::Result;
use crate::storage::StorageError;

pub struct Config {
    storage_path: String,
}

const METADATA_CF_NAME: &'static str = "metadta_cf";
const RAFT_LOG_CF_NAME: &'static str = "raft_log_cf";

const RAFT_HARD_STATE_PREFIX: &'static str = "hs";
const RAFT_CONF_STATE_PREFIX: &'static str = "cs";

#[derive(Clone)]
pub struct RocksdbStorage {
    group_id: u64,
    replica_id: u64,
    db: Arc<DB>,
}

#[inline]
fn get_metadata_cf<'a>(db: &'a DB) -> Result<&'a ColumnFamily> {
    db.cf_handle(METADATA_CF_NAME)
        .map_or(Err(StorageError::Unavailable), |cf| Ok(cf))
}

impl RaftStorage for RocksdbStorage {
    fn initial_state(&self) -> super::Result<super::RaftState> {
        unimplemented!()
    }

    fn append_entries(&self, entries: &Vec<crate::proto::Entry>) -> super::Result<()> {
        unimplemented!()
    }

    fn apply_snapshot(&self, snapshot: crate::proto::Snapshot) -> super::Result<()> {
        let mut meta = snapshot.take_metadata();
        let index = meta.index;

        if self.first_index()? > index {
            return Err(StorageError::SnapshotOutOfDate);
        }

        unimplemented!()
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
    ) -> super::Result<Vec<crate::proto::Entry>> {
        unimplemented!()
    }

    fn first_index(&self) -> super::Result<u64> {
        unimplemented!()
    }

    fn last_index(&self) -> super::Result<u64> {
        unimplemented!()
    }

    fn set_commit(&self, commit: u64) {
        unimplemented!()
    }

    /// Get the hard state.
    fn get_hard_state(&self) -> super::Result<crate::proto::HardState> {
        let cf = get_metadata_cf(&self.db)?;

        let key = format!("{}_{}", self.group_id, RAFT_HARD_STATE_PREFIX);

        let pinned_data = self
            .db
            .get_pinned_cf_opt(cf, &key, &ReadOptions::default())
            .map_err(|error| StorageError::Other(Box::new(error)))?;

        pinned_data.map_or(Err(StorageError::Unavailable), |pinned_data| {
            let hs = HardState::decode(pinned_data.as_ref())
                .map_err(|error| StorageError::Other(Box::new(error)))?;
            Ok(hs)
        })
    }

    /// Save the current hard state.
    fn set_hardstate(&self, hs: crate::proto::HardState) -> Result<()> {
        let cf = get_metadata_cf(&self.db)?;

        let key = format!("{}_{}", self.group_id, RAFT_HARD_STATE_PREFIX);

        let mut buf = Vec::with_capacity(hs.encoded_len());
        let _ = hs
            .encode(&mut buf)
            .map_err(|err| StorageError::Other(Box::new(err)))?;

        self.db
            .put_cf_opt(cf, &key, &buf, &WriteOptions::default())
            .map_err(|err| StorageError::Other(Box::new(err)))
    }

    /// Get the conf state.
    fn get_confstate(&self) -> Result<ConfState> {
        let cf = get_metadata_cf(&self.db)?;

        let key = format!("{}_{}", self.group_id, RAFT_HARD_STATE_PREFIX);

        let pinned_data = self
            .db
            .get_pinned_cf_opt(cf, &key, &ReadOptions::default())
            .map_err(|error| StorageError::Other(Box::new(error)))?;

        pinned_data.map_or(Err(StorageError::Unavailable), |pinned_data| {
            let cs = ConfState::decode(pinned_data.as_ref())
                .map_err(|error| StorageError::Other(Box::new(error)))?;
            Ok(cs)
        })
    }

    /// Save the current conf state.
    fn set_confstate(&self, cs: ConfState) -> Result<()> {
        let cf = get_metadata_cf(&self.db)?;

        let key = format!("{}_{}", self.group_id, RAFT_CONF_STATE_PREFIX);

        let mut buf = Vec::with_capacity(cs.encoded_len());
        let _ = cs
            .encode(&mut buf)
            .map_err(|err| StorageError::Other(Box::new(err)))?;

        self.db
            .put_cf_opt(cf, &key, &buf, &WriteOptions::default())
            .map_err(|err| StorageError::Other(Box::new(err)))
    }

    fn snapshot(&self, request_index: u64) -> super::Result<crate::proto::Snapshot> {
        unimplemented!()
    }

    fn term(&self, idx: u64) -> super::Result<u64> {
        unimplemented!()
    }
}

impl RaftSnapshotBuilder for RocksdbStorage {
    fn build_snapshot(&self, applied: u64) -> super::Result<crate::proto::Snapshot> {
        unimplemented!()
    }
}

#[allow(unused)]
#[derive(Clone)]
pub struct MultiRaftRocksdbStorage {
    store_id: u64,
    db: Arc<DB>,
}

#[allow(unused)]
impl MultiRaftRocksdbStorage {
    fn new(cfg: Config) -> Self {
        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let mut cfs = vec![];
        let metadata_cf_opts = Options::default();
        let metadata_cf = ColumnFamilyDescriptor::new(METADATA_CF_NAME, metadata_cf_opts);
        cfs.push(metadata_cf);

        let raft_log_cf_opts = Options::default();
        let raft_log_cfd = ColumnFamilyDescriptor::new(RAFT_LOG_CF_NAME, raft_log_cf_opts);
        cfs.push(raft_log_cfd);

        let db = DB::open_cf_descriptors(&db_opts, &cfg.storage_path, cfs).unwrap();
        Self {
            store_id: 0,
            db: Arc::new(db),
        }
    }
}

impl MultiRaftStorage<RocksdbStorage> for MultiRaftRocksdbStorage {
    type GroupStorageFuture<'life0> = impl Future<Output = super::storage::Result<super::RaftStorageImpl<RocksdbStorage>>>
    where
        Self: 'life0;

    type ReplicaMetadataFuture<'life0> = impl Future<Output =  super::storage::Result<crate::proto::ReplicaMetadata>>
    where
        Self: 'life0;

    type CreateGroupStorageFuture<'life0> = impl Future<Output = super::storage::Result<super::RaftStorageImpl<RocksdbStorage>>>
    where
        Self: 'life0;

    type CreateGroupStorageWithConfStateFuture<'life0, T> = impl Future<Output = super::storage::Result<super::RaftStorageImpl<RocksdbStorage>>>
    where
        Self: 'life0,
        ConfState: From<T>,
        T: Send;

    fn create_group_storage(
        &self,
        group_id: u64,
        replica_id: u64,
    ) -> Self::CreateGroupStorageFuture<'_> {
        async move {
            let write_opts = WriteOptions::default();
            let metadata_cf_handle = self.db.cf_handle(METADATA_CF_NAME).unwrap();

            // store replica metadata
            let key = format!("{}_{}", group_id, replica_id);
            let value = format!("{}", self.store_id);

            self.db
                .put_cf_opt(
                    metadata_cf_handle,
                    key.as_bytes(),
                    value.as_bytes(),
                    &write_opts,
                )
                .unwrap();

            Ok(RaftStorageImpl::new(RocksdbStorage {
                group_id,
                replica_id,
                db: self.db.clone(),
            }))
        }
    }

    fn create_group_storage_with_conf_state<T>(
        &self,
        group_id: u64,
        replica_id: u64,
        conf_state: T,
    ) -> Self::CreateGroupStorageWithConfStateFuture<'_, T>
    where
        ConfState: From<T>,
        T: Send,
    {
        async { unimplemented!() }
    }

    fn group_storage(&self, group_id: u64, replica_id: u64) -> Self::GroupStorageFuture<'_> {
        async move {
            
        }
    }

    fn replica_metadata(&self, group_id: u64, replica_id: u64) -> Self::ReplicaMetadataFuture<'_> {
        async move {
            let cf = get_metadata_cf(&self.db)?;
            let key = format!("{}_{}", group_id, replica_id);

            let pinned_data = match self
                .db
                .get_cf_opt(cf, &key, &ReadOptions::default())
                .map_err(|err| StorageError::Other(Box::new(err)))?
            {
                None => return Err(StorageError::Unavailable),
                Some(pinned) => pinned,
            };

            ReplicaMetadata::decode(pinned_data.as_ref())
                .map_err(|err| StorageError::Other(Box::new(err)))
        }
    }
}
