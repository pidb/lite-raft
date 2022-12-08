use std::sync::Arc;

use futures::Future;
use rocksdb::rocksdb_options::ColumnFamilyDescriptor;
use rocksdb::ColumnFamilyOptions;
use rocksdb::DBOptions;
use rocksdb::DB;

use crate::proto::ConfState;
use crate::storage::MultiRaftStorage;
use crate::storage::RaftSnapshotBuilder;
use crate::storage::RaftStorage;
use crate::storage::RaftStorageImpl;

pub struct Config {
    storage_path: String,
}

const METADATA_CF_NAME: &'static str = "metadta_cf";
const RAFT_LOG_CF_NAME: &'static str = "raft_log_cf";

#[derive(Clone)]
pub struct RocksdbStorage {}

impl RaftStorage for RocksdbStorage {
    fn initial_state(&self) -> super::Result<super::RaftState> {
        unimplemented!()
    }

    fn append_entries(&self, entries: &Vec<crate::proto::Entry>) -> super::Result<()> {
        unimplemented!()
    }

    fn apply_snapshot(&self, snapshot: crate::proto::Snapshot) -> super::Result<()> {
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

    fn get_hard_state(&self) -> super::Result<crate::proto::HardState> {
        unimplemented!()
    }

    fn last_index(&self) -> super::Result<u64> {
        unimplemented!()
    }

    fn set_commit(&self, commit: u64) {
        unimplemented!()
    }

    fn set_hardstate(&self, hs: crate::proto::HardState) {
        unimplemented!()
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
        let mut cfds = vec![];
        let metadata_cf_opts = ColumnFamilyOptions::default();
        let metadata_cfd = ColumnFamilyDescriptor::new(METADATA_CF_NAME, metadata_cf_opts);
        cfds.push(metadata_cfd);

        let raft_log_cf_opts = ColumnFamilyOptions::default();
        let raft_log_cfd = ColumnFamilyDescriptor::new(RAFT_LOG_CF_NAME, raft_log_cf_opts);
        cfds.push(raft_log_cfd);

        let db = DB::open_cf(DBOptions::default(), &cfg.storage_path, cfds).unwrap();
        Self { 
            store_id: 0,
            db: Arc::new(db) 
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
        async { unimplemented!() }
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
        async { unimplemented!() }
    }

    fn replica_metadata(&self, group_id: u64, replica_id: u64) -> Self::ReplicaMetadataFuture<'_> {
        async { unimplemented!() }
    }
}
