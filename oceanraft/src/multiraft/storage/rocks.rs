mod storage {
    use std::sync::Arc;

    use futures::Future;
    use prost::Message;
    use raft::RaftState;
    use raft::Result as RaftResult;
    use rocksdb::BoundColumnFamily;
    use rocksdb::ColumnFamilyDescriptor;
    use rocksdb::DBWithThreadMode;
    use rocksdb::IteratorMode;
    use rocksdb::MultiThreaded;
    use rocksdb::Options as RocksdbOptions;
    use rocksdb::ReadOptions;
    use rocksdb::WriteBatch;
    use rocksdb::WriteOptions;

    use crate::prelude::ConfState;
    use crate::prelude::Entry;
    use crate::prelude::HardState;
    use crate::prelude::ReplicaDesc;
    use crate::prelude::Snapshot;

    use crate::multiraft::storage::Error;
    use crate::multiraft::storage::MultiRaftStorage;
    use crate::multiraft::storage::RaftSnapshotReader;
    use crate::multiraft::storage::RaftSnapshotWriter;
    use crate::multiraft::storage::RaftStorage;
    use crate::multiraft::storage::RaftStorageReader;
    use crate::multiraft::storage::RaftStorageWriter;
    use crate::multiraft::storage::Result;

    use super::state_machine::RockStateMachine;

    const METADATA_CF_NAME: &'static str = "metadta_cf";
    const LOG_CF_NAME: &'static str = "raft_log_cf";

    const HARD_STATE_PREFIX: &'static str = "hs";
    const CONF_STATE_PREFIX: &'static str = "cs";
    const GROUP_STORE_PREFIX: &'static str = "gs";
    const REPLICA_DESC_PREFIX: &'static str = "rd";

    const LOG_EMPTY_FLAG: &'static str = "log_empty";
    const LOG_FIRST_INDEX_PREFIX: &'static str = "fidx";
    const LOG_LAST_INDEX_PREFIX: &'static str = "lidx";

    // const SNAPSHOT_METADATA_PREFIX: &str = "sm";

    type MDB = DBWithThreadMode<MultiThreaded>;

    #[derive(thiserror::Error, Debug)]
    pub enum RocksError {
        #[error("log column family not found")]
        LogColumnFamilyNotExists,

        #[error("log column family not found")]
        MetaColumnFamilyNotExists,

        /// Some other error occurred.
        #[error("error {0}")]
        Other(#[from] Box<dyn std::error::Error + Sync + Send>),
    }

    /// A lightweight helper method for mdb
    struct DBEnv;

    impl DBEnv {
        #[inline]
        fn get_metadata_cf(db: &Arc<MDB>) -> Result<Arc<BoundColumnFamily>> {
            db.cf_handle(METADATA_CF_NAME).map_or(
                Err(Error::Other(Box::new(
                    RocksError::MetaColumnFamilyNotExists,
                ))),
                |cf| Ok(cf),
            )
        }

        #[inline]
        fn get_log_cf(db: &Arc<MDB>) -> Result<Arc<BoundColumnFamily>> {
            db.cf_handle(LOG_CF_NAME).map_or(
                Err(Error::Other(Box::new(RocksError::LogColumnFamilyNotExists))),
                |cf| Ok(cf),
            )
        }

        #[inline]
        fn hardstate_key(group_id: u64, replica_id: u64) -> String {
            format!("{}_{}_{}", group_id, replica_id, HARD_STATE_PREFIX)
        }

        #[inline]
        fn confstate_key(group_id: u64, replica_id: u64) -> String {
            format!("{}_{}_{}", group_id, replica_id, CONF_STATE_PREFIX)
        }

        #[inline]
        fn empty_logs_flag_key(group_id: u64, replica_id: u64) -> String {
            format!("{}_{}_{}", group_id, replica_id, LOG_EMPTY_FLAG)
        }

        #[inline]
        fn replica_desc_key(group_id: u64, replica_id: u64) -> String {
            format!("{}_{}_{}", REPLICA_DESC_PREFIX, group_id, replica_id)
        }

        #[inline]
        fn set_hardstate_with_batch(
            group_id: u64,
            replica_id: u64,
            hs: HardState,
            meta_cf: &Arc<BoundColumnFamily>,
            batch: &mut WriteBatch,
        ) {
            let key = Self::hardstate_key(group_id, replica_id);
            let value = hs.encode_to_vec();
            batch.put_cf(meta_cf, key, value)
        }

        #[inline]
        fn set_confstate_with_batch(
            group_id: u64,
            replica_id: u64,
            cs: ConfState,
            meta_cf: &Arc<BoundColumnFamily>,
            batch: &mut WriteBatch,
        ) {
            let key = Self::confstate_key(group_id, replica_id);
            let value = cs.encode_to_vec();
            batch.put_cf(meta_cf, key, value)
        }
        #[inline]
        fn set_empty_logs_flag(
            group_id: u64,
            replica_id: u64,
            flag: bool,
            log_cf: &Arc<BoundColumnFamily>,
            db: &Arc<MDB>,
        ) -> Result<()> {
            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);
            db.put_cf_opt(
                log_cf,
                Self::empty_logs_flag_key(group_id, replica_id),
                flag.to_string(),
                &writeopts,
            )
            .map_err(|err| Error::Other(Box::new(err)))
        }

        #[inline]
        fn set_empty_logs_flag_with_batch(
            group_id: u64,
            replica_id: u64,
            log_cf: &Arc<BoundColumnFamily>,
            batch: &mut WriteBatch,
        ) {
            let key = Self::empty_logs_flag_key(group_id, replica_id);
            let value = "true".as_bytes();
            batch.put_cf(log_cf, key, value)
        }
    }

    struct LogMetadata {
        first_index: u64,
        last_index: u64,
        empty: bool,
    }

    #[derive(Clone)]
    pub(crate) struct RockStoreCore<SR: RaftSnapshotReader, SW: RaftSnapshotWriter> {
        group_id: u64,
        replica_id: u64,
        db: Arc<MDB>,
        rsnap: Arc<SR>,
        wsnap: Arc<SW>,
    }

    impl<SR: RaftSnapshotReader, SW: RaftSnapshotWriter> RockStoreCore<SR, SW> {
        #[inline]
        fn first_index_key(&self) -> String {
            format!(
                "{}_{}_{}",
                self.group_id, self.replica_id, LOG_FIRST_INDEX_PREFIX
            )
        }

        #[inline]
        fn last_index_key(&self) -> String {
            format!(
                "{}_{}_{}",
                self.group_id, self.replica_id, LOG_LAST_INDEX_PREFIX
            )
        }

        // #[inline]
        // fn empty_logs_flag_key(&self) -> String {
        //     format!("{}_{}_{}", self.group_id, self.replica_id, LOG_EMPTY_FLAG)
        // }

        fn is_empty(&self, log_cf: &Arc<BoundColumnFamily>) -> Result<bool> {
            let key = DBEnv::empty_logs_flag_key(self.group_id, self.replica_id);
            let readopts = ReadOptions::default();
            let value = self
                .db
                .get_cf_opt(log_cf, key, &readopts)
                .map_err(|err| Error::Other(Box::new(RocksError::Other(Box::new(err)))))?
                .unwrap();

            match String::from_utf8(value).unwrap().as_str() {
                "true" => Ok(true),
                "false" => Ok(false),
                _ => unreachable!(),
            }
        }

        // #[inline]
        // fn snapshot_metadata_key(&self) -> String {
        //     format!(
        //         "{}_{}_{}",
        //         self.group_id, self.replica_id, SNAPSHOT_METADATA_PREFIX
        //     )
        // }

        // #[inline]
        // fn hardstate_key(&self) -> String {
        //     format!(
        //         "{}_{}_{}",
        //         self.group_id, self.replica_id, HARD_STATE_PREFIX
        //     )
        // }

        // #[inline]
        // fn confstate_key(&self) -> String {
        //     format!(
        //         "{}_{}_{}",
        //         self.group_id, self.replica_id, CONF_STATE_PREFIX
        //     )
        // }

        /// Format raft log entry prefix of key  with mode `ent_{group_id}/`.
        #[inline]
        fn format_entry_key_prefix(&self) -> String {
            format!("ent_{}/", self.group_id)
        }

        /// Format raft log entry key with mode `ent_{group_id}/{raw_key}`.
        #[inline]
        fn format_entry_key(&self, index: u64) -> String {
            format!("ent_{}/{}", self.group_id, index)
        }

        // fn snapshot_metadata(&self, meta_cf: &Arc<BoundColumnFamily>) -> Result<SnapshotMetadata> {
        //     let key = self.empty_logs_flag_key();
        //     let readopts = ReadOptions::default();
        //     self.db
        //         .get_cf_opt(meta_cf, key, &readopts)
        //         .map_err(|err| Error::Storage(StorageError::Other(Box::new(err))))?
        //         .map_or_else(
        //             || panic!(""), // TODO: add more info
        //             |data| {
        //                 let mut sm = SnapshotMetadata::default();
        //                 sm.merge(data.as_ref()).unwrap(); // TODO: handle error
        //                 Ok(sm)
        //             },
        //         )
        // }

        fn get_log_metaedata(&self) -> Result<LogMetadata> {
            let cf = DBEnv::get_log_cf(&self.db)?;

            let keys = vec![
                DBEnv::empty_logs_flag_key(self.group_id, self.replica_id),
                self.first_index_key(),
                self.last_index_key(),
            ];

            let readopts = ReadOptions::default();

            let mut batchs = self
                .db
                .batched_multi_get_cf_opt(&cf, &keys, false, &readopts)
                .into_iter();
            assert_eq!(batchs.len(), keys.len());

            let empty = batchs
                .next()
                .unwrap()
                .map_err(|err| Error::Other(Box::new(err)))?
                .map_or_else(
                    || panic!(""), // TODO: panic with some info
                    |data| match String::from_utf8(data.to_vec()).unwrap().as_str() {
                        "true" => true,
                        "false" => false,
                        _ => unreachable!(),
                    },
                );

            if empty {
                // let meta_cf = self.get_metadata_cf().unwrap();
                let snap_meta = self
                    .rsnap
                    .snapshot_metadata(self.group_id, self.replica_id)
                    .unwrap();
                return Ok(LogMetadata {
                    first_index: snap_meta.index + 1,
                    last_index: snap_meta.index,
                    empty,
                });
            }

            let first_index = batchs
                .next()
                .unwrap()
                .map_err(|err| Error::Other(Box::new(err)))?
                .map_or(0, |data| {
                    u64::from_be_bytes(data.to_vec().try_into().unwrap())
                });

            let last_index = batchs
                .next()
                .unwrap()
                .map_err(|err| Error::Other(Box::new(err)))?
                .map_or(0, |data| {
                    u64::from_be_bytes(data.to_vec().try_into().unwrap())
                });

            Ok(LogMetadata {
                first_index,
                last_index,
                empty,
            })
        }

        fn first_entry_index(&self) -> Result<u64> {
            let log_cf = DBEnv::get_log_cf(&self.db)?;
            let key = self.first_index_key();
            let readopts = ReadOptions::default();
            let value = self
                .db
                .get_cf_opt(&log_cf, &key, &readopts)
                .map_err(|err| Error::Other(Box::new(err)))?
                .unwrap();
            let idx = u64::from_be_bytes(value.try_into().unwrap());
            Ok(idx)
        }

        fn last_entry_index(&self) -> Result<u64> {
            let log_cf = DBEnv::get_log_cf(&self.db)?;
            let key = self.last_index_key();
            let readopts = ReadOptions::default();
            let value = self
                .db
                .get_cf_opt(&log_cf, &key, &readopts)
                .map_err(|err| Error::Other(Box::new(err)))?
                .unwrap();
            let idx = u64::from_be_bytes(value.try_into().unwrap());
            Ok(idx)
        }

        #[inline]
        fn set_hardstate_with_batch(&self, cf: &Arc<BoundColumnFamily>, batch: &mut WriteBatch) {
            let key = DBEnv::hardstate_key(self.group_id, self.replica_id);
            let value = HardState::default().encode_to_vec();
            batch.put_cf(cf, key, value)
        }

        #[inline]
        fn set_confstate_with_batch(&self, cf: &Arc<BoundColumnFamily>, batch: &mut WriteBatch) {
            let key = DBEnv::hardstate_key(self.group_id, self.replica_id);
            let value = ConfState::default().encode_to_vec();
            batch.put_cf(cf, key, value)
        }

        #[inline]
        fn set_empty_logs_flag_with_batch(
            &self,
            cf: &Arc<BoundColumnFamily>,
            batch: &mut WriteBatch,
        ) {
            let key = DBEnv::empty_logs_flag_key(self.group_id, self.replica_id);
            let value = "true".as_bytes();
            batch.put_cf(cf, key, value)
        }
    }

    /*****************************************************************************
     * TESTS METHOD
     *****************************************************************************/
    impl<SR: RaftSnapshotReader, SW: RaftSnapshotWriter> RockStoreCore<SR, SW> {
        pub(crate) fn append_unchecked(&self, ents: &[Entry]) {
            if ents.is_empty() {
                return;
            }

            let log_cf = DBEnv::get_log_cf(&self.db).unwrap();
            let log_meta = self.get_log_metaedata().unwrap();

            let mut batch = WriteBatch::default();
            if log_meta.empty {
                // set first index
                let key = self.first_index_key();
                let value = ents[0].index.to_be_bytes();
                batch.put_cf(&log_cf, key, value);

                // set not empty
                let key = DBEnv::empty_logs_flag_key(self.group_id, self.replica_id);
                let value = "false".as_bytes();
                batch.put_cf(&log_cf, key, value);
            }

            for ent in ents.iter() {
                let key = self.format_entry_key(ent.index);
                // TODO: use feature to use difference ser
                let value = ent.encode_to_vec();
                batch.put_cf(&log_cf, key, value);
            }

            // set last index
            let key = self.last_index_key();
            let value = ents.last().unwrap().index.to_be_bytes();
            batch.put_cf(&log_cf, key, value);

            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);
            self.db.write_opt(batch, &writeopts).unwrap();
        }

        pub fn entries_unchecked(&self) -> Vec<Entry> {
            let mut ents = vec![];
            let log_cf = DBEnv::get_log_cf(&self.db).unwrap();
            let prefix = self.format_entry_key_prefix();
            let iter_mode = IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Forward);
            let readopts = ReadOptions::default();
            let iter = self.db.iterator_cf_opt(&log_cf, readopts, iter_mode);
            for ent in iter {
                let (key_data, value_data) = ent.unwrap();
                let key = std::str::from_utf8(&key_data).unwrap();
                if !key.contains("ent_") {
                    break;
                }
                let ent = Entry::decode(value_data.as_ref()).unwrap();
                ents.push(ent);
            }

            ents
        }
    }

    impl<SR: RaftSnapshotReader, SW: RaftSnapshotWriter> RaftStorageReader for RockStoreCore<SR, SW> {
        fn initial_state(&self) -> RaftResult<RaftState> {
            let cf = DBEnv::get_metadata_cf(&self.db)?;
            let readopts = ReadOptions::default();
            let keys = vec![
                DBEnv::hardstate_key(self.group_id, self.replica_id),
                DBEnv::confstate_key(self.group_id, self.replica_id),
            ];

            let mut batches = self
                .db
                .batched_multi_get_cf_opt(&cf, &keys, false, &readopts)
                .into_iter();
            assert_eq!(batches.len(), keys.len());

            let hard_state = batches
                .next()
                .unwrap()
                .map_err(|err| Error::Other(Box::new(err)))?
                .map_or(HardState::default(), |data| {
                    let mut hs = HardState::default();
                    hs.merge(data.as_ref()).unwrap(); // TODO: handle error
                    hs
                });

            let conf_state = batches
                .next()
                .unwrap()
                .map_err(|err| Error::Other(Box::new(err)))?
                .map_or(ConfState::default(), |data| {
                    let mut cs = ConfState::default();
                    cs.merge(data.as_ref()).unwrap(); // TODO: handle error
                    cs
                });

            Ok(RaftState {
                hard_state,
                conf_state,
            })
        }

        fn entries(
            &self,
            low: u64,
            high: u64,
            max_size: impl Into<Option<u64>>,
            _context: raft::GetEntriesContext,
        ) -> RaftResult<Vec<raft::prelude::Entry>> {
            let log_meta = self
                .get_log_metaedata()
                .map_err(|err| raft::Error::Store(raft::StorageError::Other(Box::new(err))))?;

            if low < log_meta.first_index {
                return Err(raft::Error::Store(raft::StorageError::Compacted));
            }

            if high > log_meta.last_index + 1 {
                panic!(
                    "index out of bound (last: {}, high: {})",
                    log_meta.last_index + 1,
                    high
                )
            }

            let mut ents = Vec::with_capacity((high - low) as usize);

            let log_cf = DBEnv::get_log_cf(&self.db)?; // TODO handle error
            let start_key = self.format_entry_key(low);
            let last_key = self.format_entry_key(high);
            let iter_mode = IteratorMode::From(start_key.as_bytes(), rocksdb::Direction::Forward);
            let mut readopts = ReadOptions::default();
            // skip delete_range to improve read performance
            readopts.set_ignore_range_deletions(true);
            // TODO: handle if temporaily unavailable
            let iter = self.db.iterator_cf_opt(&log_cf, readopts, iter_mode);
            for ent in iter {
                let (key_data, value_data) = ent.unwrap();
                if last_key.as_bytes() == key_data.as_ref() {
                    break;
                }

                let ent = Entry::decode(value_data.as_ref()).unwrap(); // TODO: handle error
                ents.push(ent);
            }

            raft::util::limit_size(&mut ents, max_size.into());

            Ok(ents)
        }

        fn term(&self, idx: u64) -> RaftResult<u64> {
            // let meta_cf = self.get_metadata_cf().unwrap();
            let snap_meta = self
                .rsnap
                .snapshot_metadata(self.group_id, self.replica_id)
                .unwrap();
            if idx == snap_meta.index {
                return Ok(snap_meta.term);
            }

            let log_meta = self.get_log_metaedata().unwrap();
            if idx < log_meta.first_index {
                return Err(raft::Error::Store(raft::StorageError::Compacted));
            }

            if idx > log_meta.last_index {
                return Err(raft::Error::Store(raft::StorageError::Unavailable));
            }

            let log_cf = DBEnv::get_log_cf(&self.db)?;
            let key = self.format_entry_key(idx);
            let readopts = ReadOptions::default();
            let value = self
                .db
                .get_cf_opt(&log_cf, &key, &readopts)
                .unwrap()
                .unwrap();
            let ent = Entry::decode(value.as_ref()).unwrap();
            Ok(ent.term)
        }

        fn first_index(&self) -> RaftResult<u64> {
            let log_cf = DBEnv::get_log_cf(&self.db)?;
            match self.is_empty(&log_cf)? {
                true => {
                    let snap_meta = self
                        .rsnap
                        .snapshot_metadata(self.group_id, self.replica_id)?;
                    Ok(snap_meta.index + 1)
                }
                false => {
                    let key = self.first_index_key();
                    let readopts = ReadOptions::default();
                    let value = self
                        .db
                        .get_cf_opt(&log_cf, &key, &readopts)
                        .unwrap()
                        .unwrap();
                    let idx = u64::from_be_bytes(value.try_into().unwrap());
                    Ok(idx)
                }
            }
        }

        fn last_index(&self) -> RaftResult<u64> {
            let log_cf = DBEnv::get_log_cf(&self.db)?;
            match self.is_empty(&log_cf)? {
                true => {
                    let snap_meta = self
                        .rsnap
                        .snapshot_metadata(self.group_id, self.replica_id)?;
                    Ok(snap_meta.index)
                }
                false => {
                    let key = self.last_index_key();
                    let readopts = ReadOptions::default();
                    let value = self
                        .db
                        .get_cf_opt(&log_cf, &key, &readopts)
                        .unwrap()
                        .unwrap();
                    let idx = u64::from_be_bytes(value.try_into().unwrap());
                    Ok(idx)
                }
            }
        }

        fn snapshot(&self, request_index: u64, to: u64) -> RaftResult<Snapshot> {
            self.rsnap
                .load_snapshot(self.group_id, self.replica_id, request_index, to)
                .map_err(|err| err.into())
        }
    }

    impl<SR: RaftSnapshotReader, SW: RaftSnapshotWriter> RaftStorageWriter for RockStoreCore<SR, SW> {
        fn set_hardstate(&self, hs: HardState) -> Result<()> {
            let metacf = DBEnv::get_metadata_cf(&self.db)?;
            let key = DBEnv::hardstate_key(self.group_id, self.replica_id);
            // TODO: add feature for difference serializers.
            let value = hs.encode_to_vec();
            let writeopts = WriteOptions::default();
            self.db
                .put_cf_opt(&metacf, &key, &value, &writeopts)
                .unwrap();
            Ok(())
        }

        fn set_confstate(&self, cs: ConfState) -> Result<()> {
            let metacf = DBEnv::get_metadata_cf(&self.db)?;
            let key = DBEnv::hardstate_key(self.group_id, self.replica_id);
            // TODO: add feature for difference serializers.
            let value = cs.encode_to_vec();
            let writeopts = WriteOptions::default();
            self.db
                .put_cf_opt(&metacf, &key, &value, &writeopts)
                .unwrap();
            Ok(())
        }

        fn append(&self, ents: &[Entry]) -> Result<()> {
            if ents.is_empty() {
                return Ok(());
            }

            let log_meta = self.get_log_metaedata()?;

            if log_meta.first_index > ents[0].index {
                panic!(
                    "overwrite compacted raft logs, compacted: {}, append: {}",
                    log_meta.first_index - 1,
                    ents[0].index,
                )
            }

            if log_meta.last_index + 1 < ents[0].index {
                panic!(
                    "raft logs should be continuous, last index: {}, new append: {}",
                    log_meta.last_index, ents[0].index
                )
            }

            let log_cf = DBEnv::get_log_cf(&self.db)?;

            let mut batch = WriteBatch::default();
            if log_meta.empty {
                // set first index
                let key = self.first_index_key();
                let value = ents[0].index.to_be_bytes();
                batch.put_cf(&log_cf, key, value);

                // set not empty
                let key = DBEnv::empty_logs_flag_key(self.group_id, self.replica_id);
                let value = "false".as_bytes();
                batch.put_cf(&log_cf, key, value);
            }

            // TODO: remove all entries overwritten by ents.

            for ent in ents.iter() {
                let key = self.format_entry_key(ent.index);
                // TODO: use feature to use difference ser
                let value = ent.encode_to_vec();
                batch.put_cf(&log_cf, key, value);
            }

            // set last index
            let key = self.last_index_key();
            let value = ents.last().unwrap().index.to_be_bytes();
            batch.put_cf(&log_cf, key, value);

            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);
            self.db.write_opt(batch, &writeopts).unwrap();
            Ok(())
        }

        fn install_snapshot(&self, snapshot: Snapshot) -> Result<()> {
            self.wsnap
                .save_snapshot(self.group_id, self.replica_id, snapshot)
        }
    }

    impl<SR: RaftSnapshotReader, SW: RaftSnapshotWriter> RaftStorage for RockStoreCore<SR, SW> {
        type SnapshotWriter = SW;
        type SnapshotReader = SR;
    }

    #[derive(Clone)]
    pub struct RockStore<SR, SW>
    where
        SR: RaftSnapshotReader,
        SW: RaftSnapshotWriter,
    {
        node_id: u64,
        db: Arc<MDB>,
        rsnap: Arc<SR>,
        wsnap: Arc<SW>,
    }

    impl<SR, SW> RockStore<SR, SW>
    where
        SR: RaftSnapshotReader,
        SW: RaftSnapshotWriter,
    {
        pub fn new<P>(node_id: u64, path: P, snapshot_reader: SR, snapshot_writer: SW) -> Self
        where
            P: AsRef<std::path::Path>,
        {
            let mut db_opts = RocksdbOptions::default();
            db_opts.create_if_missing(true);
            db_opts.create_missing_column_families(true);

            let mut cfs = vec![];
            let metadata_cf_opts = RocksdbOptions::default();
            cfs.push(ColumnFamilyDescriptor::new(
                METADATA_CF_NAME,
                metadata_cf_opts,
            ));

            let mut raft_log_cf_opts = RocksdbOptions::default();
            cfs.push(ColumnFamilyDescriptor::new(LOG_CF_NAME, raft_log_cf_opts));

            let db = MDB::open_cf_descriptors(&db_opts, &path, cfs).unwrap();
            Self {
                node_id,
                db: Arc::new(db),
                rsnap: Arc::new(snapshot_reader),
                wsnap: Arc::new(snapshot_writer),
            }
        }

        #[inline]
        fn group_store_key(&self, group_id: u64, replica_id: u64) -> String {
            format!("{}_{}_{}", GROUP_STORE_PREFIX, group_id, replica_id)
        }

        fn create_group_store(
            &self,
            group_id: u64,
            replica_id: u64,
        ) -> Result<RockStoreCore<SR, SW>> {
            let meta_cf = DBEnv::get_metadata_cf(&self.db)?;
            let log_cf = DBEnv::get_log_cf(&self.db)?;

            DBEnv::set_empty_logs_flag(group_id, replica_id, true, &log_cf, &self.db)?;

            let mut batch = WriteBatch::default();
            DBEnv::set_hardstate_with_batch(
                group_id,
                replica_id,
                HardState::default(),
                &meta_cf,
                &mut batch,
            );
            DBEnv::set_confstate_with_batch(
                group_id,
                replica_id,
                ConfState::default(),
                &meta_cf,
                &mut batch,
            );
            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);
            self.db
                .write_opt(batch, &writeopts)
                .map_err(|err| Error::Other(Box::new(err)))?;

            Ok(RockStoreCore {
                group_id,
                replica_id,
                db: self.db.clone(),
                rsnap: self.rsnap.clone(),
                wsnap: self.wsnap.clone(),
            })
        }

        pub(crate) fn create_group_store_if_missing(
            &self,
            group_id: u64,
            replica_id: u64,
        ) -> Result<RockStoreCore<SR, SW>> {
            let meta_cf = DBEnv::get_metadata_cf(&self.db)?;
            let key = self.group_store_key(group_id, replica_id);
            let readopts = ReadOptions::default();
            match self
                .db
                .get_cf_opt(&meta_cf, &key, &readopts)
                .map_err(|err| Error::Other(Box::new(err)))?
            {
                Some(_) => Ok(RockStoreCore {
                    group_id,
                    replica_id,
                    db: self.db.clone(),
                    rsnap: self.rsnap.clone(),
                    wsnap: self.wsnap.clone(),
                }),
                None => self.create_group_store(group_id, replica_id),
            }
        }

        fn get_replica_desc(&self, group_id: u64, replica_id: u64) -> Result<Option<ReplicaDesc>> {
            let metacf = DBEnv::get_metadata_cf(&self.db)?;
            let key = DBEnv::replica_desc_key(group_id, replica_id);
            let readopts = ReadOptions::default();

            match self
                .db
                .get_pinned_cf_opt(&metacf, &key, &readopts)
                .map_err(|err| Error::Other(Box::new(err)))?
            {
                Some(data) => {
                    let rd = ReplicaDesc::decode(data.as_ref()).unwrap();
                    Ok(Some(rd))
                }
                None => Ok(None),
            }
        }

        fn set_replica_desc(&self, group_id: u64, rd: &ReplicaDesc) -> Result<()> {
            let metacf = DBEnv::get_metadata_cf(&self.db)?;
            let key = DBEnv::replica_desc_key(group_id, rd.replica_id);
            let value = rd.encode_to_vec();
            let writeopts = WriteOptions::default();
            // TODO: with fsync by config
            self.db
                .put_cf_opt(&metacf, &key, value, &writeopts)
                .map_err(|err| Error::Other(Box::new(err)))
        }

        fn remove_replica_desc(&self, group_id: u64, replica_id: u64) -> Result<()> {
            let metacf = DBEnv::get_metadata_cf(&self.db)?;
            let key = DBEnv::replica_desc_key(group_id, replica_id);
            let writeopts = WriteOptions::default();
            // TODO: with fsync by config
            self.db
                .delete_cf_opt(&metacf, &key, &writeopts)
                .map_err(|err| Error::Other(Box::new(err)))
        }
    }

    impl<SR, SW> MultiRaftStorage<RockStoreCore<SR, SW>> for RockStore<SR, SW>
    where
        SR: RaftSnapshotReader,
        SW: RaftSnapshotWriter,
    {
        type GroupStorageFuture<'life0> = impl Future<Output = Result<RockStoreCore<SR, SW>>> + 'life0
    where
        Self: 'life0;

        fn group_storage(&self, group_id: u64, replica_id: u64) -> Self::GroupStorageFuture<'_> {
            async move { self.create_group_store_if_missing(group_id, replica_id) }
        }

        type ReplicaDescFuture<'life0> = impl Future<Output = Result<Option<ReplicaDesc>>> + 'life0
    where
        Self: 'life0;
        fn get_replica_desc(&self, group_id: u64, replica_id: u64) -> Self::ReplicaDescFuture<'_> {
            async move { self.get_replica_desc(group_id, replica_id) }
        }

        type SetReplicaDescFuture<'life0> = impl Future<Output = Result<()>> + 'life0
    where
        Self: 'life0;
        fn set_replica_desc(
            &self,
            group_id: u64,
            replica_desc: ReplicaDesc,
        ) -> Self::SetReplicaDescFuture<'_> {
            async move { self.set_replica_desc(group_id, &replica_desc) }
        }

        type RemoveReplicaDescFuture<'life0> = impl Future<Output = Result<()>> + 'life0
    where
        Self: 'life0;
        fn remove_replica_desc(
            &self,
            group_id: u64,
            replica_id: u64,
        ) -> Self::RemoveReplicaDescFuture<'_> {
            async move { self.remove_replica_desc(group_id, replica_id) }
        }

        type ReplicaForNodeFuture<'life0> = impl Future<Output = Result<Option<ReplicaDesc>>> + 'life0
    where
        Self: 'life0;

        fn replica_for_node(&self, group_id: u64, node_id: u64) -> Self::ReplicaForNodeFuture<'_> {
            async move {
                let metacf = DBEnv::get_metadata_cf(&self.db)?;
                let seek = format!("{}_{}_", REPLICA_DESC_PREFIX, group_id);
                let iter_mode = IteratorMode::From(seek.as_bytes(), rocksdb::Direction::Forward);
                let readopts = ReadOptions::default();
                let iter = self.db.iterator_cf_opt(&metacf, readopts, iter_mode);
                for item in iter {
                    let (_, value) = item.unwrap();
                    let rd = ReplicaDesc::decode(value.as_ref()).unwrap();
                    if rd.node_id == node_id {
                        return Ok(Some(rd));
                    }
                }

                return Ok(None);
            }
        }
    }
}

mod state_machine {
    use std::collections::BTreeMap;
    use std::marker::PhantomData;
    use std::path::Path;
    use std::sync::Arc;
    use std::vec::IntoIter;

    use futures::Future;
    use rocksdb::BoundColumnFamily;
    use rocksdb::ColumnFamilyDescriptor;
    use rocksdb::DBWithThreadMode;
    use rocksdb::Direction;
    use rocksdb::IteratorMode;
    use rocksdb::MultiThreaded;
    use rocksdb::Options as RocksdbOptions;
    use rocksdb::ReadOptions;
    use rocksdb::WriteBatch;
    use rocksdb::WriteOptions;

    use crate::multiraft::storage::RaftSnapshotReader;
    use crate::multiraft::storage::RaftSnapshotWriter;
    use crate::multiraft::storage::Result as StorageResult;
    use crate::multiraft::types::WriteData;
    use crate::multiraft::Apply;
    use crate::multiraft::StateMachine;
    use crate::multiraft::WriteResponse;
    use crate::prelude::ConfState;
    use crate::prelude::Snapshot;
    use crate::prelude::SnapshotMetadata;

    use super::storage::RockStore;

    type Result<T> = std::result::Result<T, RockStateMachineError>;

    const DATA_CF_NAME: &'static str = "data";
    const META_CF_NAME: &'static str = "meta";
    const SNAP_CF_NAME: &'static str = "snapshot";

    const SNAP_META_POSTIFX: &'static str = "snap_meta";
    const SNAP_DATA_POSTIFX: &'static str = "snap_data";

    const APPLIED_INDEX_PREFIX: &'static str = "applied_index";
    const APPLIED_TERM_PREFIX: &'static str = "applied_term";

    /*****************************************************************************
     * DATA
     *****************************************************************************/
    #[derive(thiserror::Error, Debug)]
    pub enum RockDataError {
        #[error("{0}")]
        Encode(String),
        #[error("{0}")]
        Decode(String),
    }

    #[derive(abomonation_derive::Abomonation)]
    struct GroupKey {
        group_id: u64,
        raw_key: Vec<u8>,
    }

    /// RockData defines a general key-value data structure for applications
    /// to store data to rocksdb. key is a string that needs to meet utf8,
    /// and value can be any byte.
    #[derive(Clone, Default, abomonation_derive::Abomonation)]
    pub struct RockData {
        pub key: String,
        pub value: Vec<u8>,
    }

    impl RockData {
        pub fn decode(bytes: &mut [u8]) -> std::result::Result<Self, RockDataError> {
            let (rocks_data, remaining) = unsafe {
                abomonation::decode::<RockData>(bytes).map_or(
                    Err(RockDataError::Decode("decode failed".to_owned())),
                    |v| Ok(v),
                )?
            };
            assert_eq!(remaining.len(), 0);
            Ok(rocks_data.clone())
        }
    }

    impl WriteData for RockData {
        type EncodeError = RockDataError;

        fn encode(&self) -> std::result::Result<Vec<u8>, Self::EncodeError> {
            let mut bytes = Vec::new();
            unsafe {
                abomonation::encode(self, &mut bytes)
                    .map_err(|err| RockDataError::Encode(err.to_string()))?;
            }

            Ok(bytes)
        }
    }

    /*****************************************************************************
     * SNAPSHOT
     *****************************************************************************/
    #[derive(serde::Serialize, serde::Deserialize, Default)]
    struct RockStateMachineSnapshotMetaSerializer {
        applied_index: u64,
        applied_term: u64,
        voters: Vec<u64>,
        learners: Vec<u64>,
        voters_outgoing: Vec<u64>,
        learners_next: Vec<u64>,
        auto_leave: bool,
    }

    impl<R> TryFrom<(u64, &RockStateMachine<R>)> for RockStateMachineSnapshotMetaSerializer
    where
        R: WriteResponse,
    {
        type Error = RockStateMachineError;
        fn try_from(val: (u64, &RockStateMachine<R>)) -> std::result::Result<Self, Self::Error> {
            let group_id = val.0;
            let state_machine = val.1;
            let (applied_index, applied_term) = state_machine.get_applied(group_id)?;
            let conf_state = state_machine.get_last_conf_state(group_id)?;

            Ok(RockStateMachineSnapshotMetaSerializer {
                applied_index,
                applied_term,
                voters: conf_state.voters,
                learners: conf_state.learners,
                learners_next: conf_state.learners_next,
                voters_outgoing: conf_state.voters_outgoing,
                auto_leave: conf_state.auto_leave,
            })
        }
    }

    #[derive(serde::Serialize, serde::Deserialize, Default)]
    struct RockStateMachineSnapshotDataSerializer {
        bt_map: BTreeMap<String, Vec<u8>>,
    }

    impl<R> TryFrom<(u64, &RockStateMachine<R>)> for RockStateMachineSnapshotDataSerializer
    where
        R: WriteResponse,
    {
        type Error = RockStateMachineError;

        fn try_from(val: (u64, &RockStateMachine<R>)) -> std::result::Result<Self, Self::Error> {
            let group_id = val.0;
            let state_machine = val.1;

            let cf = state_machine.get_data_cf()?;
            let prefix = format!("{}/", group_id);
            let iter_mode = IteratorMode::From(prefix.as_bytes(), Direction::Forward);
            let readopts = ReadOptions::default();
            let iter = state_machine.db.iterator_cf_opt(&cf, readopts, iter_mode);
            let mut data = RockStateMachineSnapshotDataSerializer::default();
            for item in iter {
                let (key, value) = item.unwrap();
                let key = std::str::from_utf8(&key).unwrap();
                match split_data_key(key) {
                    Some((owner_group_id, raw_key)) if owner_group_id == group_id => {
                        data.bt_map.insert(raw_key, value.to_vec());
                    }
                    None | Some(_) => break,
                }
            }

            Ok(data)
        }
    }

    impl From<RockStateMachineSnapshotMetaSerializer> for SnapshotMetadata {
        fn from(ser: RockStateMachineSnapshotMetaSerializer) -> Self {
            let conf_state = Some(ConfState {
                voters: ser.voters,
                voters_outgoing: ser.voters_outgoing,
                learners: ser.learners,
                learners_next: ser.learners_next,
                auto_leave: ser.auto_leave,
            });

            SnapshotMetadata {
                index: ser.applied_index,
                term: ser.applied_term,
                conf_state,
            }
        }
    }

    impl From<&RockStateMachineSnapshotMetaSerializer> for SnapshotMetadata {
        fn from(ser: &RockStateMachineSnapshotMetaSerializer) -> Self {
            let conf_state = Some(ConfState {
                voters: ser.voters.clone(),
                voters_outgoing: ser.voters_outgoing.clone(),
                learners: ser.learners.clone(),
                learners_next: ser.learners_next.clone(),
                auto_leave: ser.auto_leave.clone(),
            });

            SnapshotMetadata {
                index: ser.applied_index,
                term: ser.applied_term,
                conf_state,
            }
        }
    }

    #[derive(Default)]
    struct RockStateMachineSnapshotSerializer {
        meta: RockStateMachineSnapshotMetaSerializer,
        data: RockStateMachineSnapshotDataSerializer,
    }

    impl<R: WriteResponse> TryFrom<(u64, u64, &RockStateMachine<R>)>
        for RockStateMachineSnapshotSerializer
    {
        type Error = RockStateMachineError;

        fn try_from(
            val: (u64, u64, &RockStateMachine<R>),
        ) -> std::result::Result<Self, Self::Error> {
            let group_id = val.0;
            let _replica_id = val.1;
            let state_machine = val.2;

            let meta = RockStateMachineSnapshotMetaSerializer::try_from((group_id, state_machine))?;
            let data = RockStateMachineSnapshotDataSerializer::try_from((group_id, state_machine))?;
            Ok(Self { meta, data })
        }
    }

    impl<R> RaftSnapshotReader for RockStateMachine<R>
    where
        R: WriteResponse,
    {
        fn load_snapshot(
            &self,
            group_id: u64,
            _replica_id: u64,
            _request_index: u64,
            _to: u64,
        ) -> StorageResult<Snapshot> {
            let snapshot = self.get_snapshot(group_id).unwrap(); // TODO: map error
            Ok(snapshot)
        }

        fn snapshot_metadata(
            &self,
            group_id: u64,
            _replica_id: u64,
        ) -> StorageResult<SnapshotMetadata> {
            let meta = self.get_snapshot_meta(group_id).unwrap(); // TODO: map error
            Ok(meta)
        }
    }

    impl<R> RaftSnapshotWriter for RockStateMachine<R>
    where
        R: WriteResponse,
    {
        fn build_snapshot(&self, group_id: u64, replica_id: u64) -> StorageResult<()> {
            let ser =
                RockStateMachineSnapshotSerializer::try_from((group_id, replica_id, self)).unwrap(); // TODO: map error

            self.serialize_snapshot(group_id, ser).unwrap(); // TODO: map error
            Ok(())
        }

        fn save_snapshot(
            &self,
            group_id: u64,
            replica_id: u64,
            snapshot: Snapshot,
        ) -> StorageResult<()> {
            unimplemented!()
        }
    }

    /*****************************************************************************
     * STATE MACHINE
     *****************************************************************************/
    #[derive(thiserror::Error, Debug)]
    pub enum RockStateMachineError {
        #[error("{0} cloumn family is missing")]
        ColumnFamilyMissing(String),

        #[error("{0}")]
        Other(Box<dyn std::error::Error>),
    }

    #[derive(Clone)]
    pub struct RockStateMachine<R: WriteResponse> {
        node_id: u64,
        db: Arc<DBWithThreadMode<MultiThreaded>>,
        _m: PhantomData<R>,
    }

    impl<R: WriteResponse> RockStateMachine<R> {
        pub fn new<P>(node_id: u64, path: P) -> Self
        where
            P: AsRef<Path>,
        {
            let mut db_opts = RocksdbOptions::default();
            db_opts.create_if_missing(true);
            db_opts.create_missing_column_families(true);

            let mut cfs = vec![];
            cfs.push(ColumnFamilyDescriptor::new(DATA_CF_NAME, db_opts.clone()));

            cfs.push(ColumnFamilyDescriptor::new(META_CF_NAME, db_opts.clone()));

            cfs.push(ColumnFamilyDescriptor::new(SNAP_CF_NAME, db_opts.clone()));

            let db = DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(&db_opts, &path, cfs)
                .unwrap();
            Self {
                node_id,
                db: Arc::new(db),
                _m: PhantomData,
            }
        }

        /// Get snapshot cloumn famly.
        #[inline]
        fn get_snapshot_cf(&self) -> Result<Arc<BoundColumnFamily>> {
            self.db.cf_handle(SNAP_CF_NAME).map_or(
                Err(RockStateMachineError::ColumnFamilyMissing(
                    "snapshot".to_owned(),
                )),
                |cf| Ok(cf),
            )
        }

        /// Get data cloumn famly.
        #[inline]
        fn get_data_cf(&self) -> Result<Arc<BoundColumnFamily>> {
            self.db.cf_handle(DATA_CF_NAME).map_or(
                Err(RockStateMachineError::ColumnFamilyMissing(
                    "data".to_owned(),
                )),
                |cf| Ok(cf),
            )
        }

        /// Get meta cloumn famly.
        #[inline]
        fn get_meta_cf(&self) -> Result<Arc<BoundColumnFamily>> {
            self.db.cf_handle(META_CF_NAME).map_or(
                Err(RockStateMachineError::ColumnFamilyMissing(
                    "meta".to_owned(),
                )),
                |cf| Ok(cf),
            )
        }

        /// Get saved applied index and term
        fn get_applied(&self, group_id: u64) -> Result<(u64, u64)> {
            let cf = self.get_data_cf()?;

            let keys = vec![
                format_applied_index_key(group_id),
                format_applied_term_key(group_id),
            ];

            let readopts = ReadOptions::default();
            let mut iter = self
                .db
                .batched_multi_get_cf_opt(&cf, &keys, false, &readopts)
                .into_iter();
            let index = iter
                .next()
                .unwrap()
                .map_err(|err| RockStateMachineError::Other(Box::new(err)))?
                .map_or(0, |data| {
                    u64::from_be_bytes(data.as_ref().try_into().unwrap())
                });

            let term = iter
                .next()
                .unwrap()
                .map_err(|err| RockStateMachineError::Other(Box::new(err)))?
                .map_or(0, |data| {
                    u64::from_be_bytes(data.as_ref().try_into().unwrap())
                });

            Ok((index, term))
        }

        fn save_last_confstate(&self, group_id: u64, cs: ConfState) -> Result<()> {
            unimplemented!()
        }

        /// Get saved last membership change
        fn get_last_conf_state(&self, group_id: u64) -> Result<ConfState> {
            Ok(ConfState::default()) // TODO: impl me
                                     // unimplemented!()
        }

        /// Get snapshot from rock storage.
        ///
        /// Note: default returned if does exists snapshot metadata.
        fn get_snapshot(&self, group_id: u64) -> Result<Snapshot> {
            let cf = self.get_snapshot_cf()?;
            let readopts = ReadOptions::default();

            let meta_key = snapshot_metadata_key(group_id);
            let meta = self
                .db
                .get_pinned_cf_opt(&cf, &meta_key, &readopts)
                .map_err(|err| RockStateMachineError::Other(Box::new(err)))?;

            if meta.is_none() {
                return Ok(Snapshot::default());
            }

            let meta: RockStateMachineSnapshotMetaSerializer =
                serde_json::from_slice(&meta.unwrap())
                    .map_err(|err| RockStateMachineError::Other(Box::new(err)))?;

            let data_key = snapshot_data_key(group_id);
            let data = self
                .db
                .get_pinned_cf_opt(&cf, &data_key, &readopts)
                .map_err(|err| RockStateMachineError::Other(Box::new(err)))?;

            let data = match data {
                None => vec![],
                Some(data) => data.to_owned(),
            };

            Ok(Snapshot {
                data,
                metadata: Some(SnapshotMetadata::from(meta)),
            })
        }

        /// Get snapshot metadata from rock storage.
        ///
        /// Note: default returned if does exists snapshot metadata.
        fn get_snapshot_meta(&self, group_id: u64) -> Result<SnapshotMetadata> {
            let cf = self.get_snapshot_cf()?;
            let readopts = ReadOptions::default();
            let meta_key = snapshot_metadata_key(group_id);
            let meta = self
                .db
                .get_pinned_cf_opt(&cf, &meta_key, &readopts)
                .map_err(|err| RockStateMachineError::Other(Box::new(err)))?;

            if meta.is_none() {
                return Ok(SnapshotMetadata::default());
            }

            let meta: RockStateMachineSnapshotMetaSerializer =
                serde_json::from_slice(&meta.unwrap())
                    .map_err(|err| RockStateMachineError::Other(Box::new(err)))?;

            Ok(SnapshotMetadata::from(meta))
        }

        /// Save the current serialized to snapshot.
        fn serialize_snapshot(
            &self,
            group_id: u64,
            ser: RockStateMachineSnapshotSerializer,
        ) -> Result<()> {
            let meta = serde_json::to_vec(&ser.meta)
                .map_err(|err| RockStateMachineError::Other(Box::new(err)))?;
            let data = serde_json::to_vec(&ser.data)
                .map_err(|err| RockStateMachineError::Other(Box::new(err)))?;

            self.save_snapshot_metadata(group_id, meta)?;
            self.save_snapshot_data(group_id, data)?;

            Ok(())
        }

        /// Save the current snapshot metadata by raw bytes.
        fn save_snapshot_metadata(&self, group_id: u64, meta: Vec<u8>) -> Result<()> {
            let cf = self.get_snapshot_cf()?;
            let key = snapshot_metadata_key(group_id);
            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);

            self.db
                .put_cf_opt(&cf, key, meta, &writeopts)
                .map_err(|err| RockStateMachineError::Other(Box::new(err)))
        }

        /// Save the current snapshot data by raw bytes.
        fn save_snapshot_data(&self, group_id: u64, data: Vec<u8>) -> Result<()> {
            let cf = self.get_snapshot_cf()?;
            let key = snapshot_data_key(group_id);
            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);

            self.db
                .put_cf_opt(&cf, key, data, &writeopts)
                .map_err(|err| RockStateMachineError::Other(Box::new(err)))
        }
    }

    impl<R> StateMachine<R> for RockStateMachine<R>
    where
        R: WriteResponse,
    {
        type ApplyFuture<'life0> = impl Future<Output =  Option<IntoIter<Apply<R>>>> + 'life0
    where
        Self: 'life0;

        fn apply(
            &self,
            group_id: u64,
            _: &crate::multiraft::GroupState,
            iter: std::vec::IntoIter<crate::multiraft::Apply<R>>,
        ) -> Self::ApplyFuture<'_> {
            async move {
                let cf = self.get_data_cf().unwrap();
                let mut batch = WriteBatch::default();

                let mut applied_index = 0;
                let mut applied_term = 0;

                for apply in iter {
                    match apply {
                        Apply::NoOp(_) => unimplemented!(),
                        Apply::Normal(mut normal) => {
                            let rockdata = RockData::decode(&mut normal.data).unwrap();
                            let key = format_data_key(group_id, &rockdata.key);
                            batch.put_cf(&cf, key.as_bytes(), rockdata.value);
                            applied_index = normal.index;
                            applied_term = normal.term;
                        }
                        Apply::Membership(changes) => unimplemented!(),
                    }
                }

                batch.put_cf(
                    &cf,
                    format_applied_index_key(group_id),
                    applied_index.to_be_bytes(),
                );
                batch.put_cf(
                    &cf,
                    format_applied_term_key(group_id),
                    applied_term.to_be_bytes(),
                );

                let mut writeopts = WriteOptions::default();
                writeopts.set_sync(true);
                self.db.write_opt(batch, &writeopts).unwrap();
                None
            }
        }
    }

    #[inline]
    fn format_applied_index_key(group_id: u64) -> String {
        format!("{}_{}", APPLIED_INDEX_PREFIX, group_id)
    }

    #[inline]
    fn format_applied_term_key(group_id: u64) -> String {
        format!("{}_{}", APPLIED_TERM_PREFIX, group_id)
    }

    #[inline]
    fn snapshot_data_key(group_id: u64) -> String {
        format!("{}/{}", group_id, SNAP_META_POSTIFX)
    }

    #[inline]
    fn snapshot_metadata_key(group_id: u64) -> String {
        format!("{}/{}", group_id, SNAP_META_POSTIFX)
    }

    /// Format data key with mode `{group_id}/{raw_key}`.
    #[inline]
    fn format_data_key(group_id: u64, raw_key: &str) -> String {
        format!("{}/{}", group_id, raw_key)
    }

    /// Split mode `{group_id}/{raw_key}` and return `(group_id, raw_key)`.
    /// Returned None if it doesn't fit the pattern.
    #[inline]
    fn split_data_key(key: &str) -> Option<(u64, String)> {
        let splied = key.split('/').collect::<Vec<_>>();

        if splied.is_empty() || splied.len() != 2 {
            return None;
        }

        let group_id = match splied[0].parse::<u64>() {
            Err(_) => return None,
            Ok(val) => val,
        };

        Some((group_id, splied[1].to_string()))
    }

    #[cfg(test)]
    mod tests {
        use std::env::temp_dir;

        use crate::multiraft::ApplyNormal;
        use crate::multiraft::GroupState;

        use super::RockStateMachine;
        use super::RockStore;
        use super::*;

        #[test]
        fn test_rocksdata_serialize() {
            let data1 = RockData {
                key: "key".to_owned(),
                value: "value".as_bytes().to_owned(),
            };

            let mut encoded = data1.encode().unwrap();
            let data2 = RockData::decode(&mut encoded).unwrap();
            assert_eq!(data1.key, data2.key);
            assert_eq!(data1.value, data2.value);
        }

        #[test]
        fn test_state_machine_split_key() {
            let group_id = 1;
            let raw_key = "raw_key".to_owned();
            let splited = split_data_key(format_data_key(group_id, &raw_key).as_str()).unwrap();
            assert_eq!(splited.0, group_id);
            assert_eq!(splited.1, raw_key);
        }

        async fn apply_to(
            group_id: u64,
            n: usize,
            term: u64,
            state_machine: &RockStateMachine<()>,
        ) {
            let state = GroupState::default();
            let mut applys = vec![];
            for i in 0..n {
                let data = RockData {
                    key: format!("raw_key_{}", i),
                    value: (0..100).map(|_| 1_u8).collect::<Vec<u8>>(),
                }
                .encode()
                .unwrap();

                applys.push(Apply::<()>::Normal(ApplyNormal {
                    group_id,
                    index: i as u64 + 1,
                    term,
                    data,
                    context: None,
                    is_conf_change: false,
                    tx: None,
                }));
            }
            state_machine
                .apply(group_id, &state, applys.into_iter())
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn test_snapshot_serialize_from_db() {
            let node_id = 1;
            let group_id = 1;
            let replica_id = 1;
            // create state machine
            let state_machine_tmp_path = temp_dir().join("oceanraft_state_machine_db");
            let state_machine =
                RockStateMachine::<()>::new(node_id, state_machine_tmp_path.clone());
            println!(
                "🐿 create state machine store {}",
                state_machine_tmp_path.display()
            );

            apply_to(group_id, 100, 1, &state_machine).await;
            apply_to(2, 10, 1, &state_machine).await;
            apply_to(3, 20, 1, &state_machine).await;

            let ser = RockStateMachineSnapshotSerializer::try_from((
                group_id,
                replica_id,
                &state_machine,
            ))
            .unwrap();

            assert_eq!(ser.meta.applied_index, 100);
            assert_eq!(ser.meta.applied_term, 1);
            // TODO: snapshot should wrap to structure.

            for i in 0..100 {
                let key = format!("raw_key_{}", i);
                let value = (0..100).map(|_| 1_u8).collect::<Vec<u8>>();
                assert_eq!(*ser.data.bt_map.get(&key).unwrap(), value);
            }

            // create rock store
            // let store_tmp_path = temp_dir().join("oceanraft_store_db");
            // println!("🚛 create raft store {}", store_tmp_path.display());
            // let rock_store = RockStore::new(
            //     node_id,
            //     store_tmp_path.clone(),
            //     state_machine.clone(),
            //     state_machine.clone(),
            // );

            // drop(rock_store);
            // DBWithThreadMode::<MultiThreaded>::destroy(
            //     &rocksdb::Options::default(),
            //     store_tmp_path.clone(),
            // )
            // .unwrap();
            // println!("🌪 destory raft store {}", state_machine_tmp_path.display());

            drop(state_machine);
            DBWithThreadMode::<MultiThreaded>::destroy(
                &rocksdb::Options::default(),
                state_machine_tmp_path.clone(),
            )
            .unwrap();
            println!(
                "👋 destory state machine store {}",
                state_machine_tmp_path.display()
            );
        }
    }
}
pub use storage::{RockStore, RocksError};

pub use state_machine::{RockData, RockDataError, RockStateMachine, RockStateMachineError};

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::panic;
    use std::panic::AssertUnwindSafe;

    use protobuf::Message as PbMessage;
    use raft::Error as RaftError;
    use raft::GetEntriesContext;
    use raft::Storage;
    use raft::StorageError as RaftStorageError;
    use rocksdb::DBWithThreadMode;
    use rocksdb::MultiThreaded;

    use crate::multiraft::storage::RaftStorageWriter;
    use crate::multiraft::WriteResponse;
    use crate::prelude::Entry;
    use crate::prelude::Snapshot;

    use super::RockStateMachine;
    use super::RockStore;

    fn new_entry(index: u64, term: u64) -> Entry {
        let mut e = Entry::default();
        e.term = term;
        e.index = index;
        e
    }

    fn size_of<T: PbMessage>(m: &Entry) -> u32 {
        m.compute_size()
    }

    fn new_snapshot(index: u64, term: u64, voters: Vec<u64>) -> Snapshot {
        let mut s = Snapshot::default();
        s.mut_metadata().index = index;
        s.mut_metadata().term = term;
        s.mut_metadata().mut_conf_state().voters = voters;
        s
    }

    fn new_state_machine<R: WriteResponse>(node_id: u64) -> RockStateMachine<R> {
        let state_machine_tmp_path = temp_dir().join("oceanraft_state_machine_db");
        DBWithThreadMode::<MultiThreaded>::destroy(
            &rocksdb::Options::default(),
            state_machine_tmp_path.clone(),
        )
        .unwrap();

        // create state machine
        let state_machine = RockStateMachine::<R>::new(node_id, state_machine_tmp_path.clone());

        println!(
            "🐿 create state machine store {}",
            state_machine_tmp_path.display()
        );

        state_machine
    }

    fn new_rockstore<R: WriteResponse>(
        node_id: u64,
        state_machine: &RockStateMachine<R>,
    ) -> RockStore<RockStateMachine<R>, RockStateMachine<R>> {
        let store_tmp_path = temp_dir().join("oceanraft_store_db");
        DBWithThreadMode::<MultiThreaded>::destroy(
            &rocksdb::Options::default(),
            store_tmp_path.clone(),
        )
        .unwrap();

        //  create rock store
        println!("🚛 create raft store {}", store_tmp_path.display());
        let rock_store = RockStore::new(
            node_id,
            store_tmp_path.clone(),
            state_machine.clone(),
            state_machine.clone(),
        );

        rock_store
    }

    #[test]
    fn test_storage_term() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut tests = vec![
            (2, Err(RaftError::Store(RaftStorageError::Compacted))),
            (3, Ok(3)),
            (4, Ok(4)),
            (5, Ok(5)),
            (6, Err(RaftError::Store(RaftStorageError::Unavailable))),
        ];

        let node_id = 1;
        let state_machine = new_state_machine::<()>(1);
        let rock_store = new_rockstore::<()>(node_id, &state_machine);
        let rock_store_core = rock_store.create_group_store_if_missing(1, 1).unwrap();

        for (i, (idx, wterm)) in tests.drain(..).enumerate() {
            rock_store_core.append_unchecked(&ents);

            let t = rock_store_core.term(idx);
            if t != wterm {
                panic!("#{}: expect res {:?}, got {:?}", i, wterm, t);
            }
        }
    }

    #[test]
    fn test_storage_append() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut tests = vec![
            (
                vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)],
                Some(vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)]),
            ),
            (
                vec![new_entry(3, 3), new_entry(4, 6), new_entry(5, 6)],
                Some(vec![new_entry(3, 3), new_entry(4, 6), new_entry(5, 6)]),
            ),
            (
                vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 5),
                ],
                Some(vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 5),
                ]),
            ),
            // overwrite compacted raft logs is not allowed
            (
                vec![new_entry(2, 3), new_entry(3, 3), new_entry(4, 5)],
                None,
            ),
            // truncate the existing entries and append
            (
                vec![new_entry(4, 5)],
                Some(vec![new_entry(3, 3), new_entry(4, 5)]),
            ),
            // direct append
            (
                vec![new_entry(6, 6)],
                Some(vec![
                    new_entry(3, 3),
                    new_entry(4, 4),
                    new_entry(5, 5),
                    new_entry(6, 6),
                ]),
            ),
        ];

        for (i, (entries, wentries)) in tests.drain(..).enumerate() {
            let node_id = 1;
            let state_machine = new_state_machine::<()>(1);
            let rock_store = new_rockstore::<()>(node_id, &state_machine);
            let rock_store_core = rock_store.create_group_store_if_missing(1, 1).unwrap();

            rock_store_core.append_unchecked(&ents);
            let res = panic::catch_unwind(AssertUnwindSafe(|| rock_store_core.append(&entries)));
            if let Some(wentries) = wentries {
                let _ = res.unwrap();
                let e = &rock_store_core.entries_unchecked();
                if *e != wentries {
                    panic!("#{}: want {:?}, entries {:?}", i, wentries, e);
                }
            } else {
                res.unwrap_err();
            }
        }
    }

    #[test]
    fn test_storage_entries() {
        let ents = vec![
            new_entry(3, 3),
            new_entry(4, 4),
            new_entry(5, 5),
            new_entry(6, 6),
        ];
        let max_u64 = u64::max_value();
        let mut tests = vec![
            (
                2,
                6,
                max_u64,
                Err(RaftError::Store(RaftStorageError::Compacted)),
            ),
            (3, 4, max_u64, Ok(vec![new_entry(3, 3)])),
            (4, 5, max_u64, Ok(vec![new_entry(4, 4)])),
            (4, 6, max_u64, Ok(vec![new_entry(4, 4), new_entry(5, 5)])),
            (
                4,
                7,
                max_u64,
                Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)]),
            ),
            // even if maxsize is zero, the first entry should be returned
            (4, 7, 0, Ok(vec![new_entry(4, 4)])),
            // limit to 2
            (
                4,
                7,
                u64::from(size_of::<Entry>(&ents[1]) + size_of::<Entry>(&ents[2])),
                Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            (
                4,
                7,
                u64::from(
                    size_of::<Entry>(&ents[1])
                        + size_of::<Entry>(&ents[2])
                        + size_of::<Entry>(&ents[3]) / 2,
                ),
                Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            (
                4,
                7,
                u64::from(
                    size_of::<Entry>(&ents[1])
                        + size_of::<Entry>(&ents[2])
                        + size_of::<Entry>(&ents[3])
                        - 1,
                ),
                Ok(vec![new_entry(4, 4), new_entry(5, 5)]),
            ),
            // all
            (
                4,
                7,
                u64::from(
                    size_of::<Entry>(&ents[1])
                        + size_of::<Entry>(&ents[2])
                        + size_of::<Entry>(&ents[3]),
                ),
                Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)]),
            ),
        ];

        let node_id = 1;
        let state_machine = new_state_machine::<()>(1);
        let rock_store = new_rockstore::<()>(node_id, &state_machine);
        let rock_store_core = rock_store.create_group_store_if_missing(1, 1).unwrap();

        for (i, (lo, hi, maxsize, wentries)) in tests.drain(..).enumerate() {
            rock_store_core.append_unchecked(&ents);
            let e = rock_store_core.entries(lo, hi, maxsize, GetEntriesContext::empty(false));
            if e != wentries {
                panic!("#{}: expect entries {:?}, got {:?}", i, wentries, e);
            }
        }
    }

    #[test]
    fn test_storage_first_index() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];

        let node_id = 1;
        let state_machine = new_state_machine::<()>(1);
        let rock_store = new_rockstore::<()>(node_id, &state_machine);
        let rock_store_core = rock_store.create_group_store_if_missing(1, 1).unwrap();
        rock_store_core.append_unchecked(&ents);

        assert_eq!(rock_store_core.first_index(), Ok(3));
        // storage.wl().compact(4).unwrap();
        // assert_eq!(storage.first_index(), Ok(4));
    }

    #[test]
    fn test_storage_last_index() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];

        let node_id = 1;
        let state_machine = new_state_machine::<()>(1);
        let rock_store = new_rockstore::<()>(node_id, &state_machine);
        let rock_store_core = rock_store.create_group_store_if_missing(1, 1).unwrap();

        rock_store_core.append_unchecked(&ents);

        let wresult = Ok(5);
        let result = rock_store_core.last_index();
        if result != wresult {
            panic!("want {:?}, got {:?}", wresult, result);
        }

        rock_store_core.append(&[new_entry(6, 5)]).unwrap();
        let wresult = Ok(6);
        let result = rock_store_core.last_index();
        if result != wresult {
            panic!("want {:?}, got {:?}", wresult, result);
        }
    }
}
