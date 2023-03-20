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

    use crate::multiraft::storage::Error;
    use crate::multiraft::storage::MultiRaftStorage;
    use crate::multiraft::storage::RaftSnapshotReader;
    use crate::multiraft::storage::RaftSnapshotWriter;
    use crate::multiraft::storage::RaftStorage;
    use crate::multiraft::storage::RaftStorageReader;
    use crate::multiraft::storage::RaftStorageWriter;
    use crate::multiraft::storage::Result;
    use crate::prelude::ConfState;
    use crate::prelude::Entry;
    use crate::prelude::HardState;
    use crate::prelude::ReplicaDesc;
    use crate::prelude::Snapshot;
    use crate::prelude::SnapshotMetadata;

    const METADATA_CF_NAME: &'static str = "metadta_cf";
    const LOG_CF_NAME: &'static str = "raft_log_cf";

    const HARD_STATE_PREFIX: &'static str = "hs";
    const CONF_STATE_PREFIX: &'static str = "cs";

    const LOG_SNAP_META_PREFIX: &'static str = "snap_meta";
    const LOG_EMPTY_PREFIX: &'static str = "log_empty";
    const LOG_FIRST_INDEX_PREFIX: &'static str = "fidx";
    const LOG_LAST_INDEX_PREFIX: &'static str = "lidx";

    const GROUP_STORE_PREFIX: &'static str = "gs";
    const REPLICA_DESC_PREFIX: &'static str = "rd";

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

        /// Format hardstate key with mode `{group_id}_{replica_id}_hs`.
        #[inline]
        fn format_hardstate_key(group_id: u64, replica_id: u64) -> String {
            format!("{}_{}_{}", group_id, replica_id, HARD_STATE_PREFIX)
        }

        /// Format hardstate key with mode `{group_id}_{replica_id}_cs`.
        #[inline]
        fn format_confstate_key(group_id: u64, replica_id: u64) -> String {
            format!("{}_{}_{}", group_id, replica_id, CONF_STATE_PREFIX)
        }

        /// Format log empty flag with mode `empty_{group_id}_{replica_id}`.
        #[inline]
        fn format_empty_key(group_id: u64, replica_id: u64) -> String {
            format!("{}_{}_{}", LOG_EMPTY_PREFIX, group_id, replica_id)
        }

        /// Format log first index key with mode `fidx_{group_id}_{replica_id}`.
        #[inline]
        fn format_first_index_key(group_id: u64, replica_id: u64) -> String {
            format!("{}_{}_{}", LOG_FIRST_INDEX_PREFIX, group_id, replica_id)
        }

        /// Format log last index key with mode `lidx_{group_id}_{replica_id}`.
        #[inline]
        fn format_last_index_key(group_id: u64, replica_id: u64) -> String {
            format!("{}_{}_{}", LOG_LAST_INDEX_PREFIX, group_id, replica_id)
        }

        /// Format log entry index key with mode `ent_{group_id}/{index}`.
        #[inline]
        fn format_entry_key(group_id: u64, index: u64) -> String {
            format!("ent_{}/{}", group_id, index)
        }

        #[inline]
        fn format_entry_key_prefix(group_id: u64) -> String {
            format!("ent_{}/", group_id)
        }

        /// Format snapshot metadata key with mode `snap_meta_{group_id}_{replica_id}/`
        #[inline]
        fn format_snapshot_metadata_key(group_id: u64, replica_id: u64) -> String {
            format!("{}_{}_{}", LOG_SNAP_META_PREFIX, group_id, replica_id)
        }

        #[inline]
        fn replica_desc_key(group_id: u64, replica_id: u64) -> String {
            format!("{}_{}_{}", REPLICA_DESC_PREFIX, group_id, replica_id)
        }
    }

    /// Stored in the `log_cf` column family, representing the
    /// metadata stored in the entries in the db
    struct EntryMetadata {
        first_index: u64,
        last_index: u64,
        empty: bool,
    }

    /*****************************************************************************
     * RockStore Core
     *****************************************************************************/
    #[derive(Clone)]
    pub(crate) struct RockStoreCore<SR: RaftSnapshotReader, SW: RaftSnapshotWriter> {
        group_id: u64,
        replica_id: u64,
        db: Arc<MDB>,
        rsnap: Arc<SR>,
        wsnap: Arc<SW>,
    }

    impl<SR: RaftSnapshotReader, SW: RaftSnapshotWriter> RockStoreCore<SR, SW> {
        /// New and initialze RockStoreCore from db.
        ///
        /// ### Panic: if RockStoreCore has been initialized
        fn new(
            group_id: u64,
            replica_id: u64,
            db: &Arc<MDB>,
            rsnap: &Arc<SR>,
            wsnap: &Arc<SW>,
        ) -> Result<Self> {
            let core = RockStoreCore {
                group_id,
                replica_id,
                db: db.clone(),
                rsnap: rsnap.clone(),
                wsnap: wsnap.clone(),
            };

            core.set_empty_flag(true)?;

            let meta_cf = DBEnv::get_metadata_cf(db)?;
            let mut batch = WriteBatch::default();
            core.set_hardstate_with_batch(HardState::default(), &meta_cf, &mut batch);
            core.set_confstate_with_batch(ConfState::default(), &meta_cf, &mut batch);
            core.set_snapshot_metadata_with_batch(
                SnapshotMetadata::default(),
                &meta_cf,
                &mut batch,
            );
            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);
            core.db
                .write_opt(batch, &writeopts)
                .map_err(|err| Error::Other(Box::new(err)))?;

            Ok(core)
        }

        /// Get `EntryMetadata` from givn rocksdb.
        fn get_entry_meta(&self) -> Result<EntryMetadata> {
            let cf = DBEnv::get_log_cf(&self.db)?;

            let keys = vec![
                DBEnv::format_empty_key(self.group_id, self.replica_id),
                DBEnv::format_first_index_key(self.group_id, self.replica_id),
                DBEnv::format_last_index_key(self.group_id, self.replica_id),
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
                    || {
                        unreachable!(
                            "the log empty flag should be created when storage is initialized"
                        )
                    },
                    |data| match String::from_utf8(data.to_vec()).unwrap().as_str() {
                        "true" => true,
                        "false" => false,
                        _ => unreachable!(
                            "the log empty flag invalid, it can only be either true or false."
                        ),
                    },
                );

            if empty {
                // let snap_meta = rsnap.snapshot_metadata(group_id, replica_id).unwrap();
                let snap_meta = self.get_snapshot_metadata()?;
                return Ok(EntryMetadata {
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

            Ok(EntryMetadata {
                first_index,
                last_index,
                empty,
            })
        }

        fn is_empty(&self, log_cf: &Arc<BoundColumnFamily>) -> Result<bool> {
            let key = DBEnv::format_empty_key(self.group_id, self.replica_id);
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

        fn get_hardstate(&self) -> Result<HardState> {
            let metacf = DBEnv::get_metadata_cf(&self.db)?;
            let key = DBEnv::format_hardstate_key(self.group_id, self.replica_id);
            let readopts = ReadOptions::default();
            self.db
                .get_cf_opt(&metacf, &key, &readopts)
                .map_err(|err| Error::Other(Box::new(err)))?
                .map_or(Ok(HardState::default()), |data| {
                    Ok(HardState::decode(data.as_ref()).unwrap()) // TODO: use difference serializer
                })
        }

        fn get_confstate(&self) -> Result<ConfState> {
            let metacf = DBEnv::get_metadata_cf(&self.db)?;
            let key = DBEnv::format_confstate_key(self.group_id, self.replica_id);
            let readopts = ReadOptions::default();
            self.db
                .get_cf_opt(&metacf, &key, &readopts)
                .map_err(|err| Error::Other(Box::new(err)))?
                .map_or(Ok(ConfState::default()), |data| {
                    Ok(ConfState::decode(data.as_ref()).unwrap()) // TODO: use difference serializer
                })
        }

        fn get_entry(&self, index: u64) -> Result<Entry> {
            let logcf = DBEnv::get_log_cf(&self.db)?;
            let key = DBEnv::format_entry_key(self.group_id, index);
            let readopts = ReadOptions::default();
            match self
                .db
                .get_cf_opt(&logcf, &key, &readopts)
                .map_err(|err| Error::Other(Box::new(err)))?
            {
                None => panic!("invalid index"),
                Some(data) => Ok(Entry::decode(data.as_ref()).unwrap()), // TODO: use difference serializer
            }
        }

        fn get_snapshot_metadata(&self) -> Result<SnapshotMetadata> {
            let metacf = DBEnv::get_metadata_cf(&self.db)?;
            let key = DBEnv::format_snapshot_metadata_key(self.group_id, self.replica_id);
            let readopts = ReadOptions::default();
            self.db
                .get_cf_opt(&metacf, &key, &readopts)
                .map_err(|err| Error::Other(Box::new(err)))?
                .map_or(Ok(SnapshotMetadata::default()), |data| {
                    Ok(SnapshotMetadata::decode(data.as_ref()).unwrap()) // TODO: use difference serializer
                })
        }

        fn set_snapshot_metadata(&self, meta: &SnapshotMetadata) -> Result<()> {
            if self.first_index()? > meta.index {
                return Err(Error::SnapshotOutOfDate);
            }

            let cf = DBEnv::get_metadata_cf(&self.db)?;
            let key = DBEnv::format_snapshot_metadata_key(self.group_id, self.replica_id);
            let value = meta.encode_to_vec(); // TODO: use difference serializer
            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);
            self.db
                .put_cf_opt(&cf, key, value, &writeopts)
                .map_err(|err| Error::Other(Box::new(err)))
        }

        fn set_empty_flag(&self, flag: bool) -> Result<()> {
            let logcf = DBEnv::get_log_cf(&self.db)?;
            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);
            self.db
                .put_cf_opt(
                    &logcf,
                    DBEnv::format_empty_key(self.group_id, self.replica_id),
                    flag.to_string(),
                    &writeopts,
                )
                .map_err(|err| Error::Other(Box::new(err)))
        }

        #[inline]
        fn set_hardstate_with_batch(
            &self,
            hs: HardState,
            meta_cf: &Arc<BoundColumnFamily>,
            batch: &mut WriteBatch,
        ) {
            let key = DBEnv::format_hardstate_key(self.group_id, self.replica_id);
            let value = hs.encode_to_vec();
            batch.put_cf(meta_cf, key, value)
        }

        #[inline]
        fn set_confstate_with_batch(
            &self,
            cs: ConfState,
            meta_cf: &Arc<BoundColumnFamily>,
            batch: &mut WriteBatch,
        ) {
            let key = DBEnv::format_confstate_key(self.group_id, self.replica_id);
            let value = cs.encode_to_vec();
            batch.put_cf(meta_cf, key, value)
        }

        #[inline]
        fn set_snapshot_metadata_with_batch(
            &self,
            meta: SnapshotMetadata,
            meta_cf: &Arc<BoundColumnFamily>,
            batch: &mut WriteBatch,
        ) {
            let key = DBEnv::format_snapshot_metadata_key(self.group_id, self.replica_id);
            let value = meta.encode_to_vec();
            batch.put_cf(meta_cf, key, value)
        }
    }

    impl<SR: RaftSnapshotReader, SW: RaftSnapshotWriter> RockStoreCore<SR, SW> {
        #[allow(unused)]
        pub(crate) fn append_unchecked(&self, ents: &[Entry]) {
            if ents.is_empty() {
                return;
            }

            let log_meta = self.get_entry_meta().unwrap();
            let log_cf = DBEnv::get_log_cf(&self.db).unwrap();

            let mut batch = WriteBatch::default();
            if log_meta.empty {
                // set first index
                let key = DBEnv::format_first_index_key(self.group_id, self.replica_id);
                let value = ents[0].index.to_be_bytes();
                batch.put_cf(&log_cf, key, value);

                // set not empty
                let key = DBEnv::format_empty_key(self.group_id, self.replica_id);
                let value = "false".as_bytes();
                batch.put_cf(&log_cf, key, value);
            }

            for ent in ents.iter() {
                // let key = self.format_entry_key(ent.index);
                let key = DBEnv::format_entry_key(self.group_id, ent.index);
                // TODO: use feature to use difference ser
                let value = ent.encode_to_vec();
                batch.put_cf(&log_cf, key, value);
            }

            // set last index
            let key = DBEnv::format_last_index_key(self.group_id, self.replica_id);
            let value = ents.last().unwrap().index.to_be_bytes();
            batch.put_cf(&log_cf, key, value);

            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);
            self.db.write_opt(batch, &writeopts).unwrap();
        }

        #[allow(unused)]
        pub fn entries_unchecked(&self) -> Vec<Entry> {
            let mut ents = vec![];
            let log_cf = DBEnv::get_log_cf(&self.db).unwrap();
            let prefix = DBEnv::format_entry_key_prefix(self.group_id);
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

            let keys = vec![
                DBEnv::format_hardstate_key(self.group_id, self.replica_id),
                DBEnv::format_confstate_key(self.group_id, self.replica_id),
            ];
            let readopts = ReadOptions::default();
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
                .get_entry_meta()
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
            let start_key = DBEnv::format_entry_key(self.group_id, low);
            let last_key = DBEnv::format_entry_key(self.group_id, log_meta.last_index);
            let high_key = DBEnv::format_entry_key(self.group_id, high);
            let iter_mode = IteratorMode::From(start_key.as_bytes(), rocksdb::Direction::Forward);
            let mut readopts = ReadOptions::default();
            readopts.set_ignore_range_deletions(true); // skip delete_range to improve read performance
                                                       // TODO: handle if temporaily unavailable
            let iter = self.db.iterator_cf_opt(&log_cf, readopts, iter_mode);
            for ent in iter {
                let (key_data, value_data) = ent.unwrap();
                if high_key.as_bytes() == key_data.as_ref() {
                    break;
                }

                let ent = Entry::decode(value_data.as_ref()).unwrap(); // TODO: handle error
                ents.push(ent);

                if last_key.as_bytes() == key_data.as_ref() {
                    break;
                }
            }

            raft::util::limit_size(&mut ents, max_size.into());

            Ok(ents)
        }

        fn term(&self, idx: u64) -> RaftResult<u64> {
            let snap_meta = self.get_snapshot_metadata().unwrap();
            if idx == snap_meta.index {
                return Ok(snap_meta.term);
            }

            let log_meta = self.get_entry_meta()?;
            if idx < log_meta.first_index {
                return Err(raft::Error::Store(raft::StorageError::Compacted));
            }

            if idx > log_meta.last_index {
                return Err(raft::Error::Store(raft::StorageError::Unavailable));
            }

            let log_cf = DBEnv::get_log_cf(&self.db)?;
            let key = DBEnv::format_entry_key(self.group_id, idx);
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
                    let snap_meta = self.get_snapshot_metadata()?;
                    Ok(snap_meta.index + 1)
                }
                false => {
                    let key = DBEnv::format_first_index_key(self.group_id, self.replica_id);
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
                    let snap_meta = self.get_snapshot_metadata()?;
                    Ok(snap_meta.index)
                }
                false => {
                    let key = DBEnv::format_last_index_key(self.group_id, self.replica_id);
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

        fn snapshot(&self, request_index: u64, _to: u64) -> RaftResult<Snapshot> {
            let mut snap = Snapshot::default();
            // get snapshot data from user state machine.
            let data = self.rsnap.load_snapshot(self.group_id, self.replica_id)?;
            snap.set_data(data);

            // constructor snapshot metadata from store.
            let snap_meta = self.get_snapshot_metadata()?;
            let cs = self.get_confstate()?;
            let hs = self.get_hardstate()?;
            // We assume all entries whose indexes are less than `hard_state.commit`
            // have been applied, so use the latest commit index to construct the snapshot.
            // TODO: This is not true for async ready.
            let mut_meta = snap.mut_metadata();
            mut_meta.index = hs.commit;
            mut_meta.term = match mut_meta.index.cmp(&snap_meta.index) {
                std::cmp::Ordering::Equal => snap_meta.term,
                std::cmp::Ordering::Greater => {
                    // committed index greater than current snapshot index
                    self.get_entry(mut_meta.index)?.term
                }
                std::cmp::Ordering::Less => {
                    panic!(
                        "commit {} < snapshot_metadata.index {}",
                        mut_meta.index, snap_meta.index
                    );
                }
            };

            mut_meta.set_conf_state(cs);
            if mut_meta.index < request_index {
                mut_meta.index = request_index;
            }

            Ok(snap)
        }
    }

    impl<SR: RaftSnapshotReader, SW: RaftSnapshotWriter> RaftStorageWriter for RockStoreCore<SR, SW> {
        fn set_hardstate(&self, hs: HardState) -> Result<()> {
            let metacf = DBEnv::get_metadata_cf(&self.db)?;
            let key = DBEnv::format_hardstate_key(self.group_id, self.replica_id);
            let value = hs.encode_to_vec(); // TODO: add feature for difference serializers.
            let writeopts = WriteOptions::default();
            self.db
                .put_cf_opt(&metacf, &key, &value, &writeopts)
                .map_err(|err| Error::Other(Box::new(err)))
        }

        fn set_confstate(&self, cs: ConfState) -> Result<()> {
            let metacf = DBEnv::get_metadata_cf(&self.db)?;
            let key = DBEnv::format_confstate_key(self.group_id, self.replica_id);
            let value = cs.encode_to_vec(); // TODO: add feature for difference serializers.
            let writeopts = WriteOptions::default();
            self.db
                .put_cf_opt(&metacf, &key, &value, &writeopts)
                .map_err(|err| Error::Other(Box::new(err)))
        }

        fn append(&self, ents: &[Entry]) -> Result<()> {
            if ents.is_empty() {
                return Ok(());
            }

            let ent_meta = self.get_entry_meta()?;

            if ent_meta.first_index > ents[0].index {
                panic!(
                    "overwrite compacted raft logs, compacted: {}, append: {}",
                    ent_meta.first_index - 1,
                    ents[0].index,
                )
            }

            if ent_meta.last_index + 1 < ents[0].index {
                panic!(
                    "raft logs should be continuous, last index: {}, new append: {}",
                    ent_meta.last_index, ents[0].index
                )
            }

            let log_cf = DBEnv::get_log_cf(&self.db)?;

            // remove all entries overwritten by ents.
            if ents[0].index <= ent_meta.last_index {
                let start_key = DBEnv::format_entry_key(self.group_id, ents[0].index);
                let last_key = DBEnv::format_entry_key(self.group_id, ent_meta.last_index + 1);
                let mut writeopts = WriteOptions::default();
                writeopts.set_sync(true);
                self.db
                    .delete_range_cf_opt(&log_cf, start_key, last_key, &writeopts)
                    .map_err(|err| Error::Other(Box::new(err)))?;
            }

            let mut batch = WriteBatch::default();
            if ent_meta.empty {
                // set first index
                let key = DBEnv::format_first_index_key(self.group_id, self.replica_id);
                let value = ents[0].index.to_be_bytes();
                batch.put_cf(&log_cf, key, value);

                // set not empty
                let key = DBEnv::format_empty_key(self.group_id, self.replica_id);
                let value = "false".as_bytes();
                batch.put_cf(&log_cf, key, value);
            }

            for ent in ents.iter() {
                let key = DBEnv::format_entry_key(self.group_id, ent.index);
                let value = ent.encode_to_vec(); // TODO: use feature to use difference ser
                batch.put_cf(&log_cf, key, value);
            }

            // set last index
            let key = DBEnv::format_last_index_key(self.group_id, self.replica_id);
            let value = ents.last().expect("unreachable").index.to_be_bytes();
            batch.put_cf(&log_cf, key, value);

            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);
            self.db.write_opt(batch, &writeopts).unwrap();
            Ok(())
        }

        fn install_snapshot(&self, mut snapshot: Snapshot) -> Result<()> {
            let mut snap_meta = snapshot.metadata.as_ref().expect("unreachable").clone();
            if self.first_index()? > snap_meta.index {
                return Err(Error::SnapshotOutOfDate);
            }

            if snap_meta == SnapshotMetadata::default() {
                return Ok(());
            }

            // save snapshot metadata
            self.set_snapshot_metadata(&snap_meta)?;
            // save snapshot data to user statemachine
            // TODO: consider save snapshot metadata to user statemachine.
            // TODO: consider use async method and add scheduler api
            self.wsnap
                .install_snapshot(self.group_id, self.replica_id, snapshot.take_data())?;

            // update hardstate
            let mut hs = self.get_hardstate()?;
            hs.term = std::cmp::max(snap_meta.term, hs.term);
            hs.commit = snap_meta.index;
            self.set_hardstate(hs)?;

            // clear entries
            let ent_meta = self.get_entry_meta()?;
            if !ent_meta.empty {
                // TODO: need add tests for here
                let cf = DBEnv::get_log_cf(&self.db)?;
                let start_key = DBEnv::format_entry_key(self.group_id, ent_meta.first_index);
                let last_key = DBEnv::format_entry_key(self.group_id, ent_meta.last_index + 1);
                let mut writeopts = WriteOptions::default();
                writeopts.set_sync(true);
                self.db
                    .delete_range_cf_opt(&cf, start_key, last_key, &writeopts)
                    .map_err(|err| Error::Other(Box::new(err)))?;
            }

            // update confstate
            self.set_confstate(snap_meta.take_conf_state())?;

            Ok(())
        }
    }

    impl<SR: RaftSnapshotReader, SW: RaftSnapshotWriter> RaftStorage for RockStoreCore<SR, SW> {
        type SnapshotWriter = SW;
        type SnapshotReader = SR;
    }

    /*****************************************************************************
     * RockStore
     *****************************************************************************/
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
        #[inline]
        fn group_store_key(&self, group_id: u64, replica_id: u64) -> String {
            format!("{}_{}_{}", GROUP_STORE_PREFIX, group_id, replica_id)
        }

        pub fn new<P>(node_id: u64, path: P, snapshot_reader: SR, snapshot_writer: SW) -> Self
        where
            P: AsRef<std::path::Path>,
        {
            let mut db_opts = RocksdbOptions::default();
            db_opts.create_if_missing(true);
            db_opts.create_missing_column_families(true);

            let cfs = vec![
                ColumnFamilyDescriptor::new(METADATA_CF_NAME, db_opts.clone()),
                ColumnFamilyDescriptor::new(LOG_CF_NAME, db_opts.clone()),
            ];

            let db = MDB::open_cf_descriptors(&db_opts, &path, cfs).unwrap();
            Self {
                node_id,
                db: Arc::new(db),
                rsnap: Arc::new(snapshot_reader),
                wsnap: Arc::new(snapshot_writer),
            }
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
                Some(_) => {
                    return Ok(RockStoreCore {
                        group_id,
                        replica_id,
                        db: self.db.clone(),
                        rsnap: self.rsnap.clone(),
                        wsnap: self.wsnap.clone(),
                    })
                }
                None => RockStoreCore::<SR, SW>::new(
                    group_id,
                    replica_id,
                    &self.db,
                    &self.rsnap,
                    &self.wsnap,
                )
                .and_then(|core| {
                    let mut writeopts = WriteOptions::default();
                    writeopts.set_sync(true);
                    // TODO: add added timestamp as data
                    self.db
                        .put_cf_opt(&meta_cf, key, b"".to_vec(), &writeopts)
                        .map_err(|err| Error::Other(Box::new(err)))?;
                    Ok(core)
                }),
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

    use futures::io::Write;
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

    use crate::multiraft::storage::Error;
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

    const SNAP_POSTIFX: &'static str = "snapshot";

    const APPLIED_INDEX_PREFIX: &'static str = "applied_index";
    const APPLIED_TERM_PREFIX: &'static str = "applied_term";
    const MEMBERSHIP_PREFIX: &'static str = "membership";

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
    pub(crate) struct SnapshotMembership {
        pub(crate) voters: Vec<u64>,
        pub(crate) learners: Vec<u64>,
        pub(crate) voters_outgoing: Vec<u64>,
        pub(crate) learners_next: Vec<u64>,
        pub(crate) auto_leave: bool,
    }

    impl From<ConfState> for SnapshotMembership {
        fn from(cs: ConfState) -> Self {
            SnapshotMembership {
                voters: cs.voters,
                learners: cs.learners,
                voters_outgoing: cs.voters_outgoing,
                learners_next: cs.learners_next,
                auto_leave: cs.auto_leave,
            }
        }
    }

    #[derive(serde::Serialize, serde::Deserialize, Default)]
    pub(crate) struct SnapshotMetaSerializer {
        pub(crate) applied_index: u64,
        pub(crate) applied_term: u64,
        pub(crate) last_membership: SnapshotMembership,
    }

    #[derive(serde::Serialize, serde::Deserialize, Default)]
    pub(crate) struct SnapshotDataSerializer {
        pub(crate) bt_map: BTreeMap<String, Vec<u8>>,
    }

    impl<R> TryFrom<(u64, &RockStateMachine<R>)> for SnapshotDataSerializer
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
            let mut data = SnapshotDataSerializer::default();
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

    #[derive(serde::Serialize, serde::Deserialize, Default)]
    pub(crate) struct SnapshotSerializer {
        pub(crate) meta: SnapshotMetaSerializer,
        pub(crate) data: SnapshotDataSerializer,
    }

    impl SnapshotSerializer {
        pub(crate) fn serialize(&self) -> Result<Vec<u8>> {
            serde_json::to_vec(self).map_err(|err| RockStateMachineError::Other(Box::new(err)))
        }

        pub(crate) fn deserialize(data: &[u8]) -> Result<Self> {
            serde_json::from_slice::<SnapshotSerializer>(data)
                .map_err(|err| RockStateMachineError::Other(Box::new(err)))
        }
    }

    impl<R> RaftSnapshotReader for RockStateMachine<R>
    where
        R: WriteResponse,
    {
        fn load_snapshot(&self, group_id: u64, replica_id: u64) -> StorageResult<Vec<u8>> {
            self.get_snapshot(group_id)
                .map_err(|err| Error::Other(Box::new(err)))
        }
    }

    impl<R> RaftSnapshotWriter for RockStateMachine<R>
    where
        R: WriteResponse,
    {
        fn build_snapshot(
            &self,
            group_id: u64,
            replica_id: u64,
            applied_index: u64,
            applied_term: u64,
            conf_state: ConfState,
        ) -> StorageResult<()> {
            let serializer = SnapshotSerializer {
                meta: SnapshotMetaSerializer {
                    applied_index,
                    applied_term,
                    last_membership: SnapshotMembership::from(conf_state),
                },
                data: SnapshotDataSerializer::try_from((group_id, self))
                    .map_err(|err| Error::Other(Box::new(err)))?,
            };

            let data = serializer
                .serialize()
                .map_err(|err| Error::Other(Box::new(err)))?;

            self.set_snapshot(group_id, &data)
                .map_err(|err| Error::Other(Box::new(err)))
        }

        fn install_snapshot(
            &self,
            group_id: u64,
            _replica_id: u64,
            data: Vec<u8>,
        ) -> StorageResult<()> {
            if data.is_empty() {
                return Ok(());
            }

            self.restore(group_id, data)
                .map_err(|err| Error::Other(Box::new(err)))
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
        Other(Box<dyn std::error::Error + Sync + Send>),
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

        fn set_snapshot(&self, group_id: u64, data: &[u8]) -> Result<()> {
            let snapcf = self.db.cf_handle(SNAP_CF_NAME).map_or(
                Err(RockStateMachineError::ColumnFamilyMissing(
                    "snapshot".to_owned(),
                )),
                |cf| Ok(cf),
            )?;

            let key = format_snapshot_key(group_id);

            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);

            self.db
                .put_cf_opt(&snapcf, key, data, &writeopts)
                .map_err(|err| RockStateMachineError::Other(Box::new(err)))
        }

        // TODO: using serializer trait for data
        fn get_snapshot(&self, group_id: u64) -> Result<Vec<u8>> {
            let cf = self.get_snapshot_cf()?;
            let readopts = ReadOptions::default();
            let key = format_snapshot_key(group_id);
            self.db
                .get_pinned_cf_opt(&cf, &key, &readopts)
                .map_err(|err| RockStateMachineError::Other(Box::new(err)))?
                .map_or(Ok(vec![]), |data| Ok(data.to_vec()))
        }

        /// Set current applied index and term.
        fn set_applied(&self, group_id: u64, val: (u64, u64) /* index, term */) -> Result<()> {
            let cf = self.get_data_cf()?;
            let mut batch = WriteBatch::default();
            let key = format_applied_index_key(group_id);
            let value = val.0.to_be_bytes();
            batch.put_cf(&cf, key, value);

            let key = format_applied_term_key(group_id);
            let value = val.1.to_be_bytes();
            batch.put_cf(&cf, key, value);

            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);
            self.db
                .write_opt(batch, &writeopts)
                .map_err(|err| RockStateMachineError::Other(Box::new(err)))
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

        /// Set current last membership to db.
        fn set_membership(
            &self,
            group_id: u64,
            last_memebrship: &SnapshotMembership,
        ) -> Result<()> {
            let cf = self.get_meta_cf()?;
            let key = format_membership_key(group_id);
            let value = serde_json::to_vec(last_memebrship)
                .map_err(|err| RockStateMachineError::Other(Box::new(err)))?;
            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);
            self.db
                .put_cf_opt(&cf, key, value, &writeopts)
                .map_err(|err| RockStateMachineError::Other(Box::new(err)))
        }

        fn restore(&self, group_id: u64, data: Vec<u8>) -> Result<()> {
            let mut batch = WriteBatch::default();
            let cf = self.get_data_cf().unwrap();
            let serializer = SnapshotSerializer::deserialize(&data)?;

            self.set_applied(
                group_id,
                (serializer.meta.applied_index, serializer.meta.applied_term),
            )?;
            self.set_membership(group_id, &serializer.meta.last_membership)?;

            for (raw_key, val) in serializer.data.bt_map.into_iter() {
                let key = format_data_key(group_id, &raw_key);
                // TODO: consider using groupings batch for large snapshot data.
                batch.put_cf(&cf, key, val);
            }

            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);
            self.db.write_opt(batch, &writeopts).unwrap();
            Ok(())
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

    /// Format applied index key with mode `applied_index_{group_id}`.
    #[inline]
    fn format_applied_index_key(group_id: u64) -> String {
        format!("{}_{}", APPLIED_INDEX_PREFIX, group_id)
    }

    /// Format applied term key with mode `applied_term_{group_id}`.
    #[inline]
    fn format_applied_term_key(group_id: u64) -> String {
        format!("{}_{}", APPLIED_TERM_PREFIX, group_id)
    }

    /// Format applied term key with mode `membership_{group_id}`.
    #[inline]
    fn format_membership_key(group_id: u64) -> String {
        format!("{}_{}", MEMBERSHIP_PREFIX, group_id)
    }

    #[inline]
    fn format_snapshot_key(group_id: u64) -> String {
        format!("{}/{}", group_id, SNAP_POSTIFX)
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

        use crate::multiraft::ApplyNormal;
        use crate::multiraft::GroupState;

        use super::RockStateMachine;
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

        // #[tokio::test(flavor = "multi_thread")]
        // async fn test_snapshot_serialize_from_db() {
        //     let node_id = 1;
        //     let group_id = 1;
        //     let replica_id = 1;
        //     // create state machine
        //     let state_machine_tmp_path = temp_dir().join("oceanraft_state_machine_db");
        //     let state_machine =
        //         RockStateMachine::<()>::new(node_id, state_machine_tmp_path.clone());
        //     println!(
        //         "🐿 create state machine store {}",
        //         state_machine_tmp_path.display()
        //     );

        //     apply_to(group_id, 100, 1, &state_machine).await;
        //     apply_to(2, 10, 1, &state_machine).await;
        //     apply_to(3, 20, 1, &state_machine).await;

        //     let ser = RockStateMachineSnapshotSerializer::try_from((
        //         group_id,
        //         replica_id,
        //         &state_machine,
        //     ))
        //     .unwrap();

        //     assert_eq!(ser.meta.applied_index, 100);
        //     assert_eq!(ser.meta.applied_term, 1);
        //     // TODO: snapshot should wrap to structure.

        //     for i in 0..100 {
        //         let key = format!("raw_key_{}", i);
        //         let value = (0..100).map(|_| 1_u8).collect::<Vec<u8>>();
        //         assert_eq!(*ser.data.bt_map.get(&key).unwrap(), value);
        //     }

        //     drop(state_machine);
        //     DBWithThreadMode::<MultiThreaded>::destroy(
        //         &rocksdb::Options::default(),
        //         state_machine_tmp_path.clone(),
        //     )
        //     .unwrap();
        //     println!(
        //         "👋 destory state machine store {}",
        //         state_machine_tmp_path.display()
        //     );
        // }
    }
}
pub use storage::{RockStore, RocksError};

pub use state_machine::{RockData, RockDataError, RockStateMachine, RockStateMachineError};

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::panic;
    use std::panic::AssertUnwindSafe;
    use std::path::Path;
    use std::path::PathBuf;

    use futures::future::BoxFuture;
    use futures::FutureExt;
    use protobuf::Message as PbMessage;
    use raft::prelude::HardState;
    use raft::Error as RaftError;
    use raft::GetEntriesContext;
    use raft::Storage;
    use raft::StorageError as RaftStorageError;
    use rand::distributions::Alphanumeric;
    use rand::Rng;
    use rocksdb::DBWithThreadMode;
    use rocksdb::MultiThreaded;

    use super::state_machine::SnapshotDataSerializer;
    use super::state_machine::SnapshotMetaSerializer;
    use super::state_machine::SnapshotSerializer;
    use super::RockData;
    use super::RockStateMachine;
    use super::RockStore;
    use crate::multiraft::storage::MultiRaftStorage;
    use crate::multiraft::storage::RaftSnapshotWriter;
    use crate::multiraft::storage::RaftStorageWriter;
    use crate::multiraft::Apply;
    use crate::multiraft::ApplyNormal;
    use crate::multiraft::GroupState;
    use crate::multiraft::StateMachine;
    use crate::multiraft::WriteData;
    use crate::multiraft::WriteResponse;
    use crate::prelude::ConfState;
    use crate::prelude::Entry;
    use crate::prelude::ReplicaDesc;
    use crate::prelude::Snapshot;

    fn rand_temp_dir() -> PathBuf {
        let rand_str: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(8)
            .map(char::from)
            .collect();
        temp_dir().join(rand_str)
    }

    fn rand_data(n: usize) -> String {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(n)
            .map(char::from)
            .collect::<String>()
    }

    fn new_entry(index: u64, term: u64) -> Entry {
        let mut e = Entry::default();
        e.term = term;
        e.index = index;
        e
    }

    fn new_rockdata_entry(index: u64, term: u64, data: &[u8]) -> Entry {
        let mut e = Entry::default();
        e.term = term;
        e.index = index;
        e.data = data.to_vec();
        e
    }

    fn new_rockdata_apply<R: WriteResponse>(
        group_id: u64,
        index: u64,
        term: u64,
        data: RockData,
    ) -> Apply<R> {
        Apply::Normal(ApplyNormal {
            group_id,
            index,
            term,
            data: data.encode().unwrap(),
            is_conf_change: false,
            context: None,
            tx: None,
        })
    }

    fn new_snapshot_serializer_from_entries(
        index: u64,
        term: u64,
        voters: Vec<u64>,
        rockdatas: Vec<RockData>,
    ) -> SnapshotSerializer {
        let mut data = SnapshotDataSerializer::default();
        for rockdata in rockdatas.into_iter() {
            data.bt_map.insert(rockdata.key, rockdata.value);
        }

        let mut meta = SnapshotMetaSerializer::default();
        meta.applied_index = index;
        meta.applied_term = term;
        meta.last_membership.voters = voters;

        SnapshotSerializer { data, meta }
    }

    fn new_snapshot_with_data(
        index: u64,
        term: u64,
        voters: Vec<u64>,
        serializer: SnapshotSerializer,
    ) -> Snapshot {
        let mut s = Snapshot::default();
        s.mut_metadata().index = index;
        s.mut_metadata().term = term;
        s.mut_metadata().mut_conf_state().voters = voters;
        s.data = serializer.serialize().unwrap();
        s
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

    fn destroy_db(path: &Path) {
        DBWithThreadMode::<MultiThreaded>::destroy(&rocksdb::Options::default(), path).unwrap();
    }

    fn new_state_machine<R: WriteResponse>(path: &Path, node_id: u64) -> RockStateMachine<R> {
        let state_machine = RockStateMachine::<R>::new(node_id, path);

        println!("🐿 create state machine store {}", path.display());

        state_machine
    }

    fn new_rockstore<R: WriteResponse>(
        path: &Path,
        node_id: u64,
        state_machine: &RockStateMachine<R>,
    ) -> RockStore<RockStateMachine<R>, RockStateMachine<R>> {
        let rock_store =
            RockStore::new(node_id, path, state_machine.clone(), state_machine.clone());

        println!("🚛 create raft store {}", path.display());
        rock_store
    }

    fn db_test_env<F, R>(f: F)
    where
        F: FnOnce(&RockStore<RockStateMachine<R>, RockStateMachine<R>>, &RockStateMachine<R>),
        R: WriteResponse,
    {
        let state_machine_temp_dir = rand_temp_dir().join("oceanraft_state_machine");
        let rock_store_temp_dir = rand_temp_dir().join("oceanraft_rock_store");

        let node_id = 1;
        let state_machine = new_state_machine::<R>(&state_machine_temp_dir, 1);
        let rock_store = new_rockstore::<R>(&rock_store_temp_dir, node_id, &state_machine);
        let rock_store_core = rock_store.create_group_store_if_missing(1, 1).unwrap();
        {
            f(&rock_store, &state_machine)
        }

        drop(rock_store_core);
        drop(rock_store);
        drop(state_machine);

        destroy_db(&rock_store_temp_dir);
        destroy_db(&state_machine_temp_dir);
    }

    /// Provide async env for tests
    ///
    /// **Note:** a little of trick to see [Reference parameter to async fn does not live long enough](https://users.rust-lang.org/t/reference-parameter-to-async-fn-does-not-live-long-enough/65230) get more informactions.
    async fn db_test_async_env<F, R>(f: F)
    where
        F: for<'r> FnOnce(
            u64,
            &'r RockStore<RockStateMachine<R>, RockStateMachine<R>>,
            &'r RockStateMachine<R>,
        ) -> BoxFuture<'r, ()>,
        R: WriteResponse,
    {
        let state_machine_temp_dir = rand_temp_dir().join("oceanraft_state_machine");
        let rock_store_temp_dir = rand_temp_dir().join("oceanraft_rock_store");

        let node_id = 1;
        let state_machine = new_state_machine::<R>(&state_machine_temp_dir, node_id);
        let rock_store = new_rockstore::<R>(&rock_store_temp_dir, node_id, &state_machine);
        {
            f(node_id, &rock_store, &state_machine).boxed().await;
        }

        drop(rock_store);
        drop(state_machine);

        destroy_db(&rock_store_temp_dir);
        destroy_db(&state_machine_temp_dir);
    }

    /*****************************************************************************
     * TEST ROCK STORAGE CORE
     *****************************************************************************
     */
    #[test]
    fn test_rock_storage_term() {
        db_test_env::<_, ()>(|rock_store, _state_machine| {
            let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
            let mut tests = vec![
                (2, Err(RaftError::Store(RaftStorageError::Compacted))),
                (3, Ok(3)),
                (4, Ok(4)),
                (5, Ok(5)),
                (6, Err(RaftError::Store(RaftStorageError::Unavailable))),
            ];

            let rock_store_core = rock_store.create_group_store_if_missing(1, 1).unwrap();

            for (i, (idx, wterm)) in tests.drain(..).enumerate() {
                rock_store_core.append_unchecked(&ents);

                let t = rock_store_core.term(idx);
                if t != wterm {
                    panic!("#{}: expect res {:?}, got {:?}", i, wterm, t);
                }
            }
        });
    }

    #[test]
    fn test_rock_storage_create_snapshot() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let nodes = vec![1, 2, 3];
        let mut conf_state = ConfState::default();
        conf_state.voters = nodes.clone();

        // let unavailable = Err(RaftError::Store(
        //     StorageError::SnapshotTemporarilyUnavailable,
        // ));

        let mut tests = vec![
            (4, Ok(new_snapshot(4, 4, nodes.clone())), 0),
            (5, Ok(new_snapshot(5, 5, nodes.clone())), 5),
            (5, Ok(new_snapshot(6, 5, nodes)), 6),
            // (5, unavailable, 6),
        ];
        for (i, (idx, wresult, windex)) in tests.drain(..).enumerate() {
            db_test_env::<_, ()>(|rock_store, _state_machine| {
                let rock_store_core = rock_store.create_group_store_if_missing(1, 1).unwrap();
                rock_store_core.append_unchecked(&ents);
                rock_store_core
                    .set_hardstate(HardState {
                        term: idx,
                        vote: 0,
                        commit: idx,
                    })
                    .unwrap();
                rock_store_core.set_confstate(conf_state.clone()).unwrap();

                let result = rock_store_core.snapshot(windex, 0);
                if result != wresult {
                    panic!("#{}: want {:?}, got {:?}", i, wresult, result);
                }
            });
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_rock_storage_create_snapshot_with_data() {
        let group_id = 1;
        let nodes = vec![1, 2, 3];
        let mut conf_state = ConfState::default();
        conf_state.voters = nodes.clone();

        // let unavailable = Err(RaftError::Store(
        //     StorageError::SnapshotTemporarilyUnavailable,
        // ));

        let datas = vec![
            RockData {
                key: rand_data(4),
                value: rand_data(8).into(),
            },
            RockData {
                key: rand_data(4),
                value: rand_data(8).into(),
            },
            RockData {
                key: rand_data(4),
                value: rand_data(8).into(),
            },
        ];

        let applys = vec![
            new_rockdata_apply::<()>(group_id, 3, 3, datas[0].clone()),
            new_rockdata_apply::<()>(group_id, 4, 4, datas[1].clone()),
            new_rockdata_apply::<()>(group_id, 5, 5, datas[2].clone()),
        ];

        let ents = applys
            .iter()
            .map(|apply| {
                new_rockdata_entry(apply.entry_index(), apply.entry_term(), &apply.entry_data())
            })
            .collect::<Vec<_>>();

        let mut tests = vec![
            // applys, apply index, snapshot
            (
                vec![
                    new_rockdata_apply(group_id, 3, 3, datas[0].clone()),
                    new_rockdata_apply(group_id, 4, 4, datas[1].clone()),
                ],
                4,
                Ok(new_snapshot_with_data(
                    4,
                    4,
                    nodes.clone(),
                    new_snapshot_serializer_from_entries(4, 4, nodes.clone(), datas[0..2].to_vec()),
                )),
                0,
            ),
            (
                vec![
                    new_rockdata_apply(group_id, 3, 3, datas[0].clone()),
                    new_rockdata_apply(group_id, 4, 4, datas[1].clone()),
                    new_rockdata_apply(group_id, 5, 5, datas[2].clone()),
                ],
                5,
                // Ok(new_snapshot(5, 5, nodes.clone())),
                Ok(new_snapshot_with_data(
                    5,
                    5,
                    nodes.clone(),
                    new_snapshot_serializer_from_entries(5, 5, nodes.clone(), datas.clone()),
                )),
                5,
            ),
            (
                vec![
                    new_rockdata_apply(group_id, 3, 3, datas[0].clone()),
                    new_rockdata_apply(group_id, 4, 4, datas[1].clone()),
                    new_rockdata_apply(group_id, 5, 5, datas[2].clone()),
                ],
                5,
                // Ok(new_snapshot(6, 5, nodes)),
                Ok(new_snapshot_with_data(
                    6,
                    5,
                    nodes.clone(),
                    new_snapshot_serializer_from_entries(5, 5, nodes.clone(), datas.clone()),
                )),
                6,
            ),
            // (5, unavailable, 6),
        ];
        for (i, (applys, apply_idx, wresult, windex)) in tests.drain(..).enumerate() {
            let conf_state = conf_state.clone();
            let ents = ents.clone();
            db_test_async_env::<_, ()>(|node_id, rock_store, state_machine| {
                let fut = async move {
                    let rock_store_core = rock_store
                        .create_group_store_if_missing(group_id, 1)
                        .unwrap();
                    rock_store_core.append_unchecked(&ents);
                    rock_store_core
                        .set_hardstate(HardState {
                            term: apply_idx,
                            vote: 0,
                            commit: apply_idx,
                        })
                        .unwrap();
                    rock_store_core.set_confstate(conf_state.clone()).unwrap();

                    state_machine
                        .apply(group_id, &GroupState::default(), applys.into_iter())
                        .await;
                    state_machine
                        .build_snapshot(group_id, 1, apply_idx, apply_idx, conf_state.clone())
                        .unwrap();

                    let result = rock_store_core.snapshot(windex, 0);
                    if result != wresult {
                        // println!("{:?}", result);
                        panic!("#{}: want {:?}, got {:?}", i, wresult, result);
                    }
                };
                Box::pin(fut)
            })
            .await;
        }
    }

    #[test]
    fn test_rock_storage_append() {
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
            db_test_env::<_, ()>(|rock_store, _state_machine| {
                let rock_store_core = rock_store.create_group_store_if_missing(1, 1).unwrap();

                rock_store_core.append_unchecked(&ents);
                let res =
                    panic::catch_unwind(AssertUnwindSafe(|| rock_store_core.append(&entries)));
                if let Some(wentries) = wentries {
                    let _ = res.unwrap();
                    let e = &rock_store_core.entries_unchecked();
                    if *e != wentries {
                        panic!("#{}: want {:?}, entries {:?}", i, wentries, e);
                    }
                } else {
                    res.unwrap_err();
                }
            });
        }
    }

    #[test]
    fn test_rock_storage_entries() {
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

        for (i, (lo, hi, maxsize, wentries)) in tests.drain(..).enumerate() {
            db_test_env::<_, ()>(|rock_store, _state_machine| {
                let rock_store_core = rock_store.create_group_store_if_missing(1, 1).unwrap();
                rock_store_core.append_unchecked(&ents);
                let e = rock_store_core.entries(lo, hi, maxsize, GetEntriesContext::empty(false));
                if e != wentries {
                    panic!("#{}: expect entries {:?}, got {:?}", i, wentries, e);
                }
            });
        }
    }

    #[test]
    fn test_rock_storage_first_index() {
        db_test_env::<_, ()>(|rock_store, _state_machine| {
            let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];

            let rock_store_core = rock_store.create_group_store_if_missing(1, 1).unwrap();
            rock_store_core.append_unchecked(&ents);

            assert_eq!(rock_store_core.first_index(), Ok(3));
            // storage.wl().compact(4).unwrap();
            // assert_eq!(storage.first_index(), Ok(4));
        });
    }

    #[test]
    fn test_rock_storage_last_index() {
        db_test_env::<_, ()>(|rock_store, _state_machine| {
            let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];

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
        });
    }

    #[test]
    fn test_rock_storage_apply_snapshot() {
        let nodes = vec![1, 2, 3];

        db_test_env::<_, ()>(|rock_store, _state_machine| {
            let rock_store_core = rock_store.create_group_store_if_missing(1, 1).unwrap();
            // Apply snapshot successfully
            let snap = new_snapshot(4, 4, nodes.clone());
            rock_store_core.install_snapshot(snap).unwrap();

            // Apply snapshot fails due to StorageError::SnapshotOutOfDate
            let snap = new_snapshot(3, 3, nodes);
            rock_store_core.install_snapshot(snap).unwrap_err();
        });
    }

    /*****************************************************************************
     * TEST MULTI STORE
     *****************************************************************************
     */
    #[tokio::test(flavor = "multi_thread")]
    async fn test_multi_rock_storage_replicas() {
        db_test_async_env::<_, ()>(|_node_id, rock_store, _state_machine| {
            let fut = async move {
                let group_id = 1;
                let replicas = vec![
                    ReplicaDesc {
                        node_id: 1,
                        group_id,
                        replica_id: 1,
                    },
                    ReplicaDesc {
                        node_id: 2,
                        group_id,

                        replica_id: 2,
                    },
                    ReplicaDesc {
                        node_id: 3,
                        group_id,
                        replica_id: 3,
                    },
                ];

                for (i, wrep) in replicas.iter().enumerate() {
                    if rock_store
                        .get_replica_desc(group_id, wrep.replica_id)
                        .await
                        .unwrap()
                        .is_some()
                    {
                        panic!("# {}: expecte replica desc is none", i)
                    }

                    rock_store
                        .set_replica_desc(group_id, wrep.clone())
                        .await
                        .unwrap();
                }

                for (i, wrep) in replicas.iter().enumerate() {
                    if let Some(grep) = rock_store
                        .get_replica_desc(group_id, wrep.replica_id)
                        .await
                        .unwrap()
                    {
                        assert_eq!(grep, *wrep, "# {}: expect {:?}, got {:?}", i, *wrep, grep);
                    } else {
                        panic!("# {}: expecte some replica desc, got none", i)
                    }
                }

                for (i, wrep) in replicas.iter().enumerate() {
                    if let Some(grep) = rock_store
                        .replica_for_node(group_id, wrep.node_id)
                        .await
                        .unwrap()
                    {
                        assert_eq!(grep, *wrep, "# {}: expect {:?}, got {:?}", i, *wrep, grep);
                    } else {
                        panic!("# {}: expecte some replica desc, got none", i)
                    }
                }

                for (i, wrep) in replicas.iter().enumerate() {
                    rock_store
                        .remove_replica_desc(group_id, wrep.replica_id)
                        .await
                        .unwrap();

                    if let Some(grep) = rock_store
                        .get_replica_desc(group_id, wrep.replica_id)
                        .await
                        .unwrap()
                    {
                        panic!("# {}: expecte none replica desc, got {:?}", i, grep);
                    }
                }
            };
            Box::pin(fut)
        })
        .await;
    }
}
