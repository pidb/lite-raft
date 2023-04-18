mod storage {
    use std::sync::Arc;

    use futures::Future;
    use prost::Message;
    use raft::RaftState;
    use raft::Result as RaftResult;
    use raft::StorageError as RaftStorageError;
    use rocksdb::BoundColumnFamily;
    use rocksdb::ColumnFamilyDescriptor;
    use rocksdb::DBWithThreadMode;
    use rocksdb::Error as RocksdbError;
    use rocksdb::ErrorKind as RocksdbErrorKind;
    use rocksdb::IteratorMode;
    use rocksdb::MultiThreaded;
    use rocksdb::Options as RocksdbOptions;
    use rocksdb::ReadOptions;
    use rocksdb::WriteBatch;
    use rocksdb::WriteOptions;
    use tracing::error;

    use crate::prelude::ConfState;
    use crate::prelude::Entry;
    use crate::prelude::HardState;
    use crate::prelude::ReplicaDesc;
    use crate::prelude::Snapshot;
    use crate::prelude::SnapshotMetadata;
    use crate::storage::Error;
    use crate::storage::MultiRaftStorage;
    use crate::storage::RaftSnapshotReader;
    use crate::storage::RaftSnapshotWriter;
    use crate::storage::RaftStorage;
    use crate::storage::Result;
    use crate::storage::Storage;
    use crate::storage::StorageExt;
    use crate::utils::flexbuffer_deserialize;
    use crate::utils::flexbuffer_serialize;

    /// Save rocksdb error context.
    #[derive(Clone)]
    struct ErrorContext {
        node_id: u64,
        group_id: u64,
        replica_id: u64,
        operation: String,
        is_log: bool,
        is_snap: bool,
    }

    impl ErrorContext {
        /// Dump the context and return an error message.
        fn dump(&self, kind: &RocksdbErrorKind) -> String {
            let who = if self.is_log {
                "log"
            } else if self.is_snap {
                "snapshot"
            } else {
                "storage"
            };

            match kind {
                RocksdbErrorKind::NotFound => {
                    format!("node {}: NotFound error occurs on the rocksdb storage. operation ({}) that replica {} of group {} is temporarily unavailable for {}",self.node_id,  self.operation, self.group_id, self.replica_id, who)
                }
                RocksdbErrorKind::Corruption => {
                    format!("node {}: Corruption error occurs on the rocksdb storage. operation ({}) that replica {} of group {} is unavailable for {}",self.node_id,  self.operation, self.group_id, self.replica_id, who)
                }
                RocksdbErrorKind::NotSupported => {
                    format!("node {}: NotSupported error occurs on the rocksdb storage. operation ({}) that replica {} of group {} is temporarily unavailable for {}",self.node_id,  self.operation, self.group_id, self.replica_id, who)
                }
                RocksdbErrorKind::InvalidArgument => {
                    format!("node {}: InvalidArgument error occurs on the rocksdb storage. operation ({}) that replica {} of group {} is temporarily unavailable for {}",self.node_id,  self.operation, self.group_id, self.replica_id, who)
                }
                RocksdbErrorKind::IOError => {
                    format!("node {}: IOError error occurs on the rocksdb storage. operation ({}) that replica {} of group {} is temporarily unavailable for {}",self.node_id,  self.operation, self.group_id, self.replica_id, who)
                }
                RocksdbErrorKind::MergeInProgress => {
                    format!("node {}: MergeInProgress error occurs on the rocksdb storage. operation ({}) that replica {} of group {} is temporarily unavailable for {}",self.node_id,  self.operation, self.group_id, self.replica_id, who)
                }
                RocksdbErrorKind::Incomplete => {
                    format!("node {}: Incomplete error occurs on the rocksdb storage. operation ({}) that replica {} of group {} is temporarily unavailable for {}",self.node_id,  self.operation, self.group_id, self.replica_id, who)
                }
                RocksdbErrorKind::ShutdownInProgress => {
                    format!("node {}: Incomplete error occurs on the rocksdb storage. operation ({}) that replica {} of group {} is unavailable for {}",self.node_id,  self.operation, self.group_id, self.replica_id, who)
                }
                RocksdbErrorKind::TimedOut => {
                    format!("node {}: TimedOut error occurs on the rocksdb storage. operation ({}) that replica {} of group {} is temporarily unavailable for {}",self.node_id,  self.operation, self.group_id, self.replica_id, who)
                }
                RocksdbErrorKind::Aborted => {
                    format!(
                    "node {}: Aborted error occurs on the rocksdb storage. operation ({}) that replica {} of group {} is unavailable for {}",
                    self.node_id,  self.operation, self.group_id, self.replica_id, who
                )
                }
                RocksdbErrorKind::Busy => {
                    format!("node {}: Busy error occurs on the rocksdb storage. operation ({}) that replica {} of group {} is temporarily unavailable for {}",self.node_id,  self.operation, self.group_id, self.replica_id, who)
                }
                RocksdbErrorKind::Expired => {
                    format!("node {}: Expired error occurs on the rocksdb storage. operation ({}) that replica {} of group {} is temporarily unavailable for {}",self.node_id,  self.operation, self.group_id, self.replica_id, who)
                }
                RocksdbErrorKind::TryAgain => {
                    format!("node {}: TryAgain error occurs on the rocksdb storage. operation ({}) that replica {} of group {} is temporarily unavailable for {}",self.node_id,  self.operation, self.group_id, self.replica_id, who)
                }
                RocksdbErrorKind::CompactionTooLarge => {
                    format!("node {}: CompactionTooLarge error occurs on the rocksdb storage. operation ({}) that replica {} of group {} is temporarily unavailable for {}",self.node_id,  self.operation, self.group_id, self.replica_id, who)
                }
                RocksdbErrorKind::ColumnFamilyDropped => {
                    format!("node {}: ColumnFamilyDropped error occurs on the rocksdb storage. operation ({}) that replica {} of group {} is temporarily unavailable for {}",self.node_id,  self.operation, self.group_id, self.replica_id, who)
                }
                RocksdbErrorKind::Unknown => {
                    format!(
                    "node {}: Unkown error occurs on the rocksdb storage. operation ({}) that replica {} of group {} is unavailable for {}",
                    self.node_id, self.operation,self.group_id, self.replica_id, who
                )
                }
            }
        }
    }

    struct ErrorHandler {
        ctx: ErrorContext,
        err: RocksdbError,
    }

    impl From<ErrorHandler> for Error {
        /// Convert rocksdb error to storage error.
        fn from(handling: ErrorHandler) -> Self {
            let kind = handling.err.kind();
            error!("{}", handling.ctx.dump(&kind));
            match kind {
                RocksdbErrorKind::NotFound
                | RocksdbErrorKind::NotSupported
                | RocksdbErrorKind::InvalidArgument
                | RocksdbErrorKind::IOError
                | RocksdbErrorKind::MergeInProgress
                | RocksdbErrorKind::Incomplete
                | RocksdbErrorKind::TimedOut
                | RocksdbErrorKind::Busy
                | RocksdbErrorKind::Expired
                | RocksdbErrorKind::TryAgain
                | RocksdbErrorKind::CompactionTooLarge
                | RocksdbErrorKind::ColumnFamilyDropped
                | RocksdbErrorKind::Aborted
                | RocksdbErrorKind::Unknown => {
                    if handling.ctx.is_log {
                        Error::LogTemporarilyUnavailable
                    } else if handling.ctx.is_log {
                        Error::SnapshotTemporarilyUnavailable
                    } else {
                        Error::StorageTemporarilyUnavailable
                    }
                }
                RocksdbErrorKind::Corruption => {
                    // If this error occurs, there is a corruption
                    // with the data.
                    // returned the unavailable error to the upper
                    // layer and output the error.
                    // TODO: Further, we should save the context of data corruption
                    if handling.ctx.is_log {
                        Error::LogUnavailable
                    } else if handling.ctx.is_snap {
                        Error::SnapshotUnavailable
                    } else {
                        Error::StorageUnavailable
                    }
                }
                RocksdbErrorKind::ShutdownInProgress => {
                    if handling.ctx.is_log {
                        Error::LogUnavailable
                    } else if handling.ctx.is_snap {
                        Error::SnapshotUnavailable
                    } else {
                        Error::StorageUnavailable
                    }
                }
            }
        }
    }

    impl From<ErrorHandler> for RaftStorageError {
        /// Convert rocksdb error to raft storage error.
        fn from(handling: ErrorHandler) -> Self {
            let kind = handling.err.kind();
            error!("{}", handling.ctx.dump(&kind));
            match kind {
                RocksdbErrorKind::NotFound
                | RocksdbErrorKind::NotSupported
                | RocksdbErrorKind::InvalidArgument
                | RocksdbErrorKind::IOError
                | RocksdbErrorKind::MergeInProgress
                | RocksdbErrorKind::Incomplete
                | RocksdbErrorKind::TimedOut
                | RocksdbErrorKind::Busy
                | RocksdbErrorKind::Expired
                | RocksdbErrorKind::TryAgain
                | RocksdbErrorKind::CompactionTooLarge
                | RocksdbErrorKind::ColumnFamilyDropped
                | RocksdbErrorKind::Aborted
                | RocksdbErrorKind::Unknown => {
                    if handling.ctx.is_log {
                        RaftStorageError::LogTemporarilyUnavailable
                    } else if handling.ctx.is_snap {
                        RaftStorageError::SnapshotTemporarilyUnavailable
                    } else {
                        // just to make the compiler happy, never reach here.
                        RaftStorageError::Unavailable
                    }
                }
                RocksdbErrorKind::Corruption => {
                    // If this error occurs, there is a corruption
                    // with the data.
                    // returned the unavailable error to the upper
                    // layer and output the error.
                    // TODO: Further, we should save the context of data corruption
                    RaftStorageError::Unavailable
                }
                RocksdbErrorKind::ShutdownInProgress => RaftStorageError::Unavailable,
            }
        }
    }

    /// Type define for rocksdb with multi-thread.
    type MDB = DBWithThreadMode<MultiThreaded>;

    /// Constant for meta column family name.
    const METADATA_CF_NAME: &'static str = "metadta_cf";

    /// Constant for log column family name.
    const LOG_CF_NAME: &'static str = "raft_log_cf";

    /// Constant prerfix for rocks core store and store in meta column family.
    const GROUP_STORE_PREFIX: &'static str = "gs";

    /// Constant prerfix for replica desc and store in meta column family.
    const REPLICA_DESC_PREFIX: &'static str = "rd";

    /// Constant prerfix for hardstate and store in meta column family.
    const HARD_STATE_PREFIX: &'static str = "hs";

    /// Constant prerfix for confstate and store in meta column family.
    const CONF_STATE_PREFIX: &'static str = "cs";

    /// Constant prerfix for applied and store in meta column family.
    const APPLIED_INDEX_PREFIX: &'static str = "applied_index";

    /// Constant prerfix for snapshot metadata and store in meta column family.
    const LOG_SNAP_META_PREFIX: &'static str = "snap_meta";

    /// Constant prerfix for log empty flag and store in log column family.
    const LOG_EMPTY_PREFIX: &'static str = "log_empty";

    /// Constant prerfix for log first index and store in log column family.
    const LOG_FIRST_INDEX_PREFIX: &'static str = "fidx";

    /// Constant prerfix for log last index and store in log column family.
    const LOG_LAST_INDEX_PREFIX: &'static str = "lidx";

    /// A lightweight helper method for mdb
    struct DBEnv;

    impl DBEnv {
        #[inline]
        fn get_metadata_cf(db: &Arc<MDB>) -> Arc<BoundColumnFamily> {
            db.cf_handle(METADATA_CF_NAME)
                .expect("unreachable: metadata_cf handler missing")
        }

        #[inline]
        fn get_log_cf(db: &Arc<MDB>) -> Arc<BoundColumnFamily> {
            db.cf_handle(LOG_CF_NAME)
                .expect("unreachable: raft_log_cf handler missing")
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

        /// Format applied_index  key with mode `{group_id}_{replica_id}_applied_index`.
        #[inline]
        fn format_applied_key(group_id: u64, replica_id: u64) -> String {
            format!("{}_{}_{}", group_id, replica_id, APPLIED_INDEX_PREFIX)
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

        /// Format log entry index key with mode `ent_{group_id}_{index}`.
        ///
        /// # Notes
        /// On rocksdb, the default lexicographical sorting is `ent_1_3 > ent_1_11`,
        /// which causes an error. although rocksdb can define additional comparactors,
        /// this incurs additional costs, so we align u64 to represent the maximum string
        /// length. For example, `ent_1_0000000000000003 < ent_1_00000000000000000011`,
        /// since we have prefix compression enabled (TODO), space consumption is not an issue.
        #[inline]
        fn format_entry_key(group_id: u64, index: u64) -> String {
            format!("ent_{}_{:0>20}", group_id, index)
        }

        #[inline]
        fn format_entry_key_prefix(group_id: u64) -> String {
            format!("ent_{}_", group_id)
        }

        /// Format snapshot metadata key with mode `snap_meta_{group_id}_{replica_id}`
        #[inline]
        fn format_snapshot_metadata_key(group_id: u64, replica_id: u64) -> String {
            format!("{}_{}_{}", LOG_SNAP_META_PREFIX, group_id, replica_id)
        }

        /// Format replica description key with mode `rd_{group_id}_{replica_id}` and
        /// stored in metadata cf.
        #[inline]
        fn format_replica_desc_key(group_id: u64, replica_id: u64) -> String {
            format!("{}_{}_{}", REPLICA_DESC_PREFIX, group_id, replica_id)
        }

        /// Format replica description seek specific group by key with mode `rd_{group_id}_` and
        /// stored in metadata cf.
        #[inline]
        fn format_replica_desc_seek_key(group_id: u64) -> String {
            format!("{}_{}_", REPLICA_DESC_PREFIX, group_id)
        }
    }

    #[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
    struct Applied {
        index: u64,
        term: u64,
    }

    /// Stored in the `log_cf` column family, representing the
    /// metadata stored in the entries in the db
    struct EntryMetadata {
        first_index: u64,
        last_index: u64,
        empty: bool,
    }

    /*****************************************************************************
     * ROCKSTORE CORE
     *****************************************************************************/
    #[derive(Clone)]
    pub struct RockStoreCore<SR: RaftSnapshotReader, SW: RaftSnapshotWriter> {
        node_id: u64,
        group_id: u64,
        replica_id: u64,
        db: Arc<MDB>,
        rsnap: SR,
        wsnap: SW,
    }

    impl<SR: RaftSnapshotReader, SW: RaftSnapshotWriter> RockStoreCore<SR, SW> {
        /// New and initialze RockStoreCore from db.
        ///
        /// # Panics
        /// if RockStoreCore has been initialized
        fn new(
            node_id: u64,
            group_id: u64,
            replica_id: u64,
            db: &Arc<MDB>,
            rsnap: &SR,
            wsnap: &SW,
        ) -> std::result::Result<Self, RocksdbError> {
            let core = RockStoreCore {
                node_id,
                group_id,
                replica_id,
                db: db.clone(),
                rsnap: rsnap.clone(),
                wsnap: wsnap.clone(),
            };

            core.set_empty_flag(true)?;

            let meta_cf = DBEnv::get_metadata_cf(db);
            let mut batch = WriteBatch::default();
            // put default hard_state
            let hs = HardState::default();
            let key = DBEnv::format_hardstate_key(group_id, replica_id);
            let value = hs.encode_to_vec();
            batch.put_cf(&meta_cf, key, value);

            // put default conf_state
            let cs = ConfState::default();
            let key = DBEnv::format_confstate_key(group_id, replica_id);
            let value = cs.encode_to_vec();
            batch.put_cf(&meta_cf, key, value);

            // put default snapshot_metadata
            let meta = SnapshotMetadata::default();
            let key = DBEnv::format_snapshot_metadata_key(group_id, replica_id);
            let value = meta.encode_to_vec();
            batch.put_cf(&meta_cf, key, value);

            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);
            core.db.write_opt(batch, &writeopts)?;

            Ok(core)
        }

        /// Handling rocksdb write related error and returned Error.
        #[inline]
        fn to_write_err(
            &self,
            err: RocksdbError,
            is_log: bool,
            is_snap: bool,
            op: String,
        ) -> Error {
            Error::from(ErrorHandler {
                ctx: ErrorContext {
                    node_id: self.node_id,
                    group_id: self.group_id,
                    replica_id: self.replica_id,
                    operation: op,
                    is_log,
                    is_snap,
                },
                err,
            })
        }

        /// Handling rocksdb read related error and returned RaftStorageError.
        #[inline]
        fn to_read_err(
            &self,
            err: RocksdbError,
            is_log: bool,
            is_snap: bool,
            op: String,
        ) -> RaftStorageError {
            RaftStorageError::from(ErrorHandler {
                ctx: ErrorContext {
                    node_id: self.node_id,
                    group_id: self.group_id,
                    replica_id: self.replica_id,
                    operation: op,
                    is_log,
                    is_snap,
                },
                err,
            })
        }

        /// Get `EntryMetadata` from given rocksdb.
        fn get_entry_meta(&self) -> std::result::Result<EntryMetadata, RocksdbError> {
            let cf = DBEnv::get_log_cf(&self.db);

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

            let empty = batchs.next().expect("unreachable")?.map_or_else(
                || unreachable!("the log empty flag should be created when storage is initialized"),
                |data| match String::from_utf8(data.to_vec())
                    .expect("invalid log empty flag data")
                    .as_str()
                {
                    "true" => true,
                    "false" => false,
                    _ => unreachable!(
                        "the log empty flag invalid, it can only be either true or false."
                    ),
                },
            );

            if empty {
                let snap_meta = self.get_snapshot_metadata()?;
                return Ok(EntryMetadata {
                    first_index: snap_meta.index + 1,
                    last_index: snap_meta.index,
                    empty,
                });
            }

            let data = batchs
                .next()
                .expect("unreachable")?
                .expect("log is not empty, but failed to multiget first_index");

            let first_index =
                u64::from_be_bytes(data.to_vec().try_into().expect("invalid first_index data"));

            let data = batchs
                .next()
                .expect("unreachable")?
                .expect("log is not empty, but failed to multiget last_index");
            let last_index =
                u64::from_be_bytes(data.to_vec().try_into().expect("invalid last_index data"));

            Ok(EntryMetadata {
                first_index,
                last_index,
                empty,
            })
        }

        fn is_empty(
            &self,
            log_cf: &Arc<BoundColumnFamily>,
        ) -> std::result::Result<bool, RocksdbError> {
            let key = DBEnv::format_empty_key(self.group_id, self.replica_id);
            let readopts = ReadOptions::default();
            let value = self
                .db
                .get_cf_opt(log_cf, key, &readopts)?
                .expect("unreachable: log empty flag always initialized");

            match String::from_utf8(value)
                .expect("unreachable: log empty data always utf8 valid")
                .as_str()
            {
                "true" => Ok(true),
                "false" => Ok(false),
                _ => unreachable!("log empty flag data invalid"),
            }
        }

        fn get_hard_state(&self) -> std::result::Result<HardState, RocksdbError> {
            let metacf = DBEnv::get_metadata_cf(&self.db);
            let key = DBEnv::format_hardstate_key(self.group_id, self.replica_id);
            let readopts = ReadOptions::default();
            self.db
                .get_cf_opt(&metacf, &key, &readopts)?
                .map_or(Ok(HardState::default()), |data| {
                    Ok(HardState::decode(data.as_ref()).unwrap()) // TODO: use difference serializer
                })
        }

        fn get_conf_state(&self) -> std::result::Result<ConfState, RocksdbError> {
            let metacf = DBEnv::get_metadata_cf(&self.db);
            let key = DBEnv::format_confstate_key(self.group_id, self.replica_id);
            let readopts = ReadOptions::default();
            self.db
                .get_cf_opt(&metacf, &key, &readopts)?
                .map_or(Ok(ConfState::default()), |data| {
                    Ok(ConfState::decode(data.as_ref()).unwrap()) // TODO: use difference serializer
                })
        }

        fn get_entry(&self, index: u64) -> std::result::Result<Entry, RocksdbError> {
            let logcf = DBEnv::get_log_cf(&self.db);
            let key = DBEnv::format_entry_key(self.group_id, index);
            let readopts = ReadOptions::default();
            match self.db.get_cf_opt(&logcf, &key, &readopts)? {
                None => panic!("index out of bounds: the index is {}", index),
                Some(data) => Ok(Entry::decode(data.as_ref()).unwrap()), // TODO: use difference serializer
            }
        }

        fn get_snapshot_metadata(&self) -> std::result::Result<SnapshotMetadata, RocksdbError> {
            let metacf = DBEnv::get_metadata_cf(&self.db);
            let key = DBEnv::format_snapshot_metadata_key(self.group_id, self.replica_id);
            let readopts = ReadOptions::default();
            self.db.get_cf_opt(&metacf, &key, &readopts)?.map_or(
                Ok(SnapshotMetadata::default()),
                |data| {
                    Ok(SnapshotMetadata::decode(data.as_ref()).unwrap()) // TODO: use difference serializer
                },
            )
        }

        fn set_snapshot_metadata(
            &self,
            meta: &SnapshotMetadata,
        ) -> std::result::Result<(), RocksdbError> {
            let cf = DBEnv::get_metadata_cf(&self.db);
            let key = DBEnv::format_snapshot_metadata_key(self.group_id, self.replica_id);
            let value = meta.encode_to_vec(); // TODO: use difference serializer
            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);
            self.db.put_cf_opt(&cf, key, value, &writeopts)
        }

        fn set_empty_flag(&self, flag: bool) -> std::result::Result<(), RocksdbError> {
            let logcf = DBEnv::get_log_cf(&self.db);
            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);
            self.db.put_cf_opt(
                &logcf,
                DBEnv::format_empty_key(self.group_id, self.replica_id),
                flag.to_string(),
                &writeopts,
            )
        }
    }

    impl<SR: RaftSnapshotReader, SW: RaftSnapshotWriter> RockStoreCore<SR, SW> {
        #[allow(unused)]
        pub(crate) fn append_unchecked(&self, ents: &[Entry]) {
            if ents.is_empty() {
                return;
            }

            let log_meta = self.get_entry_meta().unwrap();
            let log_cf = DBEnv::get_log_cf(&self.db);

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
            let log_cf = DBEnv::get_log_cf(&self.db);
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

    impl<SR: RaftSnapshotReader, SW: RaftSnapshotWriter> Storage for RockStoreCore<SR, SW> {
        fn initial_state(&self) -> RaftResult<RaftState> {
            let cf = DBEnv::get_metadata_cf(&self.db);

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
                .expect("unreachable: multiget for hard_state of initial_state")
                .map_err(|err| {
                    self.to_read_err(
                        err,
                        true,
                        false,
                        "initial_state: multiget for hard_state".into(),
                    )
                })?
                .map_or(HardState::default(), |data| {
                    let mut hs = HardState::default();
                    hs.merge(data.as_ref()).unwrap(); // TODO: handle error
                    hs
                });

            let conf_state = batches
                .next()
                .expect("unreachable: multiget for hard_state of initial_state")
                .map_err(|err| {
                    self.to_read_err(
                        err,
                        true,
                        false,
                        "initial_state: multiget for hard_state".into(),
                    )
                })?
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
                .map_err(|err| self.to_read_err(err, true, false, "entires".into()))?;

            if low < log_meta.first_index {
                error!(
                    "replica {}: entries compacted, low = {}, first_index = {}",
                    self.replica_id, low, log_meta.first_index
                );
                return Err(raft::Error::Store(raft::StorageError::Compacted));
            }

            if high > log_meta.last_index + 1 {
                panic!(
                    "index out of bound (last: {}, high: {})",
                    log_meta.last_index + 1,
                    high
                )
            }

            let high = std::cmp::min(high, log_meta.last_index + 1);

            let mut ents = Vec::with_capacity((high - low) as usize);
            let log_cf = DBEnv::get_log_cf(&self.db); // TODO handle error
            let start_key = DBEnv::format_entry_key(self.group_id, low);
            let iter_mode = IteratorMode::From(start_key.as_bytes(), rocksdb::Direction::Forward);
            let mut readopts = ReadOptions::default();
            readopts.set_ignore_range_deletions(true); // skip delete_range to improve read performance
                                                       // TODO: handle if temporaily unavailable
            let iter = self.db.iterator_cf_opt(&log_cf, readopts, iter_mode);

            // iterator enteris from [low, high)
            let mut next = low;
            for ent in iter {
                if next == high {
                    break;
                }

                let next_key = DBEnv::format_entry_key(self.group_id, next);
                let (key_data, value_data) = ent.unwrap();
                if next_key.as_bytes() != key_data.as_ref() {
                    break;
                }

                let ent = Entry::decode(value_data.as_ref())
                    .expect(format!("prase error {:?}", value_data).as_str()); // TODO: handle error
                ents.push(ent);
                next += 1;
            }

            raft::util::limit_size(&mut ents, max_size.into());

            Ok(ents)
        }

        fn term(&self, idx: u64) -> RaftResult<u64> {
            let snap_meta = self.get_snapshot_metadata().unwrap(); // already initialized
            if idx == snap_meta.index {
                return Ok(snap_meta.term);
            }

            let log_meta = self
                .get_entry_meta()
                .map_err(|err| self.to_read_err(err, true, false, "term".into()))?;

            if idx < log_meta.first_index {
                return Err(raft::Error::Store(raft::StorageError::Compacted));
            }

            if idx > log_meta.last_index {
                return Err(raft::Error::Store(raft::StorageError::Unavailable));
            }

            let log_cf = DBEnv::get_log_cf(&self.db);
            let key = DBEnv::format_entry_key(self.group_id, idx);
            let readopts = ReadOptions::default();
            let value = self
                .db
                .get_cf_opt(&log_cf, &key, &readopts)
                .map_err(|err| self.to_read_err(err, true, false, "term".into()))?
                .expect("unreachable: the entry index valid but can't got entry data");
            let ent = Entry::decode(value.as_ref()).unwrap();
            Ok(ent.term)
        }

        fn first_index(&self) -> RaftResult<u64> {
            let log_cf = DBEnv::get_log_cf(&self.db);
            let empty = self
                .is_empty(&log_cf)
                .map_err(|err| self.to_read_err(err, true, false, "first_index".into()))?;

            if empty {
                let snap_meta = self
                    .get_snapshot_metadata()
                    .map_err(|err| self.to_read_err(err, true, false, "first_index".into()))?;
                return Ok(snap_meta.index + 1);
            }

            let key = DBEnv::format_first_index_key(self.group_id, self.replica_id);
            let readopts = ReadOptions::default();
            let value = self
                .db
                .get_cf_opt(&log_cf, &key, &readopts)
                .map_err(|err| self.to_read_err(err, true, false, "first_index".into()))?
                .expect("unreachable: log empty flag is false, but get first_index data is none");
            let idx = u64::from_be_bytes(value.try_into().unwrap());
            Ok(idx)
        }

        fn last_index(&self) -> RaftResult<u64> {
            let log_cf = DBEnv::get_log_cf(&self.db);
            let empty = self
                .is_empty(&log_cf)
                .map_err(|err| self.to_read_err(err, true, false, "last_index".into()))?;

            if empty {
                let snap_meta = self
                    .get_snapshot_metadata()
                    .map_err(|err| self.to_read_err(err, true, false, "last_index".into()))?;
                return Ok(snap_meta.index);
            }

            let key = DBEnv::format_last_index_key(self.group_id, self.replica_id);
            let readopts = ReadOptions::default();
            let value = self
                .db
                .get_cf_opt(&log_cf, &key, &readopts)
                .map_err(|err| self.to_read_err(err, true, false, "last_index".into()))?
                .expect("unreachable: log empty flag is false, but get last_index data is none");
            let idx = u64::from_be_bytes(value.try_into().unwrap());
            Ok(idx)
        }

        fn snapshot(&self, request_index: u64, _to: u64) -> RaftResult<Snapshot> {
            let mut snap = Snapshot::default();
            // get snapshot data from user state machine.
            let data = self.rsnap.load_snapshot(self.group_id, self.replica_id)?;
            snap.set_data(data);

            // constructor snapshot metadata from store.
            let snap_meta = self
                .get_snapshot_metadata()
                .map_err(|err| self.to_read_err(err, false, true, "snapshot".into()))?;
            let cs = self
                .get_conf_state()
                .map_err(|err| self.to_read_err(err, false, true, "snapshot".into()))?;
            let hs = self
                .get_hard_state()
                .map_err(|err| self.to_read_err(err, false, true, "snapshot".into()))?;
            // We assume all entries whose indexes are less than `hard_state.commit`
            // have been applied, so use the latest commit index to construct the snapshot.
            // TODO: This is not true for async ready.
            let mut_meta = snap.mut_metadata();
            mut_meta.index = hs.commit;
            mut_meta.term = match mut_meta.index.cmp(&snap_meta.index) {
                std::cmp::Ordering::Equal => snap_meta.term,
                std::cmp::Ordering::Greater => {
                    // committed index greater than current snapshot index
                    self.get_entry(mut_meta.index)
                        .map_err(|err| self.to_read_err(err, false, true, "snapshot".into()))?
                        .term
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

    impl<SR: RaftSnapshotReader, SW: RaftSnapshotWriter> StorageExt for RockStoreCore<SR, SW> {
        fn set_hardstate(&self, hs: HardState) -> Result<()> {
            let metacf = DBEnv::get_metadata_cf(&self.db);
            let key = DBEnv::format_hardstate_key(self.group_id, self.replica_id);
            let value = hs.encode_to_vec(); // TODO: add feature for difference serializers.
            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);
            self.db
                .put_cf_opt(&metacf, &key, &value, &writeopts)
                .map_err(|err| {
                    self.to_write_err(
                        err,
                        true,
                        false,
                        format!("set_hard_state: hard_state = {:?}", hs),
                    )
                })
        }

        fn set_commit(&self, commit: u64) -> Result<()> {
            let mut hs = self.get_hard_state().unwrap();
            hs.commit = commit;
            self.set_hardstate(hs)
        }

        fn set_confstate(&self, cs: ConfState) -> Result<()> {
            let metacf = DBEnv::get_metadata_cf(&self.db);
            let key = DBEnv::format_confstate_key(self.group_id, self.replica_id);
            let value = cs.encode_to_vec(); // TODO: add feature for difference serializers.
            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);
            self.db
                .put_cf_opt(&metacf, &key, &value, &writeopts)
                .map_err(|err| {
                    self.to_write_err(
                        err,
                        true,
                        false,
                        format!("set_conf_state: conf_state = {:?}", cs),
                    )
                })
        }

        fn set_applied(&self, applied_index: u64, applied_term: u64) -> Result<()> {
            let applied = Applied {
                index: applied_index,
                term: applied_term,
            };

            let metacf = DBEnv::get_metadata_cf(&self.db);
            let key = DBEnv::format_applied_key(self.group_id, self.replica_id);
            let mut ser = flexbuffer_serialize(&applied).unwrap(); // FIXME: handle err
            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);
            self.db
                .put_cf_opt(&metacf, &key, ser.take_buffer(), &writeopts)
                .map_err(|err| {
                    self.to_write_err(
                        err,
                        true,
                        false,
                        format!("set_applied_index: applied_index = {:?}", applied),
                    )
                })
        }

        fn get_applied(&self) -> Result<(u64, u64)> {
            let metacf = DBEnv::get_metadata_cf(&self.db);
            let key = DBEnv::format_applied_key(self.group_id, self.replica_id);
            let readopts = ReadOptions::default();
            self.db
                .get_cf_opt(&metacf, &key, &readopts)
                .map_err(|err| self.to_write_err(err, true, false, format!("get_applied_index")))?
                .map_or(Ok((0, 0)), |data| {
                    let applied = flexbuffer_deserialize::<Applied>(&data).unwrap();
                    Ok((applied.index, applied.term))
                })
        }

        fn append(&self, ents: &[Entry]) -> Result<()> {
            if ents.is_empty() {
                return Ok(());
            }

            let ent_meta = self
                .get_entry_meta()
                .map_err(|err| self.to_write_err(err, true, false, "append".into()))?;

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

            let log_cf = DBEnv::get_log_cf(&self.db);

            // remove all entries overwritten by ents.
            if ents[0].index <= ent_meta.last_index {
                // FIXME: delete range has bug, see https://medium.com/@pingcap/how-we-found-a-data-corruption-bug-in-rocksdb-60e708769352
                // to get more information, we need refactor it.
                let start_key = DBEnv::format_entry_key(self.group_id, ents[0].index);
                let last_key = DBEnv::format_entry_key(self.group_id, ent_meta.last_index + 1);
                let mut writeopts = WriteOptions::default();
                writeopts.set_sync(true);
                self.db
                    .delete_range_cf_opt(&log_cf, &start_key, &last_key, &writeopts)
                    .map_err(|err| {
                        self.to_write_err(
                            err,
                            true,
                            false,
                            format!(
                                "append: delete entries ranges is start = {}, last = {}",
                                start_key, last_key
                            ),
                        )
                    })?;
            }

            // batch writes empty_flag (if need), first_index(if need), last_index and
            // entries to log column family.
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
            self.db
                .write_opt(batch, &writeopts)
                .map_err(|err| self.to_write_err(err, true, false, "append".into()))
        }

        fn install_snapshot(&self, mut snapshot: Snapshot) -> Result<()> {
            let mut snap_meta = snapshot.metadata.as_ref().expect("unreachable").clone();
            let ent_meta = self
                .get_entry_meta()
                .map_err(|err| self.to_write_err(err, false, true, "install_snapshot".into()))?;

            if ent_meta.first_index > snap_meta.index {
                return Err(Error::SnapshotOutOfDate);
            }

            if snap_meta == SnapshotMetadata::default() {
                return Ok(());
            }

            // save snapshot metadata
            self.set_snapshot_metadata(&snap_meta).map_err(|err| {
                self.to_write_err(
                    err,
                    false,
                    true,
                    format!("install_snapshot: meta = {:?}", snap_meta),
                )
            })?;
            // save snapshot data to user statemachine
            // TODO: consider save snapshot metadata to user statemachine.
            // TODO: consider use async method and add scheduler api
            self.wsnap
                .install_snapshot(self.group_id, self.replica_id, snapshot.take_data())?;

            // update hardstate
            let mut hs = self
                .get_hard_state()
                .map_err(|err| self.to_write_err(err, false, true, "install_snapshot".into()))?;
            hs.term = std::cmp::max(snap_meta.term, hs.term);
            hs.commit = snap_meta.index;
            self.set_hardstate(hs)?;

            // clear entries
            if !ent_meta.empty {
                // TODO: need add tests for here
                // FIXME: delete range has bug, see https://medium.com/@pingcap/how-we-found-a-data-corruption-bug-in-rocksdb-60e708769352
                // to get more information, we need refactor it.
                let cf = DBEnv::get_log_cf(&self.db);
                let start_key = DBEnv::format_entry_key(self.group_id, ent_meta.first_index);
                let last_key = DBEnv::format_entry_key(self.group_id, ent_meta.last_index + 1);
                let mut writeopts = WriteOptions::default();
                writeopts.set_sync(true);
                self.db
                    .delete_range_cf_opt(&cf, &start_key, &last_key, &writeopts)
                    .map_err(|err| {
                        self.to_write_err(
                            err,
                            false,
                            true,
                            format!(
                                "install_snapshot: clear entries ranges is start = {}, last = {}",
                                start_key, last_key
                            ),
                        )
                    })?;
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
        #[allow(unused)]
        node_id: u64,
        db: Arc<MDB>,
        rsnap: SR,
        wsnap: SW,
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
            // db_opts.set_comparator(name, compare_fn)

            let cfs = vec![
                ColumnFamilyDescriptor::new(METADATA_CF_NAME, db_opts.clone()),
                ColumnFamilyDescriptor::new(LOG_CF_NAME, db_opts.clone()),
            ];

            let db = MDB::open_cf_descriptors(&db_opts, &path, cfs).unwrap();
            Self {
                node_id,
                db: Arc::new(db),
                rsnap: snapshot_reader,
                wsnap: snapshot_writer,
            }
        }

        /// Convert rocksdb error to storage error.
        #[inline]
        fn to_storage_err(
            &self,
            group_id: u64,
            replica_id: u64,
            err: RocksdbError,
            op: String,
        ) -> Error {
            Error::from(ErrorHandler {
                ctx: ErrorContext {
                    node_id: self.node_id,
                    group_id,
                    replica_id,
                    operation: op,
                    is_log: false,
                    is_snap: false,
                },
                err,
            })
        }

        pub(crate) fn create_group_store_if_missing(
            &self,
            group_id: u64,
            replica_id: u64,
        ) -> std::result::Result<RockStoreCore<SR, SW>, RocksdbError> {
            let meta_cf = DBEnv::get_metadata_cf(&self.db);
            let key = self.group_store_key(group_id, replica_id);
            let readopts = ReadOptions::default();
            match self.db.get_cf_opt(&meta_cf, &key, &readopts)? {
                Some(_) => {
                    return Ok(RockStoreCore {
                        node_id: self.node_id,
                        group_id,
                        replica_id,
                        db: self.db.clone(),
                        rsnap: self.rsnap.clone(),
                        wsnap: self.wsnap.clone(),
                    })
                }
                None => RockStoreCore::<SR, SW>::new(
                    self.node_id,
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
                        .put_cf_opt(&meta_cf, key, b"".to_vec(), &writeopts)?;
                    Ok(core)
                }),
            }
        }

        fn get_replica_desc(
            &self,
            group_id: u64,
            replica_id: u64,
        ) -> std::result::Result<Option<ReplicaDesc>, RocksdbError> {
            let metacf = DBEnv::get_metadata_cf(&self.db);
            let key = DBEnv::format_replica_desc_key(group_id, replica_id);
            let readopts = ReadOptions::default();

            match self.db.get_pinned_cf_opt(&metacf, &key, &readopts)? {
                Some(data) => {
                    let rd = ReplicaDesc::decode(data.as_ref()).unwrap();
                    Ok(Some(rd))
                }
                None => Ok(None),
            }
        }

        fn set_replica_desc(
            &self,
            group_id: u64,
            rd: &ReplicaDesc,
        ) -> std::result::Result<(), RocksdbError> {
            let metacf = DBEnv::get_metadata_cf(&self.db);
            let key = DBEnv::format_replica_desc_key(group_id, rd.replica_id);
            let value = rd.encode_to_vec();
            let writeopts = WriteOptions::default();
            // TODO: with fsync by config
            self.db.put_cf_opt(&metacf, &key, value, &writeopts)
        }

        fn remove_replica_desc(
            &self,
            group_id: u64,
            replica_id: u64,
        ) -> std::result::Result<(), RocksdbError> {
            let metacf = DBEnv::get_metadata_cf(&self.db);
            let key = DBEnv::format_replica_desc_key(group_id, replica_id);
            let writeopts = WriteOptions::default();
            // TODO: with fsync by config
            self.db.delete_cf_opt(&metacf, &key, &writeopts)
        }

        fn search_replica_desc_on_node(
            &self,
            group_id: u64,
            target_node_id: u64,
        ) -> std::result::Result<Option<ReplicaDesc>, RocksdbError> {
            let metacf = DBEnv::get_metadata_cf(&self.db);
            let seek = DBEnv::format_replica_desc_seek_key(group_id);
            let iter_mode = IteratorMode::From(seek.as_bytes(), rocksdb::Direction::Forward);
            let readopts = ReadOptions::default();
            let iter = self.db.iterator_cf_opt(&metacf, readopts, iter_mode);
            for item in iter {
                let (key, value) = item?;

                let key = match std::str::from_utf8(&key) {
                    Ok(key) => key,
                    Err(_) => return Ok(None), /* cross the boundary of the seek prefix */
                };

                match key.starts_with(&seek) {
                    true => {
                        let rd = ReplicaDesc::decode(value.as_ref()).unwrap();
                        if rd.node_id == target_node_id {
                            return Ok(Some(rd));
                        }
                    }
                    false => return Ok(None), /* prefix is no longer matched */
                }
            }

            return Ok(None);
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
            async move {
                self.create_group_store_if_missing(group_id, replica_id)
                    .map_err(|err| {
                        self.to_storage_err(group_id, replica_id, err, "group_storage".into())
                    })
            }
        }

        type ReplicaDescFuture<'life0> = impl Future<Output = Result<Option<ReplicaDesc>>> + 'life0
    where
        Self: 'life0;
        fn get_replica_desc(&self, group_id: u64, replica_id: u64) -> Self::ReplicaDescFuture<'_> {
            async move {
                self.get_replica_desc(group_id, replica_id).map_err(|err| {
                    self.to_storage_err(group_id, replica_id, err, "get_replica_desc".into())
                })
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
                self.set_replica_desc(group_id, &replica_desc)
                    .map_err(|err| {
                        self.to_storage_err(
                            group_id,
                            replica_desc.replica_id,
                            err,
                            "set_replica_desc".into(),
                        )
                    })
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
                self.remove_replica_desc(group_id, replica_id)
                    .map_err(|err| {
                        self.to_storage_err(group_id, replica_id, err, "remove_replica_desc".into())
                    })
            }
        }

        type ReplicaForNodeFuture<'life0> = impl Future<Output = Result<Option<ReplicaDesc>>> + 'life0
    where
        Self: 'life0;

        fn replica_for_node(&self, group_id: u64, node_id: u64) -> Self::ReplicaForNodeFuture<'_> {
            async move {
                self.search_replica_desc_on_node(group_id, node_id)
                    .map_err(|err| {
                        self.to_storage_err(
                            group_id,
                            0, /*search replica can't provide replica_id */
                            err,
                            "replica_for_node".into(),
                        )
                    })
            }
        }
    }
}

mod state_machine {
    use std::collections::BTreeMap;
    use std::marker::PhantomData;
    use std::path::Path;
    use std::sync::Arc;

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

    use crate::prelude::ConfState;
    use crate::prelude::StoreData;
    use crate::storage::Error;
    use crate::storage::RaftSnapshotReader;
    use crate::storage::RaftSnapshotWriter;
    use crate::storage::Result as StorageResult;
    use crate::ProposeResponse;

    type Result<T> = std::result::Result<T, StateMachineStoreError>;

    #[derive(thiserror::Error, Debug)]
    pub enum StateMachineStoreError {
        #[error("{0} cloumn family is missing")]
        ColumnFamilyMissing(String),

        #[error("{0}")]
        Other(Box<dyn std::error::Error + Sync + Send>),
    }

    /// Constant for data column family name.
    const DATA_CF_NAME: &'static str = "data";

    /// Constant for snapshot column family name.
    const SNAP_CF_NAME: &'static str = "snapshot";

    /// Constant prerfix for snapshot and store in `SNAP_CF_NAME` column family.
    const SNAP_PREFIX: &'static str = "snapshot";

    /// Constant prerfix for applied index and store in `DATA_CF_NAME` column family.
    const APPLIED_INDEX_PREFIX: &'static str = "applied_index";

    /// Constant prerfix for applied term and store in `DATA_CF_NAME` column family.
    const APPLIED_TERM_PREFIX: &'static str = "applied_term";

    /// Constant prerfix for membership index and store in `DATA_CF_NAME` column family.
    const MEMBERSHIP_PREFIX: &'static str = "membership";

    /// Constant for split data key  
    const DATA_KEY_SPLIT_PAT: char = '_';

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

    /// Format membership key with mode `membership_{group_id}`.
    #[inline]
    fn format_membership_key(group_id: u64) -> String {
        format!("{}_{}", MEMBERSHIP_PREFIX, group_id)
    }

    /// Format snapshot key with mode `snapshot_{group_id}`.
    #[inline]
    fn format_snapshot_key(group_id: u64) -> String {
        format!("{}_{}", SNAP_PREFIX, group_id)
    }

    /// Format data key with mode `{group_id}_{raw_key}`.
    #[inline]
    fn format_data_key(group_id: u64, raw_key: &str) -> String {
        format!("{}_{}", group_id, raw_key)
    }

    /// Format data key prefix with mode `{group_id}_`.
    #[inline]
    fn format_data_key_prefix(group_id: u64) -> String {
        format!("{}_", group_id)
    }

    /// Split mode `{group_id}/{raw_key}` and return `(group_id, raw_key)`.
    /// Returned None if it doesn't fit the pattern.
    #[inline]
    fn split_data_key(key: &str) -> Option<(u64, String)> {
        let splied = key.split(DATA_KEY_SPLIT_PAT).collect::<Vec<_>>();

        if splied.is_empty() || splied.len() != 2 {
            return None;
        }

        let group_id = match splied[0].parse::<u64>() {
            Err(_) => return None,
            Ok(val) => val,
        };

        Some((group_id, splied[1].to_string()))
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

    impl From<SnapshotMembership> for ConfState {
        fn from(membership: SnapshotMembership) -> Self {
            ConfState {
                voters: membership.voters,
                learners: membership.learners,
                voters_outgoing: membership.voters_outgoing,
                learners_next: membership.learners_next,
                auto_leave: membership.auto_leave,
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

    impl<R> TryFrom<(u64, &StateMachineStore<R>)> for SnapshotDataSerializer
    where
        R: ProposeResponse,
    {
        type Error = StateMachineStoreError;

        fn try_from(val: (u64, &StateMachineStore<R>)) -> std::result::Result<Self, Self::Error> {
            let group_id = val.0;
            let state_machine = val.1;

            let cf = state_machine.get_data_cf()?;
            let prefix = format_data_key_prefix(group_id);
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
            serde_json::to_vec(self).map_err(|err| StateMachineStoreError::Other(Box::new(err)))
        }

        pub(crate) fn deserialize(data: &[u8]) -> Result<Self> {
            serde_json::from_slice::<SnapshotSerializer>(data)
                .map_err(|err| StateMachineStoreError::Other(Box::new(err)))
        }
    }

    impl<R> RaftSnapshotReader for StateMachineStore<R>
    where
        R: ProposeResponse,
    {
        fn load_snapshot(&self, group_id: u64, _replica_id: u64) -> StorageResult<Vec<u8>> {
            self.get_snapshot(group_id)
                .map_err(|err| Error::Other(Box::new(err)))
        }
    }

    impl<R> RaftSnapshotWriter for StateMachineStore<R>
    where
        R: ProposeResponse,
    {
        fn build_snapshot(
            &self,
            group_id: u64,
            _replica_id: u64,
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

    /*****************************************************************************
     * ROCK KEY VALUE STATE MACHINE
     *****************************************************************************/
    // struct KVStateMachine<R: WriteResponse> {
    //     group_id: u64,
    //     noop_tx: Option<Sender<ApplyNoOp>>,
    //     normal_tx: Option<Sender<ApplyNormal<StoreData, R>>>,
    //     member_tx: Option<Sender<ApplyMembership<R>>>,
    //     store: KVStore<R>, // TODO: use weak ref
    // }
    // impl<R> StateMachine<StoreData, R> for KVStateMachine<R>
    // where
    //     R: WriteResponse,
    // {
    //     type ApplyFuture<'life0> = impl Future<Output =  Option<IntoIter<Apply<StoreData, R>>>> + 'life0
    // where
    //     Self: 'life0;

    //     fn apply(
    //         &self,
    //         group_id: u64,
    //         _: &crate::multiraft::GroupState,
    //         iter: std::vec::IntoIter<crate::multiraft::Apply<StoreData, R>>,
    //     ) -> Self::ApplyFuture<'_> {
    //         async move {
    //             let mut batch = WriteBatch::default();
    //             let mut applied_index = 0;
    //             let mut applied_term = 0;
    //             let cf = self.store.get_data_cf().unwrap();
    //             for apply in iter {
    //                 match apply {
    //                     Apply::NoOp(noop) => {
    //                         applied_index = noop.entry_index;
    //                         applied_term = noop.entry_term;
    //                     }
    //                     Apply::Normal(normal) => {
    //                         let key = format_data_key(group_id, &normal.data.key);
    //                         batch.put_cf(&cf, key.as_bytes(), normal.data.value);
    //                         applied_index = normal.index;
    //                         applied_term = normal.term;
    //                     }
    //                     Apply::Membership(changes) => {
    //                         // changes.done().await.unwrap();
    //                         // TODO: set change to rocksdb
    //                         applied_index = changes.index;
    //                         applied_term = changes.term;
    //                     }
    //                 }
    //             }

    //             batch.put_cf(
    //                 &cf,
    //                 format_applied_index_key(group_id),
    //                 applied_index.to_be_bytes(),
    //             );
    //             batch.put_cf(
    //                 &cf,
    //                 format_applied_term_key(group_id),
    //                 applied_term.to_be_bytes(),
    //             );

    //             let mut writeopts = WriteOptions::default();
    //             writeopts.set_sync(true);
    //             self.store.db.write_opt(batch, &writeopts).unwrap();
    //             None
    //         }
    //     }
    // }

    /*****************************************************************************
     * APPLY WRITE BATCH
     *****************************************************************************/
    /// The `ApplyWriteBatch<R>` provides batch write `StoreData` and
    /// applied index and term to rocksdb.
    pub struct ApplyWriteBatch<R>
    where
        R: ProposeResponse,
    {
        db: Arc<DBWithThreadMode<MultiThreaded>>,
        batch: WriteBatch,
        group_id: u64,
        applied_index: u64,
        applied_term: u64,
        _m: PhantomData<R>,
    }

    impl<R> ApplyWriteBatch<R>
    where
        R: ProposeResponse,
    {
        /// Save the current applied index inside the batch.
        ///
        /// # Notes
        /// It's must be called when the apply goes to the state machine.
        ///
        /// # Panics
        /// If index is smaller than already applied index.
        #[inline]
        pub fn set_applied_index(&mut self, index: u64) {
            assert!(self.applied_index <= index);
            self.applied_index = index
        }

        /// Save the current applied index inside the batch.
        ///
        /// # Notes
        /// It's must be called when the apply goes to the state machine.
        ///
        /// # Panics
        /// If term is smaller than already applied term.
        #[inline]
        pub fn set_applied_term(&mut self, term: u64) {
            assert!(self.applied_term <= term);
            self.applied_term = term
        }

        /// Save the data to batch.
        #[inline]
        pub fn put_data(&mut self, data: &StoreData) {
            let cf = self.db.cf_handle(DATA_CF_NAME).unwrap();
            let key = format_data_key(self.group_id, &data.key);
            self.batch.put_cf(&cf, &key, &data.value);
        }

        #[inline]
        pub fn put_conf_state(&mut self, conf_state: &ConfState) {
            let cf = self.db.cf_handle(DATA_CF_NAME).unwrap();
            let key = format_membership_key(self.group_id);
            let to_membership = SnapshotMembership::from(conf_state.clone());
            let data = serde_json::to_vec(&to_membership).unwrap();
            self.batch.put_cf(&cf, &key, &data);
        }
    }

    /*****************************************************************************
     * STATE MACHINE STORE
     *****************************************************************************/
    /// The `StateMachineStore` implements snapshot, which provides a generic
    /// storage implementation for state machines to store multiple consensus
    /// group data using rocksdb. It uses a key-value model where the key is
    /// a string in UTF-8 valid format and the value is bytes of arbitraray
    /// length. It uses `StoreData` struct to represent this key-value model
    /// and uses flexbuffer serialization.
    #[derive(Clone)]
    pub struct StateMachineStore<R: ProposeResponse> {
        _node_id: u64,
        db: Arc<DBWithThreadMode<MultiThreaded>>,
        _m: PhantomData<R>,
    }

    // impl<R: WriteResponse> MultiStateMachine<StoreData, R> for KVStore<R> {
    //     type E = RockStateMachineError;
    //     type S = KVStateMachine<R>;
    //     fn create_state_machine(&self, group_id: u64) -> std::result::Result<Self::S, Self::E> {
    //         let (noop_tx, noop_rx) = channel(1);
    //         let (normal_tx, normal_rx) = channel(1);
    //         let (member_tx, memebr_rx) = channel(1);

    //         Ok(KVStateMachine {
    //             group_id,
    //             noop_tx: Some(noop_tx),
    //             normal_tx: Some(normal_tx),
    //             member_tx: Some(member_tx),
    //             store: self.clone(),
    //         })
    //     }
    // }

    impl<R: ProposeResponse> StateMachineStore<R> {
        /// Open a rocksdb using the path provided and open the rocksdb with
        /// the following opts:
        /// - CreateIfMissing
        /// - CreateIfMissingColumnFamilies
        pub fn new<P>(node_id: u64, path: P) -> Self
        where
            P: AsRef<Path>,
        {
            let mut db_opts = RocksdbOptions::default();
            db_opts.create_if_missing(true);
            db_opts.create_missing_column_families(true);

            let mut cfs = vec![];
            cfs.push(ColumnFamilyDescriptor::new(DATA_CF_NAME, db_opts.clone()));

            cfs.push(ColumnFamilyDescriptor::new(SNAP_CF_NAME, db_opts.clone()));

            let db = DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(&db_opts, &path, cfs)
                .unwrap();
            Self {
                _node_id: node_id,
                db: Arc::new(db),
                _m: PhantomData,
            }
        }

        /// Batch apply to `StateMachineStore`, which provides a convenient
        /// method for state machine apply.
        // pub fn apply(&self, group_id: u64, applys: &mut Vec<Apply<StoreData, R>>) -> Result<()> {
        //     let mut applied_index = 0;
        //     let mut applied_term = 0;
        //     let mut batch = WriteBatch::default();
        //     let cf = self.get_data_cf()?;
        //     for apply in applys {
        //         match apply {
        //             Apply::NoOp(noop) => {
        //                 applied_index = noop.entry_index;
        //                 applied_term = noop.entry_term;
        //             }
        //             Apply::Normal(normal) => {
        //                 let key = format_data_key(group_id, &normal.data.key);
        //                 batch.put_cf(&cf, key.as_bytes(), &normal.data.value);
        //                 applied_index = normal.index;
        //                 applied_term = normal.term;
        //             }
        //             Apply::Membership(membership) => {
        //                 applied_index = membership.index;
        //                 applied_term = membership.term;
        //             }
        //         }
        //     }

        //     batch.put_cf(
        //         &cf,
        //         format_applied_index_key(group_id),
        //         applied_index.to_be_bytes(),
        //     );
        //     batch.put_cf(
        //         &cf,
        //         format_applied_term_key(group_id),
        //         applied_term.to_be_bytes(),
        //     );

        //     let mut writeopts = WriteOptions::default();
        //     writeopts.set_sync(true);
        //     self.db
        //         .write_opt(batch, &writeopts)
        //         .map_err(|err| StateMachineStoreError::Other(Box::new(err)))
        // }

        /// Create a `ApplyWriteBatch<R>` for apply of `StateMachine`.
        pub fn write_batch_for_apply(&self, group_id: u64) -> ApplyWriteBatch<R> {
            ApplyWriteBatch {
                db: self.db.clone(),
                batch: WriteBatch::default(),
                group_id,
                applied_index: 0,
                applied_term: 0,
                _m: PhantomData,
            }
        }

        /// Consume `ApplyWriteBatch<R>` and write to rocksdb.
        pub fn write_apply_bath(&self, group_id: u64, mut batch: ApplyWriteBatch<R>) -> Result<()> {
            let cf = self.get_data_cf()?;
            batch.batch.put_cf(
                &cf,
                format_applied_index_key(group_id),
                batch.applied_index.to_be_bytes(),
            );
            batch.batch.put_cf(
                &cf,
                format_applied_term_key(group_id),
                batch.applied_term.to_be_bytes(),
            );

            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);
            self.db
                .write_opt(batch.batch, &writeopts)
                .map_err(|err| StateMachineStoreError::Other(Box::new(err)))
        }

        /// Save current tuple (applied_index, applied_term) to data column of rocksdb.
        pub fn set_applied(
            &self,
            group_id: u64,
            val: (u64, u64), /* index, term */
        ) -> Result<()> {
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
                .map_err(|err| StateMachineStoreError::Other(Box::new(err)))
        }

        #[allow(unused)]
        pub fn set_applied_index(&self, group_id: u64, apply_index: u64) -> Result<()> {
            let cf = self.get_data_cf()?;
            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);
            self.db
                .put_cf_opt(
                    &cf,
                    format_applied_index_key(group_id),
                    apply_index.to_be_bytes(),
                    &writeopts,
                )
                .map_err(|err| StateMachineStoreError::Other(Box::new(err)))
        }

        #[allow(unused)]
        pub fn set_applied_term(&self, group_id: u64, apply_term: u64) -> Result<()> {
            let cf = self.get_data_cf()?;
            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);
            self.db
                .put_cf_opt(
                    &cf,
                    format_applied_term_key(group_id),
                    apply_term.to_be_bytes(),
                    &writeopts,
                )
                .map_err(|err| StateMachineStoreError::Other(Box::new(err)))
        }

        /// Get saved applied index and term
        #[allow(unused)]
        pub fn get_applied(&self, group_id: u64) -> Result<(u64, u64)> {
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
                .map_err(|err| StateMachineStoreError::Other(Box::new(err)))?
                .map_or(0, |data| {
                    u64::from_be_bytes(data.as_ref().try_into().unwrap())
                });

            let term = iter
                .next()
                .unwrap()
                .map_err(|err| StateMachineStoreError::Other(Box::new(err)))?
                .map_or(0, |data| {
                    u64::from_be_bytes(data.as_ref().try_into().unwrap())
                });

            Ok((index, term))
        }

        /// Get snapshot cloumn famly.
        #[inline]
        fn get_snapshot_cf(&self) -> Result<Arc<BoundColumnFamily>> {
            self.db.cf_handle(SNAP_CF_NAME).map_or(
                Err(StateMachineStoreError::ColumnFamilyMissing(
                    SNAP_CF_NAME.to_owned(),
                )),
                |cf| Ok(cf),
            )
        }

        /// Get data cloumn famly.
        #[inline]
        fn get_data_cf(&self) -> Result<Arc<BoundColumnFamily>> {
            self.db.cf_handle(DATA_CF_NAME).map_or(
                Err(StateMachineStoreError::ColumnFamilyMissing(
                    DATA_CF_NAME.to_owned(),
                )),
                |cf| Ok(cf),
            )
        }

        /// Save current snapshot raw datas to snapshot column of rocksdb.
        fn set_snapshot(&self, group_id: u64, data: &[u8]) -> Result<()> {
            let cf = self.get_snapshot_cf()?;
            let key = format_snapshot_key(group_id);
            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);
            self.db
                .put_cf_opt(&cf, key, data, &writeopts)
                .map_err(|err| StateMachineStoreError::Other(Box::new(err)))
        }

        // Get current snapshot raw datas from snapshot column of rocksdb.
        fn get_snapshot(&self, group_id: u64) -> Result<Vec<u8>> {
            let cf = self.get_snapshot_cf()?;
            let readopts = ReadOptions::default();
            let key = format_snapshot_key(group_id);
            self.db
                .get_pinned_cf_opt(&cf, &key, &readopts)
                .map_err(|err| StateMachineStoreError::Other(Box::new(err)))?
                .map_or(Ok(vec![]), |data| Ok(data.to_vec()))
        }

        /// Get current conf_state from rocksdb.s
        pub fn get_conf_state(&self, group_id: u64) -> Result<ConfState> {
            let cf = self.get_data_cf()?;
            let key = format_membership_key(group_id);
            let readopts = ReadOptions::default();
            self.db
                .get_pinned_cf_opt(&cf, &key, &readopts)
                .map_err(|err| StateMachineStoreError::Other(Box::new(err)))?
                .map_or(Ok(ConfState::default()), |data| {
                    let membership: SnapshotMembership =
                        serde_json::from_slice(data.as_ref()).unwrap();
                    Ok(membership.into())
                })
        }

        /// Save conf_state to rocksdb.s
        pub fn save_conf_state(&self, group_id: u64, conf_state: ConfState) -> Result<()> {
            let membership = SnapshotMembership::from(conf_state);
            let cf = self.get_data_cf()?;
            let key = format_membership_key(group_id);
            let value = serde_json::to_vec(&membership)
                .map_err(|err| StateMachineStoreError::Other(Box::new(err)))?;
            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);
            self.db
                .put_cf_opt(&cf, key, value, &writeopts)
                .map_err(|err| StateMachineStoreError::Other(Box::new(err)))
        }

        /// Save current last membership to data column of rocksdb.
        fn set_membership(
            &self,
            group_id: u64,
            last_memebrship: &SnapshotMembership,
        ) -> Result<()> {
            let cf = self.get_data_cf()?;
            let key = format_membership_key(group_id);
            let value = serde_json::to_vec(last_memebrship)
                .map_err(|err| StateMachineStoreError::Other(Box::new(err)))?;
            let mut writeopts = WriteOptions::default();
            writeopts.set_sync(true);
            self.db
                .put_cf_opt(&cf, key, value, &writeopts)
                .map_err(|err| StateMachineStoreError::Other(Box::new(err)))
        }

        fn restore(&self, group_id: u64, data: Vec<u8>) -> Result<()> {
            let mut batch = WriteBatch::default();
            let serializer = SnapshotSerializer::deserialize(&data)?;
            // restore meta
            self.set_applied(
                group_id,
                (serializer.meta.applied_index, serializer.meta.applied_term),
            )?;
            self.set_membership(group_id, &serializer.meta.last_membership)?;

            // restore data
            let cf = self.get_data_cf().unwrap();
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

    #[cfg(test)]
    mod tests {
        use serde::Deserialize;
        use serde::Serialize;

        use super::*;

        #[test]
        fn test_rocksdata_serialize() {
            let data1 = StoreData {
                key: "key".to_owned(),
                value: "value".as_bytes().to_owned(),
            };

            let mut s = flexbuffers::FlexbufferSerializer::new();
            data1.serialize(&mut s).unwrap();

            let data2 =
                StoreData::deserialize(flexbuffers::Reader::get_root(s.view()).unwrap()).unwrap();
            assert_eq!(data1.key, data2.key);
            assert_eq!(data1.value, data2.value);
        }
    }
}

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
    use serde::Serialize;

    use super::state_machine::SnapshotDataSerializer;
    use super::state_machine::SnapshotMetaSerializer;
    use super::state_machine::SnapshotSerializer;
    use super::StateMachineStore;
    // use super::KVStateMachine;
    use super::RockStore;
    use crate::multiraft::ProposeResponse;
    use crate::prelude::ConfState;
    use crate::prelude::Entry;
    use crate::prelude::ReplicaDesc;
    use crate::prelude::Snapshot;
    use crate::protos::StoreData;
    use crate::storage::MultiRaftStorage;
    use crate::storage::RaftSnapshotWriter;
    use crate::storage::StorageExt;
    use crate::Apply;
    use crate::ApplyNormal;

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

    fn new_rockdata_apply<R: ProposeResponse>(
        group_id: u64,
        index: u64,
        term: u64,
        data: StoreData,
    ) -> Apply<StoreData, R> {
        let mut s = flexbuffers::FlexbufferSerializer::new();
        data.serialize(&mut s).unwrap();
        Apply::Normal(ApplyNormal {
            group_id,
            index,
            term,
            data,
            is_conf_change: false,
            context: None,
            tx: None,
        })
    }

    fn new_snapshot_serializer_from_entries(
        index: u64,
        term: u64,
        voters: Vec<u64>,
        rockdatas: Vec<StoreData>,
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

    fn new_state_machine<R: ProposeResponse>(path: &Path, node_id: u64) -> StateMachineStore<R> {
        let state_machine = StateMachineStore::<R>::new(node_id, path);

        println!("🐿 create state machine store {}", path.display());

        state_machine
    }

    fn new_rockstore<R: ProposeResponse>(
        path: &Path,
        node_id: u64,
        state_machine: &StateMachineStore<R>,
    ) -> RockStore<StateMachineStore<R>, StateMachineStore<R>> {
        let rock_store =
            RockStore::new(node_id, path, state_machine.clone(), state_machine.clone());

        println!("🚛 create raft store {}", path.display());
        rock_store
    }

    fn db_test_env<F, R>(f: F)
    where
        F: FnOnce(&RockStore<StateMachineStore<R>, StateMachineStore<R>>, &StateMachineStore<R>),
        R: ProposeResponse,
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
            &'r RockStore<StateMachineStore<R>, StateMachineStore<R>>,
            &'r StateMachineStore<R>,
        ) -> BoxFuture<'r, ()>,
        R: ProposeResponse,
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
            StoreData {
                key: rand_data(4),
                value: rand_data(8).into(),
            },
            StoreData {
                key: rand_data(4),
                value: rand_data(8).into(),
            },
            StoreData {
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
                let mut s = flexbuffers::FlexbufferSerializer::new();
                let _ = match apply {
                    Apply::Normal(normal) => normal.data.serialize(&mut s).unwrap(),
                    _ => unreachable!(),
                };
                new_rockdata_entry(apply.get_index(), apply.get_term(), &s.take_buffer())
            })
            .collect::<Vec<_>>();

        let mut tests = vec![
            // applys, apply index, snapshot
            (
                vec![
                    new_rockdata_apply::<()>(group_id, 3, 3, datas[0].clone()),
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
        for (i, (mut applys, apply_idx, wresult, windex)) in tests.drain(..).enumerate() {
            let conf_state = conf_state.clone();
            let ents = ents.clone();
            db_test_async_env::<_, ()>(|_node_id, rock_store, state_machine| {
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

                    let mut batch = state_machine.write_batch_for_apply(group_id);
                    for apply in applys.iter_mut() {
                        match apply {
                            Apply::NoOp(noop) => {
                                batch.set_applied_index(noop.index);
                                batch.set_applied_term(noop.term);
                            }
                            Apply::Normal(normal) => {
                                batch.put_data(&normal.data);
                                batch.set_applied_index(normal.index);
                                batch.set_applied_term(normal.term);
                            }
                            Apply::Membership(membership) => {
                                batch.set_applied_index(membership.index);
                                batch.set_applied_term(membership.term);
                            }
                        }
                    }
                    state_machine.write_apply_bath(group_id, batch).unwrap();
                    // state_machine.apply(group_id, &mut applys).unwrap();
                    // .await
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

pub use storage::{RockStore, RockStoreCore};

pub use state_machine::{ApplyWriteBatch, StateMachineStore, StateMachineStoreError};
