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

use super::Error;
use super::MultiRaftStorage;
use super::RaftSnapshotReader;
use super::RaftSnapshotWriter;
use super::RaftStorage;
use super::RaftStorageReader;
use super::RaftStorageWriter;
use super::Result;

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

mod rock_statemachine {
    use std::marker::PhantomData;
    use std::sync::Arc;
    use std::vec::IntoIter;

    use futures::Future;
    use rocksdb::DBWithThreadMode;
    use rocksdb::MultiThreaded;

    use crate::multiraft::response::AppWriteResponse;
    use crate::multiraft::storage::RaftSnapshotReader;
    use crate::multiraft::storage::RaftSnapshotWriter;
    use crate::multiraft::storage::Result;
    use crate::multiraft::types::Decode;
    use crate::multiraft::types::Encode;
    use crate::multiraft::types::WriteData;
    use crate::multiraft::Apply;
    use crate::multiraft::StateMachine;
    use crate::prelude::Snapshot;
    use crate::prelude::SnapshotMetadata;

    impl Encode for RocksData {
        type EncodeError = RocksDataError;

        fn encode(&mut self) -> std::result::Result<Vec<u8>, Self::EncodeError> {
            let mut bytes = Vec::new();
            unsafe {
                abomonation::encode(self, &mut bytes)
                    .map_err(|err| RocksDataError::Encode(err.to_string()))?;
            }

            Ok(bytes)
        }
    }

    impl Decode for RocksData {
        type DecodeError = RocksDataError;

        fn decode(&mut self, bytes: &mut [u8]) -> std::result::Result<(), Self::DecodeError> {
            let (rocks_data, remaining) = unsafe {
                abomonation::decode::<RocksData>(bytes).map_or(
                    Err(RocksDataError::Decode("decode failed".to_owned())),
                    |v| Ok(v),
                )?
            };
            assert_eq!(remaining.len(), 0);
            *self = rocks_data.clone();
            Ok(())
        }
    }

    impl WriteData for RocksData {}

    #[derive(Clone, Default, abomonation_derive::Abomonation)]
    pub struct RocksData {
        #[unsafe_abomonate_ignore]
        pub key: Vec<u8>,

        #[unsafe_abomonate_ignore]
        pub value: Vec<u8>,
    }

    #[derive(thiserror::Error, Debug)]
    pub enum RocksDataError {
        #[error("{0}")]
        Encode(String),
        #[error("{0}")]
        Decode(String),
    }

    #[test]
    fn test_rocksdata_serialize() {
        let mut data1 = RocksData {
            key: "key".as_bytes().to_owned(),
            value: "value".as_bytes().to_owned(),
        };

        let mut encoded = data1.encode().unwrap();
        let mut data2 = RocksData::default();
        data2.decode(&mut encoded).unwrap();
        assert_eq!(data1.key, data2.key);
        assert_eq!(data1.value, data2.value);
    }

    #[derive(Clone)]
    pub struct RockStateMachine<R: AppWriteResponse> {
        db: Arc<DBWithThreadMode<MultiThreaded>>,
        _m: PhantomData<R>,
    }

    impl<R> StateMachine<R> for RockStateMachine<R>
    where
        R: AppWriteResponse,
    {
        type ApplyFuture<'life0> = impl Future<Output =  Option<IntoIter<Apply<R>>>> + 'life0
        where
            Self: 'life0;

        fn apply(
            &mut self,
            group_id: u64,
            state: &crate::multiraft::GroupState,
            iter: std::vec::IntoIter<crate::multiraft::Apply<R>>,
        ) -> Self::ApplyFuture<'_> {
            async move { unimplemented!() }
        }
    }

    impl<R> RaftSnapshotReader for RockStateMachine<R>
    where
        R: AppWriteResponse,
    {
        fn load_snapshot(
            &self,
            group_id: u64,
            replica_id: u64,
            request_index: u64,
            to: u64,
        ) -> Result<Snapshot> {
            unimplemented!()
        }

        fn snapshot_metadata(&self, group_id: u64, replica_id: u64) -> Result<SnapshotMetadata> {
            unimplemented!()
        }
    }

    impl<R> RaftSnapshotWriter for RockStateMachine<R>
    where
        R: AppWriteResponse,
    {
        fn build_snapshot(&self, group_id: u64, replica_id: u64, applied: u64) -> Result<()> {
            unimplemented!()
        }

        fn save_snapshot(&self, group_id: u64, replica_id: u64, snapshot: Snapshot) -> Result<()> {
            unimplemented!()
        }
    }
}

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
struct RockStoreCore<SR: RaftSnapshotReader, SW: RaftSnapshotWriter> {
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

    #[inline]
    fn entry_key(&self, index: u64) -> String {
        format!("{}_{}", self.group_id, index)
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
    fn set_empty_logs_flag_with_batch(&self, cf: &Arc<BoundColumnFamily>, batch: &mut WriteBatch) {
        let key = DBEnv::empty_logs_flag_key(self.group_id, self.replica_id);
        let value = "true".as_bytes();
        batch.put_cf(cf, key, value)
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
        let start_key = self.entry_key(low);
        let last_key = self.entry_key(high);
        let iter_mode = IteratorMode::From(start_key.as_bytes(), rocksdb::Direction::Forward);
        let readopts = ReadOptions::default();
        // TODO: handle if temporaily unavailable
        let iter = self.db.iterator_cf_opt(&log_cf, readopts, iter_mode);
        for ent in iter {
            let (key_data, value_data) = ent.unwrap();
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
        let key = self.entry_key(idx);
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

        for ent in ents.iter() {
            let key = format!("{}_{}", self.group_id, ent.index);
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
struct RockStore<SR, SW>
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

        let raft_log_cf_opts = RocksdbOptions::default();
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

    fn create_group_store(&self, group_id: u64, replica_id: u64) -> Result<RockStoreCore<SR, SW>> {
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

    fn create_group_store_if_missing(
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

#[cfg(test)]
mod tests {}
