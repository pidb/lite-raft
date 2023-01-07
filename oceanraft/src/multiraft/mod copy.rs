mod memory;
mod storage;
// mod rocksdb;
pub use self::storage::MultiRaftStorage;
pub use self::storage::RaftSnapshotBuilder;
pub use self::storage::Result;
pub use self::storage::MultiRaftStorageError;
pub use self::memory::MultiRaftMemoryStorage;
