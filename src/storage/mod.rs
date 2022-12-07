mod memory;
mod storage;

pub use self::storage::transmute_entries;
pub use self::storage::transmute_entry;
pub use self::storage::transmute_error;
pub use self::storage::transmute_raft_state;
pub use self::storage::transmute_snapshot;
pub use self::storage::RaftStorageImpl;
pub use self::storage::MultiRaftStorage;
pub use self::storage::RaftSnapshotBuilder;
pub use self::storage::RaftState;
pub use self::storage::RaftStorage;
pub use self::storage::Result;
pub use self::storage::StorageError;
pub use self::memory::MemStorage;
pub use self::memory::MemStorageCore;
pub use self::memory::MultiRaftGroupMemoryStorage;
