// pub type Result<T> = std::result::Result<T, Error>;

/// RaftCoreError is raft::Error re-exported.
pub type RaftCoreError = raft::Error;

/// An error with the storage.
#[derive(Debug, thiserror::Error)]
pub enum MultiRaftStorageError {
    /// Some other error occurred.
    #[error("unknown error {0}")]
    Other(#[from] Box<dyn std::error::Error + Sync + Send>),
}

impl PartialEq for MultiRaftStorageError {
    // #[cfg_attr(feature = "cargo-clippy", allow(clippy::match_same_arms))]
    fn eq(&self, other: &MultiRaftStorageError) -> bool {
        matches!(
            (self, other),
            (
                MultiRaftStorageError::Other(_),
                MultiRaftStorageError::Other(_)
            ),
            // (
            //     MultiRaftStorageError::RaftStorage(..),
            //     MultiRaftStorageError::RaftStorage(..)
            // )
        )
    }
}

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum RaftGroupError {
    // #[error("the proposal need leader role, the current leader at {0}")]
    // NotLeader(u64, u64, u64),

    // #[error("bootstrap group ({0}) error, the voters of initial_state is empty in store ({1})")]
    // BootstrapError(u64, u64),
    #[error("raft group not exist, node_id = {1}, group_id = {1}")]
    NotExist(u64, u64),

    #[error("raft group deleted, node_id = {1}, group_id = {1}")]
    Deleted(u64, u64),

    #[error("group({1}) already exists in node({0})")]
    Exists(u64, u64),
}

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum ChannelError {
    #[error("{0}")]
    Full(String),

    #[error("{0}")]
    SenderClosed(String),

    #[error("{0}")]
    ReceiverClosed(String),
}

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum ProposeError {
    // TODO: more error info
    #[error("node {node_id:?} not leader: group = {group_id:?}, replica = {replica_id:?}")]
    NotLeader {
        node_id: u64,
        group_id: u64,
        replica_id: u64,
    },

    #[error("stale write: expected is term {0}, current term is {1}")]
    Stale(u64, u64),

    #[error("node {node_id:?}: got unexpected index during proposal at group {group_id:?}, expected {expected:?}, got {unexpected:?}")]
    UnexpectedIndex {
        node_id: u64,
        group_id: u64,
        replica_id: u64,
        expected: u64,
        unexpected: u64,
    },

    #[error("node {0}: has pending membership change is being processed on group {1}")]
    MembershipPending(u64 /* node_id */, u64 /* group_id */),
}

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum NodeActorError {
    #[error("the multiraft actor stopped")]
    Stopped,
}

/// Wrap serialization errors that occurred for specific types
#[derive(thiserror::Error, Debug)]
pub enum SerializationError {
    /// An error occurred when encode with prost.
    #[error("{0}")]
    Prost(#[from] prost::EncodeError),

    /// An error occurred when serializing with flexbuffer.
    #[error("{0}")]
    Flexbuffer(#[from] flexbuffers::SerializationError),
}

/// Wrap deserialization errors that occurred for specific types
#[derive(thiserror::Error, Debug)]
pub enum DeserializationError {
    /// An error occurred when decode with prost.
    #[error("{0}")]
    Prost(#[from] prost::DecodeError),

    /// An error occurred when deserializing with flexbuffer.
    #[error("{0}")]
    Flexbuffer(#[from] flexbuffers::DeserializationError),
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// The configuration is invalid.
    #[error("{0}")]
    ConfigInvalid(String),

    #[error("{0}")]
    BadParameter(String),

    #[error("{0}")]
    Channel(#[from] ChannelError),

    /// An error occurred during the proposal.
    #[error("{0}")]
    Propose(#[from] ProposeError),

    #[error("{0}")]
    NodeActor(#[from] NodeActorError),

    #[error("{0}")]
    Storage(#[from] super::storage::Error),

    #[error("{0}")]
    Serialization(#[from] SerializationError),

    #[error("{0}")]
    Deserialization(#[from] DeserializationError),

    /// A raft error occurred.
    #[error("{0}")]
    Raft(#[from] RaftCoreError),

    #[error("{0}")]
    RaftGroup(#[from] RaftGroupError),
}
