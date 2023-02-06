pub type Result<T> = std::result::Result<T, Error>;

/// RaftCoreError is raft::Error re-exported.
pub type RaftCoreError = raft::Error;

#[derive(thiserror::Error, Debug, Clone, PartialEq)]
pub enum TransportError {
    #[error("the node {0} of server not found")]
    ServerNodeFound(u64),

    #[error("the node {0} of server already exists")]
    ServerAlreadyExists(u64),

    #[error("server error: {0}")]
    Server(String),
}

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

    #[error("raft group not exist, node_id = {1}, group_d = {1}")]
    NotExist(u64, u64),

    #[error("the raft group ({1}) already exists in node ({0}")]
    Exists(u64, u64),
}



#[derive(thiserror::Error, Debug)]
pub enum ProposalError {
    #[error("not leader")] // TODO: more error info
    NotLeader {
        // node_id: u64, TODO: support node id
        group_id: u64,
        replica_id: u64,
        // reditect_node: Option<> TODO: support redirect hint
    },

    #[error("unexpected at index = {0}")]
    Unexpected(u64),

    #[error("stale at term = {0}")]
    Stale(u64),

    #[error("group = {0}, replcia = {1} removed")]
    GroupRemoved(u64, u64),

    #[error("{0}")]
    Other(#[from] Box<dyn std::error::Error + Sync + Send>),
}

impl PartialEq for ProposalError {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (ProposalError::NotLeader{..}, ProposalError::NotLeader{..})
                | (ProposalError::Unexpected(..), ProposalError::Unexpected(..))
                | (ProposalError::Stale(..), ProposalError::Stale(..))
                | (ProposalError::Other(..), ProposalError::Other(..)),
        )
        
    }
}


#[derive(thiserror::Error, Debug, PartialEq)]
pub enum Error {
    #[error("{0}")]
    BadParameter(String),

    #[error("the multiraft stopped")]
    Stop,
    
    #[error("{0}")]
    Internal(String),

    /// Raft storage error occurred.
    #[error("{0}")]
    Store(#[from] MultiRaftStorageError),

    /// Transport error occurred.
    #[error("{0}")]
    Transport(#[from] TransportError),

    /// Proposal error occurred.
    #[error("{0}")]
    Proposal(#[from] ProposalError),

    // #[error("{0}")]
    // RaftGroup(#[from] ::raft::Error),
    /// A raft error occurred.
    #[error("{0}")]
    Raft(#[from] RaftCoreError),

    #[error("{0}")]
    RaftGroup(#[from] RaftGroupError),

    // #[error(
    //     "inconsistent replica id: passed {0}, but found {1} by scanning conf_state for store {2}"
    // )]
    // InconsistentReplicaId(u64, u64, u64),

    // // the tuple is (group_id, store_id)s
    // #[error("couldn't find replica id for this store ({1}) in group ({0})")]
    // ReplicaNotFound(u64, u64),
}



