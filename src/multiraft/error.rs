use super::transport::TransportError;
use crate::storage::StorageError;

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum Error {
    #[error("{0}")]
    BadParameter(String),

    /// Raft storage error occurred.
    #[error("{0}")]
    Store(#[from] StorageError),

    /// Transport error occurred.
    #[error("{0}")]
    Transport(#[from] TransportError),

    /// Proposal error occurred.
    #[error("{0}")]
    Proposal(#[from] ProposalError),

    #[error("{0}")]
    RaftGroup(#[from] ::raft::Error),

    /// A raft group error occurred.
    #[error("{0}")]
    RaftGroupError(#[from] RaftGroupError),

    #[error("raft group ({0}) already exists")]
    RaftGroupAlreayExists(u64),

    #[error(
        "inconsistent replica id: passed {0}, but found {1} by scanning conf_state for store {2}"
    )]
    InconsistentReplicaId(u64, u64, u64),

    // the tuple is (group_id, store_id)s
    #[error("couldn't find replica id for this store ({1}) in group ({0})")]
    ReplicaNotFound(u64, u64),
}

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum RaftGroupError {
    #[error("bootstrap group ({0}) error, the voters of initial_state is empty in store ({1})")]
    BootstrapError(u64, u64),
}

#[derive(thiserror::Error, Debug)]
pub enum ProposalError {
    #[error("unexpected at index = {0}")]
    Unexpected(u64),

    #[error("stale at term = {0}")]
    Stale(u64),

    #[error("{0}")]
    Other(#[from] Box<dyn std::error::Error + Sync + Send>),
}

impl PartialEq for ProposalError {
    fn eq(&self, other: &Self) -> bool {
        match self {
            ProposalError::Unexpected(v1) => match other {
                ProposalError::Unexpected(v2) => v1 == v2,
                ProposalError::Stale(_) => false,
                ProposalError::Other(_) => false,
            },
            ProposalError::Stale(v1) => match other {
                ProposalError::Unexpected(_) => false,
                ProposalError::Stale(v2) => v1 == v2,
                ProposalError::Other(_) => false,
            },
            ProposalError::Other(v1) => match other {
                ProposalError::Unexpected(_) => false,
                ProposalError::Stale(_) => false,
                ProposalError::Other(v2) => matches!(v1, v2),
            },
        }
    }
}
