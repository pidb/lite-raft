#[derive(thiserror::Error, Debug)]
pub enum Error {
     /// A raft group error occurred.
     #[error("{0}")]
    RaftGroupError(#[from] RaftGroupError),

    #[error("raft group ({0}) already exists")]
    RaftGroupAlreayExists(u64),
    
    

    #[error("inconsistent replica id: passed {0}, but found {1} by scanning conf_state for store {2}")]
    InconsistentReplicaId(u64, u64, u64),

    // the tuple is (group_id, store_id)s
    #[error("couldn't find replica id for this store ({1}) in group ({0})")]
    ReplicaNotFound(u64, u64),
}

#[derive(thiserror::Error, Debug)]
pub enum RaftGroupError {
    #[error("bootstrap group ({0}) error, the voters of initial_state is empty in store ({1})")]
    BootstrapError(u64, u64),
}