use tokio::sync::oneshot;

use crate::proto::Entry;

use super::error::Error;

#[derive(Debug)]
pub struct LeaderElectionEvent {
    pub group_id: u64,
    pub leader_id: u64,
    pub committed_term: u64,
}

#[derive(Debug)]
pub struct ApplyEvent {
    pub group_id: u64,
    pub entry: Entry,
    pub is_conf_change: bool,
    pub tx: Option<oneshot::Sender<Result<(), Error>>>,
}

#[derive(Debug)]
pub enum Event {
    LederElection(LeaderElectionEvent),

    Apply(ApplyEvent),
}
