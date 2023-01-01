#[derive(Debug)]
pub struct LeaderElectionEvent {
    pub group_id: u64,
    pub leader_id: u64,
    pub committed_term: u64,
}


#[derive(Debug)]
pub enum Event {
    LederElection(LeaderElectionEvent),
}


