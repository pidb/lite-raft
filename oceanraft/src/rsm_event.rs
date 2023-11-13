#[derive(Debug)]
pub struct LeaderElectionEvent {
    pub node_id: u64,
    pub group_id: u64,
    pub leader_id: u64,
    pub replica_id: u64,
    pub term: u64,
}

#[derive(Debug)]
pub struct GroupCreateEvent {
    pub node_id: u64,
    pub group_id: u64,
    pub replica_id: u64,
}
