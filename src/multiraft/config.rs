#[derive(Clone, Debug)]
/// RaftGroup configuration in physical node.
pub struct GroupConfig {
    pub election_tick: usize,
    pub heartbeat_tick: usize,
    pub tick_interval: u64, // ms
}
