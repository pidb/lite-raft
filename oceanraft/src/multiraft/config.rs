#[derive(Clone, Debug)]
/// RaftGroup configuration in physical node.
pub struct Config {
    pub node_id: u64,
    pub election_tick: usize,
    pub heartbeat_tick: usize,
    pub tick_interval: u64, // ms
    pub batch_apply: bool,
    pub batch_size: usize,

    /// The size of the FIFO queue for write requests, default is `1`.
    /// 
    /// > Note: Consensus groups handles write proposals sequentially. 
    /// > the write proposal queue are used to concurrently write to multiple consensus groups. 
    /// > The request queue is shared among all groups on the node, which means 
    /// that the value is set based on the number of consensus groups on the node. 
    pub write_proposal_queue_size: usize,
}


impl Default for Config {
    fn default() -> Self {
        Config {
            node_id: 0,
            election_tick: 1,
            heartbeat_tick: 3,
            tick_interval: 10,
            batch_apply: false,
            batch_size: 0,
            write_proposal_queue_size: 1,
        }
    }
}