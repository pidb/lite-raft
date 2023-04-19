use crate::Error;

/// A constant represents invalid node id of oceanraft node.
pub const INVALID_NODE_ID: u64 = 0;

const HEARTBEAT_TICK: usize = 2;

#[derive(Clone, Debug)]
/// RaftGroup configuration in physical node.
pub struct Config {
    pub node_id: u64,
    pub election_tick: usize,
    pub heartbeat_tick: usize,
    pub tick_interval: u64, // ms

    /// Batchs apply msg if not equal `1`. It provides msg buf for
    /// batch apply, default is `1`.
    ///
    /// # Panics
    /// If the value is `0`.
    pub max_batch_apply_msgs: usize,

    /// Whenever `ReplicaDesc` is cached, if true will be written to disk
    /// at the same time (fsync).
    pub replica_sync: bool,

    /// Limit the max size of each append message. Smaller value lowers
    /// the raft recovery cost(initial probing and message lost during normal operation).
    /// On the other side, it might affect the throughput during normal replication.
    /// Note: math.MaxUusize64 for unlimited, 0 for at most one entry per message.
    pub max_size_per_msg: u64,

    /// Limit the max number of in-flight append messages during optimistic
    /// replication phase. The application transportation layer usually has its own sending
    /// buffer over TCP/UDP. Set to avoid overflowing that sending buffer.
    /// TODO: feedback to application to limit the proposal rate?
    pub max_inflight_msgs: usize,

    /// Batches every append msg if any append msg already exists
    pub batch_append: bool,

    pub batch_apply: bool,

    pub batch_size: usize,

    pub event_capacity: usize,

    /// The size of the FIFO queue for write requests, default is `1`.
    ///
    /// > Note: Consensus groups handles write proposals sequentially.
    /// > the write proposal queue are used to concurrently write to multiple consensus groups.
    /// > The request queue is shared among all groups on the node, which means
    /// that the value is set based on the number of consensus groups on the node.
    pub proposal_queue_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            node_id: 0,
            event_capacity: 1,
            election_tick: HEARTBEAT_TICK * 10,
            heartbeat_tick: HEARTBEAT_TICK,
            tick_interval: 10,
            max_batch_apply_msgs: 1,
            max_size_per_msg: 1024 * 1024,
            max_inflight_msgs: 256,
            batch_append: false,
            batch_apply: false,
            batch_size: 0,
            replica_sync: true,
            proposal_queue_size: 1,
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<(), Error> {
        if self.node_id == INVALID_NODE_ID {
            return Err(Error::ConfigInvalid("invalid node id".to_owned()));
        }

        if self.heartbeat_tick == 0 {
            return Err(Error::ConfigInvalid(
                "heartbeat tick must greater than 0".to_owned(),
            ));
        }

        if self.election_tick <= self.heartbeat_tick {
            return Err(Error::ConfigInvalid(
                "election tick must be greater than heartbeat tick".to_owned(),
            ));
        }

        if self.tick_interval == 0 {
            return Err(Error::ConfigInvalid(
                "tick interval must be greater than 0".to_owned(),
            ));
        }

        if self.max_batch_apply_msgs == 0 {
            return Err(Error::ConfigInvalid(
                "max batch apply msgs must be greater than 0".to_owned(),
            ));
        }

        if self.max_inflight_msgs == 0 {
            return Err(Error::ConfigInvalid(
                "max inflight messages must be greater than 0".to_owned(),
            ));
        }

        if self.proposal_queue_size == 0 {
            return Err(Error::ConfigInvalid(
                "write queue size must be greater than 0".to_owned(),
            ));
        }

        Ok(())
    }
}
