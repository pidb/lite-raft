use std::collections::HashMap;
use std::mem::take;
use std::sync::Arc;

use tokio::sync::mpsc::Receiver;

use oceanraft::tick::ManualTick;
use oceanraft::transport::LocalTransport;
use oceanraft::Apply;
use oceanraft::Config;
use oceanraft::MultiRaft;
use oceanraft::MultiRaftTypeSpecialization;

use super::Cluster;

pub struct ClusterBuilder<T>
where
    T: MultiRaftTypeSpecialization,
{
    node_size: usize,
    election_ticks: usize,
    storages: Vec<T::MS>,
    apply_rxs: Vec<Option<Receiver<Vec<Apply<T::D, T::R>>>>>,
    state_machines: Vec<Option<T::M>>,
}

impl<T> ClusterBuilder<T>
where
    T: MultiRaftTypeSpecialization,
{
    pub fn new(nodes: usize) -> Self {
        Self {
            node_size: nodes,
            election_ticks: 0,
            storages: Vec::new(),
            state_machines: Vec::new(),
            apply_rxs: Vec::new(),
        }
    }

    pub fn storages(mut self, storages: Vec<T::MS>) -> Self {
        assert_eq!(
            storages.len(),
            self.node_size,
            "expect node {}, got nums {} of state machines",
            self.node_size,
            storages.len(),
        );

        self.storages = storages;
        self
    }

    pub fn apply_rxs(mut self, rxs: Vec<Option<Receiver<Vec<Apply<T::D, T::R>>>>>) -> Self {
        assert_eq!(
            rxs.len(),
            self.node_size,
            "expect node {}, got nums {} of state machines",
            self.node_size,
            rxs.len(),
        );

        self.apply_rxs = rxs;
        self
    }

    pub fn state_machines(mut self, state_machines: Vec<T::M>) -> Self {
        assert_eq!(
            state_machines.len(),
            self.node_size,
            "expect node {}, got nums {} of kv stores",
            self.node_size,
            state_machines.len(),
        );

        self.state_machines = state_machines.into_iter().map(|sm| Some(sm)).collect();
        self
    }

    pub fn election_ticks(mut self, election_ticks: usize) -> Self {
        self.election_ticks = election_ticks;
        self
    }

    pub async fn build(mut self) -> Cluster<T> {
        assert_eq!(
            self.storages.len(),
            self.node_size,
            "expect node {}, got nums {} of state machines",
            self.node_size,
            self.storages.len(),
        );

        assert_eq!(
            self.apply_rxs.len(),
            self.node_size,
            "expect node {}, got nums {} of state machines",
            self.node_size,
            self.apply_rxs.len(),
        );

        assert_eq!(
            self.state_machines.len(),
            self.node_size,
            "expect node {}, got nums {} of kv stores",
            self.node_size,
            self.state_machines.len(),
        );

        let mut nodes = vec![];
        let mut tickers = vec![];
        // let mut apply_events = vec![];

        let transport = LocalTransport::new();
        for i in 0..self.node_size {
            let node_id = (i + 1) as u64;
            let config = Config {
                node_id,
                batch_append: false,
                election_tick: 2,
                event_capacity: 100,
                heartbeat_tick: 1,
                max_size_per_msg: 0,
                max_inflight_msgs: 256,
                tick_interval: 10, // hour ms
                max_batch_apply_msgs: 1,
                batch_apply: false,
                batch_size: 0,
                proposal_queue_size: 1000,
                replica_sync: true,
            };
            let ticker = ManualTick::new();
            let node = MultiRaft::new(
                config,
                transport.clone(),
                self.storages[i].clone(),
                self.state_machines[i]
                    .take()
                    .expect("state machines can't initialize"),
                // &event_tx,
                Some(ticker.clone()),
            )
            .unwrap();

            transport
                .listen(
                    node_id,
                    format!("test://node/{}", node_id).as_str(),
                    node.message_sender(),
                )
                .await
                .unwrap();

            nodes.push(Arc::new(node));
            // apply_events.push(Some(apply_event_rx));

            tickers.push(ticker.clone());
        }
        Cluster {
            storages: self.storages,
            apply_events: take(&mut self.apply_rxs),
            nodes,
            transport,
            tickers,
            election_ticks: self.election_ticks,
            groups: HashMap::new(),
        }
    }
}
