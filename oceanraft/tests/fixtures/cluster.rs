use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use oceanraft::multiraft::ApplyNormal;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use tokio::time::timeout_at;
use tokio::time::Instant;

use oceanraft::memstore::MultiRaftMemoryStorage;
use oceanraft::multiraft::storage::MultiRaftStorage;
use oceanraft::multiraft::Apply;
use oceanraft::multiraft::ApplyMembership;
use oceanraft::multiraft::Config;
use oceanraft::multiraft::Error;
use oceanraft::multiraft::Event;
use oceanraft::multiraft::LeaderElectionEvent;
use oceanraft::multiraft::ManualTick;
use oceanraft::multiraft::MultiRaftMessageSenderImpl;
use oceanraft::prelude::ConfState;
use oceanraft::prelude::MultiRaft;
use oceanraft::prelude::ReplicaDesc;
use oceanraft::prelude::Snapshot;
use oceanraft::util::TaskGroup;

use oceanraft::multiraft::transport::LocalTransport;

// use crate::fixtures::transport::LocalTransport;

use super::rsm::FixtureStateMachine;

// type FixtureMultiRaft = MultiRaft<
//     LocalTransport<MultiRaftMessageSenderImpl>,
//     RaftMemStorage,
//     MultiRaftMemoryStorage,
//     FixtureStateMachine,
//     (),
// >;

pub struct FixtureCluster {
    pub election_ticks: usize,
    pub nodes: Vec<Arc<MultiRaft<()>>>,
    pub apply_events: Vec<Option<Receiver<Vec<Apply<()>>>>>,
    pub transport: LocalTransport<MultiRaftMessageSenderImpl>,
    pub tickers: Vec<ManualTick>,
    pub groups: HashMap<u64, Vec<u64>>, // track group which nodes, group_id -> nodes
    pub storages: Vec<MultiRaftMemoryStorage>,
}

#[derive(Default)]
pub struct MakeGroupPlan {
    pub group_id: u64,
    pub first_node_id: u64,
    pub replica_nums: usize,
}

#[derive(Default)]
pub struct MakeGroupPlanStatus {
    group_id: u64,
    first_node_id: u64,
    replica_nums: usize,
    replicas: Vec<ReplicaDesc>,
    conf_states: Vec<ConfState>,
}

impl std::fmt::Display for MakeGroupPlanStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "plan status\n    group_id: {}\n", self.group_id)?;
        for i in 0..self.replica_nums {
            write!(
                f,
                "       node {}: {:?}, {:?}\n",
                self.first_node_id as usize + i,
                self.replicas[i],
                self.conf_states[i]
            )?
        }

        write!(f, "")
    }
}

impl FixtureCluster {
    /// Make a `FixtureCluster` with `n` nodes and nodes ids starting at 1.
    pub async fn make(n: u64, task_group: TaskGroup) -> FixtureCluster {
        let mut nodes = vec![];
        let mut storages = vec![];
        let mut tickers = vec![];

        let mut apply_events = vec![];

        let transport = LocalTransport::new();
        for i in 0..n {
            let node_id = i + 1;
            let config = Config {
                node_id,
                batch_append: false,
                election_tick: 2,
                event_capacity: 100,
                heartbeat_tick: 1,
                max_size_per_msg: 0,
                max_inflight_msgs: 256,
                tick_interval: 3_600_000, // hour ms
                batch_apply: false,
                batch_size: 0,
                proposal_queue_size: 1000,
                replica_sync: true,
            };
            let ticker = ManualTick::new();
            // let (event_tx, event_rx) = channel(1);

            let (apply_event_tx, apply_event_rx) = channel(100);

            let rsm = FixtureStateMachine::new(apply_event_tx);
            let storage = MultiRaftMemoryStorage::new(config.node_id);
            let node = MultiRaft::new(
                config,
                transport.clone(),
                storage.clone(),
                rsm,
                task_group.clone(),
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
            // events.push(Some(event_rx));
            apply_events.push(Some(apply_event_rx));
            storages.push(storage);
            tickers.push(ticker.clone());
        }
        Self {
            // events,
            apply_events,
            storages,
            nodes,
            transport,
            tickers,
            election_ticks: 2,
            groups: HashMap::new(),
        }
    }

    /// Provide a plan to create a consensus group with a number of `replica_nums` starting from `first_node_id`.
    ///
    /// the plan files explains follows:
    /// - `group_id` is self-explanatory.
    /// - `first_node_id` indicates the start node where the replica is placed in the consensus group.
    /// The current constraints on `fist_node_id` are that it cannot be `0` and must be less than the
    /// number of `FixtureCluster` nodes.
    /// - replica_nums specifies the number of consensus groups. The replica_nums limit the number
    /// of nodes to `FixtureCluster` nodes.
    pub async fn make_group(&mut self, plan: &MakeGroupPlan) -> Result<MakeGroupPlanStatus, Error> {
        assert!(
            plan.first_node_id != 0 && plan.first_node_id - 1 < self.nodes.len() as u64,
            "first_node_id violates the current constraint"
        );

        assert!(
            plan.replica_nums != 0
                && plan.replica_nums <= (self.nodes.len() - (plan.first_node_id as usize - 1)),
            "replica_nums violates the current constraint"
        );

        let mut status = MakeGroupPlanStatus {
            group_id: plan.group_id,
            first_node_id: plan.first_node_id,
            replica_nums: plan.replica_nums,
            ..Default::default()
        };

        let mut voters = vec![];
        let mut replicas = vec![];

        // create replica descs
        for i in 0..plan.replica_nums {
            let replica_id = (i + 1) as u64;
            let node_id = (plan.first_node_id - 1) + (i + 1) as u64;
            voters.push(replica_id);
            replicas.push(ReplicaDesc {
                node_id,
                replica_id,
            });
        }

        status.replicas = replicas.clone();

        for i in 0..plan.replica_nums {
            let place_node_index = (plan.first_node_id - 1) as usize + i;
            let place_node_id = plan.first_node_id + i as u64;
            let replica_id = (i + 1) as u64;
            let storage = &self.storages[place_node_index];
            let gs = storage.group_storage(plan.group_id, replica_id).await?;
            // apply snapshot should mutable raft state (conf_state and hard_state)
            let mut ss = Snapshot::default();
            ss.mut_metadata().mut_conf_state().voters = voters.clone();
            ss.mut_metadata().index = 1;
            ss.mut_metadata().term = 1;

            // record cc
            status
                .conf_states
                .push(ss.get_metadata().get_conf_state().clone());

            // apply cc
            gs.wl().apply_snapshot(ss).unwrap();

            let node = &self.nodes[place_node_index];

            // create admin message for create raft grop
            let _ = node
                .create_group(plan.group_id, replica_id, Some(replicas.clone()))
                .await?;

            match self.groups.get_mut(&plan.group_id) {
                None => {
                    self.groups
                        .insert(plan.group_id, vec![place_node_id as u64]);
                }
                Some(nodes) => nodes.push(place_node_id as u64),
            };
        }

        Ok(status)
    }

    /// Start all nodes of the `FixtureCluster`.
    // pub fn start(&self) {
    //     for (i, node) in self.nodes.iter().enumerate() {
    //         node.start(Some(self.tickers[i].clone()));
    //     }
    // }

    /// Get mutable event receiver by given `node_id`.
    // pub fn mut_event_rx(&mut self, node_id: u64) -> &mut Receiver<Vec<Event>> {
    //     self.events[to_index(node_id)].as_mut().unwrap()
    // }

    pub fn mut_apply_event_rx(&mut self, node_id: u64) -> &mut Receiver<Vec<Apply<()>>> {
        self.apply_events[to_index(node_id)].as_mut().unwrap()
    }

    /// Remove event rx from cluster events.
    // pub fn take_event_rx(&mut self, index: usize) -> Receiver<Vec<Event>> {
    //     std::mem::take(&mut self.events[index]).unwrap()
    // }

    /// Gets a `ReplicaDesc` of the consensus group on the node by given `node_id` and `group_id`.
    pub async fn replica_desc(&self, node_id: u64, group_id: u64) -> ReplicaDesc {
        let storage = &self.storages[to_index(node_id)];
        storage
            .replica_for_node(group_id, node_id)
            .await
            .unwrap()
            .unwrap()
    }

    /// Campaigns the consensus group by the given `node_id` and `group_id`.
    ///
    /// This method is used, for example, to trigger an election.
    pub async fn campaign_group(&mut self, node_id: u64, group_id: u64) {
        self.nodes[to_index(node_id)]
            .campaign_group(group_id)
            .await
            .unwrap();
    }

    /// Wait elected.
    pub async fn wait_leader_elect_event(
        cluster: &mut FixtureCluster,
        node_id: u64,
        // rx: &mut Option<Receiver<Vec<Event>>>,
    ) -> Result<LeaderElectionEvent, String> {
        // let rx = cluster.mut_event_rx(node_id);
        let mut rx = cluster.nodes[to_index(node_id)].subscribe();

        let wait_loop_fut = async {
            loop {
                let event = match rx.recv().await {
                    Err(err) => return Err(err.to_string()), // TODO: handle lagged
                    Ok(event) => event,
                };

                // for event in events {
                match event {
                    Event::LederElection(leader_elect) => return Ok(leader_elect),
                    _ => {}
                }
                // }
            }
        };
        match timeout_at(Instant::now() + Duration::from_millis(100), wait_loop_fut).await {
            Err(_) => Err(format!("wait for leader elect event timeouted")),
            Ok(res) => res,
        }
    }

    /// Wait elected.
    pub async fn wait_membership_change_apply_event(
        cluster: &mut FixtureCluster,
        node_id: u64,
        timeout: Duration,
    ) -> Result<ApplyMembership<()>, String> {
        let rx = cluster.mut_apply_event_rx(node_id);
        let wait_loop_fut = async {
            loop {
                let events = match rx.recv().await {
                    None => return Err(String::from("the event sender dropped")),
                    Some(evs) => evs,
                };

                for event in events {
                    match event {
                        Apply::Membership(membership) => return Ok(membership),
                        _ => {}
                    }
                }
            }
        };
        match timeout_at(Instant::now() + timeout, wait_loop_fut).await {
            Err(_) => Err(format!("wait for apply change event timeouted")),
            Ok(res) => res,
        }
    }

    /// Write data to raft. return a onshot::Receiver to recv apply result.
    pub fn write_command(
        &self,
        node_id: u64,
        group_id: u64,
        data: Vec<u8>,
    ) -> oneshot::Receiver<Result<(), Error>> {
        self.nodes[to_index(node_id)]
            .write_non_block(group_id, 0, data, None)
            .unwrap()
    }

    // Wait normal apply.
    pub async fn wait_for_command_apply(
        rx: &mut Receiver<Vec<Apply<()>>>,
        timeout: Duration,
        size: usize,
    ) -> Result<Vec<ApplyNormal<()>>, String> {
        let mut results_len = 0;
        let wait_loop_fut = async {
            let mut results = vec![];
            loop {
                results_len = results.len();
                if results.len() == size {
                    return Ok(results);
                }
                let events = match rx.recv().await {
                    None => return Err(String::from("the event sender dropped")),
                    Some(evs) => evs,
                };

                // check all events type should apply
                for event in events {
                    match event {
                        Apply::Normal(data) => results.push(data),
                        // Event::ApplyNormal(event) => results.push(event),
                        _ => {}
                    }
                }
            }
        };
        match timeout_at(Instant::now() + timeout, wait_loop_fut).await {
            Err(_) => Err(format!(
                "wait for apply normal event timeouted, waited nums = {}",
                results_len
            )),
            Ok(res) => res,
        }
    }

    /// Tick node by given `node_id`. if `delay` is some that each tick will delay.
    pub async fn tick_node(&mut self, node_id: u64, delay: Option<Duration>) {
        self.tickers[to_index(node_id)].tick().await;
        if let Some(delay) = delay {
            tokio::time::sleep(delay).await
        }
    }

    pub async fn stop(&mut self) {
        for node in std::mem::take(&mut self.nodes).into_iter() {
            node.stop().await
        }
    }
}

/// Multiple consensus groups are quickly started. Node and consensus group ids start from 1.
/// All consensus group replicas equal to 1 are elected as the leader.
pub async fn quickstart_multi_groups(
    node_nums: usize,
    group_nums: usize,
) -> (TaskGroup, FixtureCluster) {
    // FIXME: each node has task group, if not that joinner can block.
    let task_group = TaskGroup::new();
    let mut cluster = FixtureCluster::make(node_nums as u64, task_group.clone()).await;
    // cluster.start();

    // create multi groups
    for i in 0..group_nums {
        let group_id = (i + 1) as u64;
        let plan = MakeGroupPlan {
            group_id,
            first_node_id: 1,
            replica_nums: 3,
        };
        let _ = cluster.make_group(&plan).await.unwrap();
        cluster.campaign_group(1, plan.group_id).await;

        for j in 0..3 {
            let leader_event = FixtureCluster::wait_leader_elect_event(&mut cluster, j + 1)
                .await
                .unwrap();
            assert_eq!(
                (1..group_nums as u64 + 1).contains(&leader_event.group_id),
                true,
                "expected group_id in {:?}, got {}",
                (1..group_nums + 1),
                leader_event.group_id,
            );
            assert_eq!(leader_event.replica_id, 1);
        }
    }

    (task_group, cluster)
}

/// Quickly start a consensus group with 3 nodes and 3 replicas, with leader being replica 1.
pub async fn quickstart_group(node_nums: usize) -> (TaskGroup, FixtureCluster) {
    // FIXME: each node has task group, if not that joinner can block.
    let task_group = TaskGroup::new();
    let mut cluster = FixtureCluster::make(node_nums as u64, task_group.clone()).await;
    // cluster.start();

    // create multi groups
    let group_id = 1;
    let plan = MakeGroupPlan {
        group_id,
        first_node_id: 1,
        replica_nums: 3,
    };
    let _ = cluster.make_group(&plan).await.unwrap();
    cluster.campaign_group(1, plan.group_id).await;

    // check each replica should recv leader election event
    for i in 0..node_nums {
        let leader_event = FixtureCluster::wait_leader_elect_event(&mut cluster, i as u64 + 1)
            .await
            .unwrap();
        assert_eq!(leader_event.group_id, 1);
        assert_eq!(leader_event.replica_id, 1);
    }

    (task_group, cluster)
}

#[inline]
fn to_index(node_id: u64) -> usize {
    node_id as usize - 1
}

impl FixtureCluster {}

// pub async fn wait_for_command_apply<P>(
//     rx: &mut Receiver<Vec<Event>>,
//     mut predicate: P,
//     timeout: Duration,
// ) where
//     P: FnMut(ApplyNormalEvent) -> Result<bool, String>,
// {
//     let fut = async {
//         loop {
//             let events = match rx.recv().await {
//                 None => panic!("sender dropped"),
//                 Some(events) => events,
//             };

//             for event in events.into_iter() {
//                 match event {
//                     Event::ApplyNormal(apply_event) => match predicate(apply_event) {
//                         Err(err) => panic!("{}", err),
//                         Ok(matched) => {
//                             if !matched {
//                                 continue;
//                             } else {
//                                 return;
//                             }
//                         }
//                     },
//                     _ => continue,
//                 }
//             }
//         }
//     };

//     match timeout_at(Instant::now() + timeout, fut).await {
//         Err(_) => {
//             panic!("timeout");
//         }
//         Ok(_) => {}
//     };
// }

// pub async fn wait_for_membership_change_apply<P, F>(
//     rx: &mut Receiver<Vec<Event>>,
//     mut predicate: P,
//     timeout: Duration,
// ) where
//     P: FnMut(ApplyMembershipChangeEvent) -> F,
//     F: Future<Output = Result<bool, String>>,
// {
//     let fut = async {
//         loop {
//             let events = match rx.recv().await {
//                 None => panic!("sender dropped"),
//                 Some(events) => events,
//             };

//             for event in events.into_iter() {
//                 match event {
//                     Event::ApplyMembershipChange(event) => match predicate(event).await {
//                         Err(err) => panic!("{}", err),
//                         Ok(matched) => {
//                             if !matched {
//                                 continue;
//                             } else {
//                                 return;
//                             }
//                         }
//                     },
//                     _ => continue,
//                 }
//             }
//         }
//     };

//     match timeout_at(Instant::now() + timeout, fut).await {
//         Err(_) => {
//             panic!("wait membership change timeout");
//         }
//         Ok(_) => {}
//     };
// }
