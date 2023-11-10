use std::collections::HashMap;
use std::env::temp_dir;

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use oceanraft::prelude::CreateGroupRequest;

use oceanraft::MultiRaftTypeSpecialization;

use rand::distributions::Alphanumeric;
use rand::Rng;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use tokio::time::timeout_at;
use tokio::time::Instant;

use oceanraft::prelude::ConfState;
use oceanraft::prelude::ReplicaDesc;
use oceanraft::prelude::Snapshot;
use oceanraft::storage::MultiRaftStorage;
use oceanraft::storage::StorageExt;
use oceanraft::tick::ManualTick;
use oceanraft::transport::LocalTransport;
use oceanraft::Apply;
use oceanraft::ApplyMembership;
use oceanraft::ApplyNormal;
use oceanraft::Error;
use oceanraft::Event;
use oceanraft::LeaderElectionEvent;
use oceanraft::MultiRaft;
use oceanraft::MultiRaftMessageSenderImpl;

use super::port::StateMachineEvent;

/// Generates a random string of n size
pub fn rand_string(n: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(n)
        .map(char::from)
        .collect()
}

/// Create a temporary `PathBuf` with a prefix identifier
pub fn rand_temp_dir<P>(postfix: P) -> PathBuf
where
    P: AsRef<Path>,
{
    let rand_str: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(8)
        .map(char::from)
        .collect();
    temp_dir().join(rand_str).join(postfix)
}

pub struct Cluster<T>
where
    T: MultiRaftTypeSpecialization,
{
    pub election_ticks: usize,
    pub nodes: Vec<Arc<MultiRaft<T, LocalTransport<MultiRaftMessageSenderImpl>>>>,
    pub apply_events: Vec<Option<Receiver<Vec<Apply<T::D, T::R>>>>>,
    pub events: Vec<Option<Receiver<StateMachineEvent<T::D, T::R>>>>,
    pub transport: LocalTransport<MultiRaftMessageSenderImpl>,
    pub tickers: Vec<ManualTick>,
    pub groups: HashMap<u64, Vec<u64>>, // track group which nodes, group_id -> nodes
    pub storages: Vec<T::MS>,
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

impl<T> Cluster<T>
where
    T: MultiRaftTypeSpecialization,
{
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
                group_id: plan.group_id,
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

            gs.install_snapshot(ss).unwrap();

            let node = &self.nodes[place_node_index];

            // create admin message for create raft grop
            // let _ = node
            //     .create_group(plan.group_id, replica_id, Some(replicas.clone()))
            //     .await?;

            let _ = node
                .create_group(CreateGroupRequest {
                    group_id: plan.group_id,
                    replica_id,
                    replicas: replicas.clone(),
                    applied_hint: 0,
                })
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
        // cluster: &mut FixtureCluster<R, S, MS>,
        &mut self,
        node_id: u64,
        // rx: &mut Option<Receiver<Vec<Event>>>,
    ) -> Result<LeaderElectionEvent, String> {
        // let rx = cluster.mut_event_rx(node_id);
        let rx = self.nodes[to_index(node_id)].subscribe();

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
        match timeout_at(Instant::now() + Duration::from_millis(1000), wait_loop_fut).await {
            Err(_) => Err(format!("wait for leader elect event timeouted")),
            Ok(res) => res,
        }
    }

    pub async fn wait_for_commands_apply(
        &mut self,
        node_id: u64,
        wait_size: usize,
        timeout: Duration,
    ) -> Result<Vec<ApplyNormal<T::D, T::R>>, String> {
        let rx = self.apply_events[to_index(node_id)].as_mut().unwrap();
        let mut results_len = 0;
        let wait_loop_fut = async {
            let mut results = vec![];
            loop {
                results_len = results.len();
                if results.len() == wait_size {
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

    /// Wait elected.
    pub async fn wait_membership_change_apply_event(
        cluster: &mut Cluster<T>,
        node_id: u64,
        timeout: Duration,
    ) -> Result<ApplyMembership<T::R>, String> {
        let rx = cluster.apply_events[to_index(node_id)].as_mut().unwrap();
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
        write_data: T::D,
    ) -> Result<oneshot::Receiver<Result<(T::R, Option<Vec<u8>>), Error>>, Error> {
        self.nodes[to_index(node_id)].write_non_block(group_id, 0, None, write_data)
    }

    // Wait normal apply.

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

#[inline]
fn to_index(node_id: u64) -> usize {
    node_id as usize - 1
}
