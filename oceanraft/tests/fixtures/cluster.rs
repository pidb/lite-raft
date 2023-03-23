use std::collections::HashMap;
use std::env::temp_dir;
use std::fs::remove_dir_all;
use std::marker::PhantomData;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use oceanraft::multiraft::storage::KVStore;
use oceanraft::multiraft::storage::RockStoreCore;
use oceanraft::prelude::StoreData;
use rand::distributions::Alphanumeric;
use rand::Rng;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot;
use tokio::time::timeout_at;
use tokio::time::Instant;

use oceanraft::multiraft::storage::MultiRaftStorage;
use oceanraft::multiraft::storage::RaftStorage;
use oceanraft::multiraft::storage::RockStore;
use oceanraft::multiraft::Apply;
use oceanraft::multiraft::ApplyMembership;
use oceanraft::multiraft::ApplyNormal;
use oceanraft::multiraft::Config;
use oceanraft::multiraft::Error;
use oceanraft::multiraft::Event;
use oceanraft::multiraft::LeaderElectionEvent;
use oceanraft::multiraft::ManualTick;
use oceanraft::multiraft::MultiRaftMessageSenderImpl;
use oceanraft::multiraft::WriteResponse;
use oceanraft::prelude::ConfState;
use oceanraft::prelude::MultiRaft;
use oceanraft::prelude::ReplicaDesc;
use oceanraft::prelude::Snapshot;
use oceanraft::util::TaskGroup;
use oceanraft::multiraft::transport::LocalTransport;

use super::rsm::FixtureStateMachine;

#[allow(unused)]
pub fn rand_string(n: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(n)
        .map(char::from)
        .collect()
}

#[allow(unused)]
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

pub fn new_rock_kv_stores<P, R>(nodes: usize, paths: Vec<P>) -> Vec<KVStore<R>>
where
    P: AsRef<Path>,
    R: WriteResponse,
{
    (0..nodes)
        .zip(paths)
        .into_iter()
        .map(|(i, p)| {
            println!("🏠 create statemachine storeage {}", p.as_ref().display());
            let node_id = (i + 1) as u64;
            KVStore::<R>::new(node_id, p)
        })
        .collect()
}

pub fn new_rocks_storeages<P, R>(
    nodes: usize,
    paths: Vec<P>,
    kv_stores: Vec<KVStore<R>>,
) -> Vec<RockStore<KVStore<R>, KVStore<R>>>
where
    P: AsRef<Path>,
    R: WriteResponse,
{
    (0..nodes)
        .zip(paths)
        .zip(kv_stores)
        .into_iter()
        .map(|((i, p), state_machine)| {
            println!("🚪 create rock storeage {}", p.as_ref().display());
            let node_id = (i + 1) as u64;
            RockStore::new(node_id, p, state_machine.clone(), state_machine.clone())
        })
        .collect()
}

/// Provides a rocksdb storage and state machine environment for cluster.
pub struct RockStorageEnv<R: WriteResponse> {
    pub storages: Vec<RockStore<KVStore<R>, KVStore<R>>>,
    pub rock_kv_stores: Vec<KVStore<R>>,
    pub storage_paths: Vec<PathBuf>,
    pub state_machine_paths: Vec<PathBuf>,
}

impl<R: WriteResponse> RockStorageEnv<R> {
    pub fn new(nodes: usize) -> Self {
        let state_machine_paths = (0..nodes)
            .map(|i| rand_temp_dir(format!("state_machine_node_{}", i)))
            .collect::<Vec<_>>();

        let storage_paths = (0..nodes)
            .map(|i| rand_temp_dir(format!("store_db_node_{}", i)))
            .collect::<Vec<_>>();

        let rock_kv_stores = new_rock_kv_stores::<_, R>(nodes, state_machine_paths.clone());
        let storages =
            new_rocks_storeages::<_, R>(nodes, storage_paths.clone(), rock_kv_stores.clone());

        Self {
            storages,
            storage_paths,
            rock_kv_stores,
            state_machine_paths,
        }
    }

    pub fn destory(mut self) {
        self.state_machine_paths.extend(self.storage_paths);
        for p in self.state_machine_paths.iter() {
            println!("🌪 remove dir {}", p.display());
            remove_dir_all(p).unwrap();
        }
    }
}

pub struct ClusterBuilder<S, MS, R>
where
    S: RaftStorage,
    MS: MultiRaftStorage<S>,
    R: WriteResponse,
    // RSM: StateMachine<W, R>,
{
    node_size: usize,
    election_ticks: usize,
    tg: Option<TaskGroup>,
    storages: Vec<MS>,
    kv_stores: Vec<KVStore<R>>,
    _m1: PhantomData<S>,
    _m2: PhantomData<R>,
}

impl<S, MS, R> ClusterBuilder<S, MS, R>
where
    S: RaftStorage,
    MS: MultiRaftStorage<S>,
    R: WriteResponse,
{
    pub fn new(nodes: usize) -> Self {
        Self {
            node_size: nodes,
            election_ticks: 0,
            tg: None,
            storages: Vec::new(),
            kv_stores: Vec::new(),
            _m1: PhantomData,
            _m2: PhantomData,
        }
    }

    pub fn task_group(mut self, tg: TaskGroup) -> Self {
        self.tg = Some(tg);
        self
    }

    pub fn storages(mut self, storages: Vec<MS>) -> Self {
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

    pub fn kv_stores(mut self, kv_stores: Vec<KVStore<R>>) -> Self {
        assert_eq!(
            kv_stores.len(),
            self.node_size,
            "expect node {}, got nums {} of kv stores",
            self.node_size,
            kv_stores.len(),
        );

        self.kv_stores = kv_stores;
        self
    }

    pub fn election_ticks(mut self, election_ticks: usize) -> Self {
        self.election_ticks = election_ticks;
        self
    }

    pub async fn build(self) -> FixtureCluster<R, S, MS> {
        let mut nodes = vec![];
        let mut tickers = vec![];
        let mut apply_events = vec![];

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
                tick_interval: 3_600_000, // hour ms
                batch_apply: false,
                batch_size: 0,
                proposal_queue_size: 1000,
                replica_sync: true,
            };
            let ticker = ManualTick::new();
            // let (event_tx, event_rx) = channel(1);

            let (apply_event_tx, apply_event_rx) = channel(100);
            let state_machine = FixtureStateMachine::new(self.kv_stores[i].clone(), apply_event_tx);
            // let rsm = FixtureStateMachine::new(apply_event_tx);
            // let storage = MultiRaftMemoryStorage::new(config.node_id);
            let node = MultiRaft::new(
                config,
                transport.clone(),
                self.storages[i].clone(),
                state_machine,
                self.tg.as_ref().unwrap().clone(),
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
            apply_events.push(Some(apply_event_rx));

            tickers.push(ticker.clone());
        }
        FixtureCluster {
            storages: self.storages,
            // state_machines,
            apply_events,
            nodes,
            transport,
            tickers,
            election_ticks: self.election_ticks,
            groups: HashMap::new(),
            _m1: PhantomData,
        }
    }
}

pub struct FixtureCluster<R, S, MS>
where
    R: WriteResponse,
    S: RaftStorage,
    MS: MultiRaftStorage<S>,
{
    pub election_ticks: usize,
    pub nodes: Vec<Arc<MultiRaft<StoreData, R>>>,
    pub apply_events: Vec<Option<Receiver<Vec<Apply<StoreData, R>>>>>,
    pub transport: LocalTransport<MultiRaftMessageSenderImpl>,
    pub tickers: Vec<ManualTick>,
    pub groups: HashMap<u64, Vec<u64>>, // track group which nodes, group_id -> nodes
    pub storages: Vec<MS>,
    _m1: PhantomData<S>,
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

pub type RockCluster<R> =
    FixtureCluster<R, RockStoreCore<KVStore<()>, KVStore<()>>, RockStore<KVStore<()>, KVStore<()>>>;

impl<R, S, MS> FixtureCluster<R, S, MS>
where
    R: WriteResponse,
    S: RaftStorage,
    MS: MultiRaftStorage<S>,
{
    /// Make a `FixtureCluster` with `n` nodes and nodes ids starting at 1.
    // pub async fn make(n: u64, task_group: TaskGroup) -> Self {
    //     let mut nodes = vec![];
    //     let mut storages = vec![];
    //     let mut tickers = vec![];

    //     let mut apply_events = vec![];

    //     let transport = LocalTransport::new();
    //     for i in 0..n {
    //         let node_id = i + 1;
    //         let config = Config {
    //             node_id,
    //             batch_append: false,
    //             election_tick: 2,
    //             event_capacity: 100,
    //             heartbeat_tick: 1,
    //             max_size_per_msg: 0,
    //             max_inflight_msgs: 256,
    //             tick_interval: 3_600_000, // hour ms
    //             batch_apply: false,
    //             batch_size: 0,
    //             proposal_queue_size: 1000,
    //             replica_sync: true,
    //         };
    //         let ticker = ManualTick::new();
    //         // let (event_tx, event_rx) = channel(1);

    //         let (apply_event_tx, apply_event_rx) = channel(100);

    //         let rsm = FixtureStateMachine::new(apply_event_tx);
    //         let storage = MultiRaftMemoryStorage::new(config.node_id);
    //         let node = MultiRaft::new(
    //             config,
    //             transport.clone(),
    //             storage.clone(),
    //             rsm,
    //             task_group.clone(),
    //             // &event_tx,
    //             Some(ticker.clone()),
    //         )
    //         .unwrap();

    //         transport
    //             .listen(
    //                 node_id,
    //                 format!("test://node/{}", node_id).as_str(),
    //                 node.message_sender(),
    //             )
    //             .await
    //             .unwrap();

    //         nodes.push(Arc::new(node));
    //         // events.push(Some(event_rx));
    //         apply_events.push(Some(apply_event_rx));
    //         storages.push(storage);
    //         tickers.push(ticker.clone());
    //     }
    //     Self {
    //         // events,
    //         apply_events,
    //         storages,
    //         nodes,
    //         transport,
    //         tickers,
    //         election_ticks: 2,
    //         groups: HashMap::new(),
    //     }
    // }

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

    pub fn mut_apply_event_rx(&mut self, node_id: u64) -> &mut Receiver<Vec<Apply<StoreData, R>>> {
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
        match timeout_at(Instant::now() + Duration::from_millis(100), wait_loop_fut).await {
            Err(_) => Err(format!("wait for leader elect event timeouted")),
            Ok(res) => res,
        }
    }

    pub async fn wait_for_commands_apply(
        &mut self,
        node_id: u64,
        wait_size: usize,
        timeout: Duration,
    ) -> Result<Vec<ApplyNormal<StoreData, R>>, String> {
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
        cluster: &mut FixtureCluster<R, S, MS>,
        node_id: u64,
        timeout: Duration,
    ) -> Result<ApplyMembership<R>, String> {
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
        write_data: StoreData,
    ) -> Result<oneshot::Receiver<Result<R, Error>>, Error> {
        self.nodes[to_index(node_id)].write_non_block(group_id, 0, write_data, None)
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

/// Multiple consensus groups are quickly started. Node and consensus group ids start from 1.
/// All consensus group replicas equal to 1 are elected as the leader.
pub async fn quickstart_multi_groups(
    rockstore_env: &RockStorageEnv<()>,
    nodes: usize,
    groups: usize,
) -> (
    TaskGroup,
    FixtureCluster<
        (),
        RockStoreCore<KVStore<()>, KVStore<()>>,
        RockStore<KVStore<()>, KVStore<()>>,
    >,
) {
    // FIXME: each node has task group, if not that joinner can block.
    let task_group = TaskGroup::new();
    // let mut cluster = FixtureCluster::make(node_nums as u64, task_group.clone()).await;
    // cluster.start();

    let mut cluster = ClusterBuilder::new(nodes)
        .election_ticks(2)
        .kv_stores(rockstore_env.rock_kv_stores.clone())
        .storages(rockstore_env.storages.clone())
        .task_group(task_group.clone())
        .build()
        .await;
    // create multi groups
    for i in 0..groups {
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
                (1..groups as u64 + 1).contains(&leader_event.group_id),
                true,
                "expected group_id in {:?}, got {}",
                (1..groups + 1),
                leader_event.group_id,
            );
            assert_eq!(leader_event.replica_id, 1);
        }
    }

    (task_group, cluster)
}

/// Quickly start a consensus group with 3 nodes and 3 replicas, with leader being replica 1.
pub async fn quickstart_group(
    rockstore_env: &RockStorageEnv<()>,
    nodes: usize,
) -> (
    TaskGroup,
    FixtureCluster<
        (),
        RockStoreCore<KVStore<()>, KVStore<()>>,
        RockStore<KVStore<()>, KVStore<()>>,
    >,
) {
    // FIXME: each node has task group, if not that joinner can block.
    let task_group = TaskGroup::new();
    //  let rockstore_env = RockStorageEnv::new(nodes);
    let mut cluster = ClusterBuilder::new(nodes)
        .election_ticks(2)
        .kv_stores(rockstore_env.rock_kv_stores.clone())
        .storages(rockstore_env.storages.clone())
        .task_group(task_group.clone())
        .build()
        .await;

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
    for i in 0..nodes {
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

