use std::collections::HashMap;
use std::env::temp_dir;
use std::fs::remove_dir_all;
use std::marker::PhantomData;
use std::mem::take;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use oceanraft::multiraft::storage::MultiRaftMemoryStorage;
use oceanraft::multiraft::storage::RaftMemStorage;
use oceanraft::multiraft::storage::RockStoreCore;
use oceanraft::multiraft::storage::StateMachineStore;
use oceanraft::multiraft::StateMachine;
use oceanraft::multiraft::WriteData;
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
use oceanraft::multiraft::transport::LocalTransport;
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

use super::rsm::MemStoreStateMachine;
use super::rsm::RockStoreStateMachine;

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

pub fn new_rock_kv_store<P, R>(node_id: u64, path: P) -> StateMachineStore<R>
where
    P: AsRef<Path>,
    R: WriteResponse,
{
    println!(
        "🏠 create statemachine storeage {}",
        path.as_ref().display()
    );
    StateMachineStore::<R>::new(node_id, path)
}

pub fn new_rock_kv_stores<P, R>(nodes: usize, paths: Vec<P>) -> Vec<StateMachineStore<R>>
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
            StateMachineStore::<R>::new(node_id, p)
        })
        .collect()
}

pub fn new_rocks_storeage<P, R>(
    node_id: u64,
    path: P,
    kv_store: StateMachineStore<R>,
) -> RockStore<StateMachineStore<R>, StateMachineStore<R>>
where
    P: AsRef<Path>,
    R: WriteResponse,
{
    println!("🚪 create rock storeage {}", path.as_ref().display());
    RockStore::new(node_id, path, kv_store.clone(), kv_store.clone())
}

pub fn new_rocks_storeages<P, R>(
    nodes: usize,
    paths: Vec<P>,
    kv_stores: Vec<StateMachineStore<R>>,
) -> Vec<RockStore<StateMachineStore<R>, StateMachineStore<R>>>
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

/// Provides a mem storage and state machine environment for cluster.
pub struct MemStoreEnv<W, R>
where
    W: WriteData,
    R: WriteResponse,
{
    pub rxs: Vec<Option<Receiver<Vec<Apply<W, R>>>>>,
    pub storages: Vec<MultiRaftMemoryStorage>,
    pub state_machines: Vec<MemStoreStateMachine<W, R>>,
}

impl<W, R> MemStoreEnv<W, R>
where
    W: WriteData,
    R: WriteResponse,
{
    /// Create environments of nodes size, including
    /// - rxs (apply receivers),
    /// - storages (multi-raft memory storage),
    /// - and state_machines (memory state machine implementation).
    pub fn new(nodes: usize) -> Self {
        let mut rxs = vec![];
        let mut storages = vec![];
        let mut state_machines = vec![];
        for i in 0..nodes {
            let (tx, rx) = channel(100);
            rxs.push(Some(rx));
            state_machines.push(MemStoreStateMachine::new(tx));
            storages.push(MultiRaftMemoryStorage::new((i + 1) as u64));
        }

        Self {
            rxs: rxs,
            storages,
            state_machines,
        }
    }
}

/// Provides a rocksdb storage and state machine environment for cluster.
pub struct RockStoreEnv<R>
where
    R: WriteResponse,
{
    pub rxs: Vec<Option<Receiver<Vec<Apply<StoreData, R>>>>>,
    pub storages: Vec<RockStore<StateMachineStore<R>, StateMachineStore<R>>>,
    pub rock_kv_stores: Vec<StateMachineStore<R>>,
    pub state_machines: Vec<RockStoreStateMachine<R>>,
    pub storage_paths: Vec<PathBuf>,
    pub state_machine_paths: Vec<PathBuf>,
}

impl<R> RockStoreEnv<R>
where
    R: WriteResponse,
{
    /// Create environments of nodes size, including
    /// - rxs (apply receivers),
    /// - storages (multi-raft storage with rocksdb),
    /// - rock_kv_stores (provides state machine storage with rocksdb)
    /// - state_machines (base of rock_kv_stores of state machine implementation).
    /// - storage_paths (rocksdbs of storages storage path, destory when test end)
    /// - state_machine_paths (rocksdbs of rock_kv_stores storage path, destory when test end)
    pub fn new(nodes: usize) -> Self {
        let mut rxs = vec![];
        let mut storage_paths = vec![];
        let mut state_machine_paths = vec![];
        let mut rock_kv_stores = vec![];
        let mut storages = vec![];
        let mut state_machines = vec![];
        for i in 0..nodes {
            let node_id = (i + 1) as u64;
            let storage_path = rand_temp_dir(format!("store_db_node_{}", node_id));
            let state_machine_path = rand_temp_dir(format!("state_machine_node_{}", node_id));
            storage_paths.push(storage_path.clone());
            state_machine_paths.push(state_machine_path.clone());

            let kv_store = new_rock_kv_store::<_, R>(node_id, state_machine_path.clone());
            rock_kv_stores.push(kv_store.clone());
            storages.push(new_rocks_storeage::<_, R>(
                node_id,
                storage_path.clone(),
                kv_store.clone(),
            ));
            let (tx, rx) = channel(100);
            state_machines.push(RockStoreStateMachine::new(kv_store, tx));
            rxs.push(Some(rx));
        }

        Self {
            rxs,
            state_machines,
            storages,
            storage_paths,
            rock_kv_stores,
            state_machine_paths,
        }
    }

    /// Destory storages and rock_kv_stores used paths.
    pub fn destory(mut self) {
        self.state_machine_paths.extend(self.storage_paths);
        for p in self.state_machine_paths.iter() {
            println!("🌪 remove dir {}", p.display());
            remove_dir_all(p).unwrap();
        }
    }
}

pub struct ClusterBuilder<S, MS, W, R, RSM>
where
    S: RaftStorage,
    MS: MultiRaftStorage<S>,
    W: WriteData,
    R: WriteResponse,
    RSM: StateMachine<W, R>,
{
    node_size: usize,
    election_ticks: usize,
    tg: Option<TaskGroup>,
    storages: Vec<MS>,
    apply_rxs: Vec<Option<Receiver<Vec<Apply<W, R>>>>>,
    // kv_stores: Vec<StateMachineStore<R>>,
    state_machines: Vec<Option<RSM>>,
    _m1: PhantomData<S>,
    _m2: PhantomData<W>,
    _m3: PhantomData<R>,
}

impl<S, MS, W, R, RSM> ClusterBuilder<S, MS, W, R, RSM>
where
    S: RaftStorage,
    MS: MultiRaftStorage<S>,
    W: WriteData,
    R: WriteResponse,
    RSM: StateMachine<W, R>,
{
    pub fn new(nodes: usize) -> Self {
        Self {
            node_size: nodes,
            election_ticks: 0,
            tg: None,
            storages: Vec::new(),
            state_machines: Vec::new(),
            apply_rxs: Vec::new(),
            _m1: PhantomData,
            _m2: PhantomData,
            _m3: PhantomData,
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

    pub fn apply_rxs(mut self, rxs: Vec<Option<Receiver<Vec<Apply<W, R>>>>>) -> Self {
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

    pub fn state_machines(mut self, state_machines: Vec<RSM>) -> Self {
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

    pub async fn build(mut self) -> FixtureCluster<S, MS, W, R, RSM> {
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
                batch_apply: false,
                batch_size: 0,
                proposal_queue_size: 1000,
                replica_sync: true,
            };
            let ticker = ManualTick::new();
            // let (event_tx, event_rx) = channel(1);

            // let (apply_event_tx, apply_event_rx) = channel(100);
            // let state_machine =
            // RockStoreStateMachine::new(self.kv_stores[i].clone(), apply_event_tx);
            // let rsm = FixtureStateMachine::new(apply_event_tx);
            // let storage = MultiRaftMemoryStorage::new(config.node_id);
            let node = MultiRaft::new(
                config,
                transport.clone(),
                self.storages[i].clone(),
                self.state_machines[i]
                    .take()
                    .expect("state machines can't initialize"),
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
            // apply_events.push(Some(apply_event_rx));

            tickers.push(ticker.clone());
        }
        FixtureCluster {
            storages: self.storages,
            apply_events: take(&mut self.apply_rxs),
            nodes,
            transport,
            tickers,
            election_ticks: self.election_ticks,
            groups: HashMap::new(),
            _m1: PhantomData,
        }
    }
}

pub struct FixtureCluster<S, MS, W, R, RSM>
where
    S: RaftStorage,
    MS: MultiRaftStorage<S>,
    W: WriteData,
    R: WriteResponse,
    RSM: StateMachine<W, R>,
{
    pub election_ticks: usize,
    pub nodes: Vec<Arc<MultiRaft<W, R, RSM>>>,
    pub apply_events: Vec<Option<Receiver<Vec<Apply<W, R>>>>>,
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

pub type MemStoreCluster = FixtureCluster<
    RaftMemStorage,
    MultiRaftMemoryStorage,
    StoreData,
    (),
    MemStoreStateMachine<StoreData, ()>,
>;

pub type RockCluster = FixtureCluster<
    RockStoreCore<StateMachineStore<()>, StateMachineStore<()>>,
    RockStore<StateMachineStore<()>, StateMachineStore<()>>,
    StoreData,
    (),
    RockStoreStateMachine<()>,
>;

impl<S, MS, W, R, RSM> FixtureCluster<S, MS, W, R, RSM>
where
    S: RaftStorage,
    MS: MultiRaftStorage<S>,
    W: WriteData,
    R: WriteResponse,
    RSM: StateMachine<W, R>,
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
    ) -> Result<Vec<ApplyNormal<W, R>>, String> {
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
        cluster: &mut FixtureCluster<S, MS, W, R, RSM>,
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
        write_data: W,
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
pub async fn quickstart_rockstore_multi_groups(
    rockstore_env: &mut RockStoreEnv<()>,
    nodes: usize,
    groups: usize,
) -> (
    TaskGroup,
    FixtureCluster<
        RockStoreCore<StateMachineStore<()>, StateMachineStore<()>>,
        RockStore<StateMachineStore<()>, StateMachineStore<()>>,
        StoreData,
        (),
        RockStoreStateMachine<()>,
    >,
) {
    // FIXME: each node has task group, if not that joinner can block.
    let task_group = TaskGroup::new();
    // let mut cluster = FixtureCluster::make(node_nums as u64, task_group.clone()).await;
    // cluster.start();

    let mut cluster = ClusterBuilder::new(nodes)
        .election_ticks(2)
        .state_machines(rockstore_env.state_machines.clone())
        .storages(rockstore_env.storages.clone())
        .task_group(task_group.clone())
        .apply_rxs(take(&mut rockstore_env.rxs))
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
pub async fn quickstart_rockstore_group(
    rockstore_env: &mut RockStoreEnv<()>,
    nodes: usize,
) -> (
    TaskGroup,
    FixtureCluster<
        RockStoreCore<StateMachineStore<()>, StateMachineStore<()>>,
        RockStore<StateMachineStore<()>, StateMachineStore<()>>,
        StoreData,
        (),
        RockStoreStateMachine<()>,
    >,
) {
    // FIXME: each node has task group, if not that joinner can block.
    let task_group = TaskGroup::new();
    //  let rockstore_env = RockStorageEnv::new(nodes);
    let mut cluster = ClusterBuilder::new(nodes)
        .election_ticks(2)
        .state_machines(rockstore_env.state_machines.clone())
        .storages(rockstore_env.storages.clone())
        .task_group(task_group.clone())
        .apply_rxs(std::mem::take(&mut rockstore_env.rxs))
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
