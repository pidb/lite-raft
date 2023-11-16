use std::mem::take;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use oceanraft::ProposeRequest;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;

use oceanraft::define_multiraft;
use oceanraft::prelude::StoreData;
use oceanraft::rsm_event::Apply;
use oceanraft::rsm_event::LeaderElectionEvent;
use oceanraft::storage::MemStorage;
use oceanraft::storage::MultiRaftMemoryStorage;
use oceanraft::storage::RockStore;
use oceanraft::storage::RockStoreCore;
use oceanraft::storage::StateMachineStore;
use oceanraft::ProposeResponse;

use super::rand_temp_dir;
use super::rsm::MemStoreStateMachine;
use super::rsm::RockStoreStateMachine;
use super::Cluster;
use super::ClusterBuilder;
use super::MakeGroupPlan;

define_multiraft! {
    pub RockType:
        Request = StoreData,
        Response= (),
        M= RockStoreStateMachine,
        S= RockStoreCore<StateMachineStore<()>, StateMachineStore<()>>,
        MS = RockStore<StateMachineStore<()>, StateMachineStore<()>>
}

define_multiraft! {
    pub MemType:
        Request = StoreData,
        Response= (),
        M= MemStoreStateMachine<StoreData>,
        S= MemStorage,
        MS = MultiRaftMemoryStorage
}

pub enum StateMachineEvent<W, R>
where
    W: ProposeRequest,
    R: ProposeResponse,
{
    LeaderElection(LeaderElectionEvent),
    Apply(Vec<Apply<W, R>>),
}

pub fn new_rock_kv_store<P>(node_id: u64, path: P) -> StateMachineStore<()>
where
    P: AsRef<Path>,
{
    println!(
        "🏠 create statemachine storeage {}",
        path.as_ref().display()
    );
    StateMachineStore::new(node_id, path)
}

pub fn new_rock_kv_stores<P, R>(nodes: usize, paths: Vec<P>) -> Vec<StateMachineStore<R>>
where
    P: AsRef<Path>,
    R: ProposeResponse,
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

pub fn new_rocks_storeage<P>(
    node_id: u64,
    path: P,
    kv_store: StateMachineStore<()>,
) -> RockStore<StateMachineStore<()>, StateMachineStore<()>>
where
    P: AsRef<Path>,
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
    R: ProposeResponse,
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
pub struct MemStoreEnv {
    // pub rxs: Vec<Option<Receiver<Vec<Apply<StoreData, ()>>>>>,
    pub rxs2: Vec<Option<Receiver<StateMachineEvent<StoreData, ()>>>>,
    pub storages: Vec<MultiRaftMemoryStorage>,
    pub state_machines: Vec<MemStoreStateMachine<StoreData>>,
}

impl MemStoreEnv {
    /// Create environments of nodes size, including
    /// - rxs (apply receivers),
    /// - storages (multi-raft memory storage),
    /// - and state_machines (memory state machine implementation).
    pub fn new(nodes: usize) -> Self {
        // let mut rxs = vec![];
        let mut rxs2 = vec![];
        let mut storages = vec![];
        let mut state_machines = vec![];
        for i in 0..nodes {
            // let (tx, rx) = channel(100);
            let (event_tx, event_rx) = channel(100);
            // rxs.push(Some(rx));
            rxs2.push(Some(event_rx));
            state_machines.push(MemStoreStateMachine::new(event_tx));
            storages.push(MultiRaftMemoryStorage::new((i + 1) as u64));
        }

        Self {
            // rxs,
            rxs2,
            storages,
            state_machines,
        }
    }
}

/// Provides a rocksdb storage and state machine environment for cluster.
pub struct RockStoreEnv {
    // pub rxs: Vec<Option<Receiver<Vec<Apply<StoreData, ()>>>>>,
    pub rxs2: Vec<Option<Receiver<StateMachineEvent<StoreData, ()>>>>,
    pub storages: Vec<RockStore<StateMachineStore<()>, StateMachineStore<()>>>,
    pub rock_kv_stores: Vec<StateMachineStore<()>>,
    pub state_machines: Vec<RockStoreStateMachine>,
    pub storage_paths: Vec<PathBuf>,
    pub state_machine_paths: Vec<PathBuf>,
}

impl RockStoreEnv {
    /// Create environments of nodes size, including
    /// - rxs (apply receivers),
    /// - storages (multi-raft storage with rocksdb),
    /// - rock_kv_stores (provides state machine storage with rocksdb)
    /// - state_machines (base of rock_kv_stores of state machine implementation).
    /// - storage_paths (rocksdbs of storages storage path, destory when test end)
    /// - state_machine_paths (rocksdbs of rock_kv_stores storage path, destory when test end)
    pub fn new(nodes: usize) -> Self {
        let mut rxs2 = vec![];
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

            let kv_store = new_rock_kv_store::<_>(node_id, state_machine_path.clone());
            rock_kv_stores.push(kv_store.clone());
            storages.push(new_rocks_storeage::<_>(
                node_id,
                storage_path.clone(),
                kv_store.clone(),
            ));
            let (event_tx, event_rx) = channel(100);
            state_machines.push(RockStoreStateMachine::new(kv_store, event_tx));
            rxs2.push(Some(event_rx));
        }

        Self {
            rxs2,
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
            std::fs::remove_dir_all(p).unwrap();
        }
    }
}

/// Multiple consensus groups are quickly started. Node and consensus group ids start from 1.
/// All consensus group replicas equal to 1 are elected as the leader.
pub async fn quickstart_rockstore_multi_groups(
    rockstore_env: &mut RockStoreEnv,
    nodes: usize,
    groups: usize,
) -> Cluster<RockType> {
    // FIXME: each node has task group, if not that joinner can block.
    // let mut cluster = FixtureCluster::make(node_nums as u64, task_group.clone()).await;
    // cluster.start();

    let mut cluster = ClusterBuilder::new(nodes)
        .election_ticks(2)
        .state_machines(rockstore_env.state_machines.clone())
        .storages(rockstore_env.storages.clone())
        .event_rxs(take(&mut rockstore_env.rxs2))
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
            let leader_event = Cluster::wait_leader_elect_event(
                &mut cluster,
                j + 1,
                Some(Duration::from_millis(1000)),
            )
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

    cluster
}

/// Quickly start a consensus group with 3 nodes and 3 replicas, with leader being replica 1.
pub async fn quickstart_rockstore_group(
    rockstore_env: &mut RockStoreEnv,
    nodes: usize,
) -> Cluster<RockType> {
    // FIXME: each node has task group, if not that joinner can block.
    //  let rockstore_env = RockStorageEnv::new(nodes);
    let mut cluster = ClusterBuilder::new(nodes)
        .election_ticks(2)
        .state_machines(rockstore_env.state_machines.clone())
        .storages(rockstore_env.storages.clone())
        .event_rxs(std::mem::take(&mut rockstore_env.rxs2))
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
        let leader_event = Cluster::wait_leader_elect_event(
            &mut cluster,
            i as u64 + 1,
            Some(Duration::from_millis(1000)),
        )
        .await
        .unwrap();
        assert_eq!(leader_event.group_id, 1);
        assert_eq!(leader_event.replica_id, 1);
    }

    cluster
}

pub async fn quickstart_memstorage_group(env: &mut MemStoreEnv, nodes: usize) -> Cluster<MemType> {
    // FIXME: each node has task group, if not that joinner can block.
    //  let rockstore_env = RockStorageEnv::new(nodes);
    let mut cluster = ClusterBuilder::new(nodes)
        .election_ticks(2)
        .state_machines(env.state_machines.clone())
        .storages(env.storages.clone())
        .event_rxs(std::mem::take(&mut env.rxs2))
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
        let leader_event = Cluster::wait_leader_elect_event(
            &mut cluster,
            i as u64 + 1,
            Some(Duration::from_millis(1000)),
        )
        .await
        .unwrap();
        assert_eq!(leader_event.group_id, 1);
        assert_eq!(leader_event.replica_id, 1);
    }
    cluster
}
