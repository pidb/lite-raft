use std::collections::HashMap;

use oceanraft::memstore::MultiRaftMemoryStorage;
use oceanraft::memstore::RaftMemStorage;

use oceanraft::multiraft::storage::MultiRaftStorage;
use oceanraft::multiraft::storage::RaftStorage;
use oceanraft::multiraft::Config;
use oceanraft::multiraft::Event;
use oceanraft::multiraft::LeaderElectionEvent;
use oceanraft::multiraft::RaftMessageDispatchImpl;

use oceanraft::prelude::AdminMessage;
use oceanraft::prelude::AdminMessageType;
use oceanraft::prelude::ConfState;
use oceanraft::prelude::HardState;
use oceanraft::prelude::MultiRaft;
use oceanraft::prelude::RaftGroupManagement;
use oceanraft::prelude::RaftGroupManagementType;
use oceanraft::prelude::ReplicaDesc;
use oceanraft::prelude::Snapshot;

use oceanraft::util::TaskGroup;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tracing::info;

use harness::transport::LocalTransport;

type FixtureMultiRaft =
    MultiRaft<LocalTransport<RaftMessageDispatchImpl>, RaftMemStorage, MultiRaftMemoryStorage>;

pub struct FixtureCluster {
    storages: Vec<MultiRaftMemoryStorage>,
    multirafts: Vec<FixtureMultiRaft>,
    events: Vec<Receiver<Vec<Event>>>,
    groups: HashMap<u64, Vec<u64>>, // track group which nodes, group_id -> nodes
    transport: LocalTransport<RaftMessageDispatchImpl>,
}

impl FixtureCluster {
    pub async fn make(num: u64, task_group: TaskGroup) -> FixtureCluster {
        let mut multirafts = vec![];
        let mut storages = vec![];
        let mut events = vec![];
        let transport = LocalTransport::new();
        for n in 0..num {
            let node_id = n + 1;
            let config = Config {
                node_id,
                election_tick: 2,
                heartbeat_tick: 1,
                tick_interval: 3_600_000, // hour ms
            };

            let (event_tx, event_rx) = channel(1);
            let storage = MultiRaftMemoryStorage::new(config.node_id);

            info!(
                "start multiraft in node {}, config for {:?}",
                config.node_id, config
            );

            let multiraft = FixtureMultiRaft::new(
                config,
                transport.clone(),
                storage.clone(),
                task_group.clone(),
                event_tx,
            );

            transport
                .listen(
                    node_id,
                    format!("test://node/{}", node_id).as_str(),
                    multiraft.dispatch_impl(),
                )
                .await
                .unwrap();

            multirafts.push(multiraft);
            events.push(event_rx);
            storages.push(storage);
        }
        Self {
            events,
            storages,
            multirafts,
            transport,
            groups: HashMap::new(),
        }
    }

    pub async fn make_group(&mut self, group_id: u64, first_node: u64, replica_num: usize) {
        let mut voters = vec![];
        let mut replicas = vec![];
        for i in 0..replica_num {
            let replica_id = (i + 1) as u64;
            let node_id = first_node + i as u64 + 1 as u64;
            voters.push(replica_id);
            replicas.push(ReplicaDesc {
                node_id,
                replica_id,
            });
        }

        for i in 0..replica_num {
            let node_index = first_node as usize + i;
            let node_id = first_node + i as u64 + 1 as u64;
            let replica_id = (i + 1) as u64;
            let storage = &self.storages[node_index];
            let gs = storage.group_storage(group_id, replica_id).await.unwrap();

            // init hardstate
            let mut hs = HardState::default();
            hs.commit = 1;
            hs.term = 1;
            gs.set_hardstate(hs).unwrap();

            // init confstate
            let mut cs = ConfState::default();
            cs.voters = voters.clone();
            gs.wl().set_conf_state(cs);

            // apply snapshot
            let mut ss = Snapshot::default();
            ss.mut_metadata().mut_conf_state().voters = voters.clone();
            ss.mut_metadata().index = 1;
            ss.mut_metadata().term = 1;
            gs.wl().apply_snapshot(ss).unwrap();

            let multiraft = &self.multirafts[node_index];

            // create admin message for create raft grop
            let mut admin_msg = AdminMessage::default();
            admin_msg.set_msg_type(AdminMessageType::RaftGroup);
            let mut msg = RaftGroupManagement::default();
            msg.set_msg_type(RaftGroupManagementType::MsgCreateGroup);
            msg.group_id = group_id;
            msg.replica_id = replica_id;
            msg.replicas = replicas.clone();
            admin_msg.raft_group = Some(msg);
            multiraft.admin(admin_msg).await.unwrap();

            match self.groups.get_mut(&group_id) {
                None => {
                    self.groups.insert(group_id, vec![node_id as u64]);
                }
                Some(nodes) => nodes.push(node_id as u64),
            };
        }
    }

    pub async fn check_elect(&mut self, node_index: u64, should_leaeder_id: u64, group_id: u64) {
        println!(
            "check node {} leader {} in group {}",
            node_index + 1,
            should_leaeder_id,
            group_id
        );
        // trigger an election for the replica in the group of the node where leader nodes.
        self.trigger_elect(node_index, group_id).await;

        for node_id in self.groups.get(&group_id).unwrap().iter() {
            let election = FixtureCluster::wait_for_leader_elect(&mut self.events, *node_id - 1)
                .await
                .unwrap();
            assert_ne!(election.leader_id, 0);
            assert_eq!(election.group_id, group_id);
            assert_eq!(election.leader_id, should_leaeder_id);
            let storage = &self.storages[node_index as usize];
            let replica_desc = storage
                .replica_for_node(group_id, *node_id)
                .await
                .unwrap()
                .unwrap();
            println!("replica_desc {:?}", replica_desc);
            // assert_eq!(election.replica_id, replica_desc.replica_id);
        }
    }

    async fn trigger_elect(&self, node_index: u64, group_id: u64) {
        self.multirafts[node_index as usize]
            .campagin(group_id)
            .await
    }

    async fn wait_for_leader_elect(
        events: &mut Vec<Receiver<Vec<Event>>>,
        node_id: u64,
    ) -> Option<LeaderElectionEvent> {
        let event = &mut events[node_id as usize];
        loop {
            let events = event.recv().await.unwrap();
            for event in events {
                match event {
                    Event::LederElection(leader_elect) => return Some(leader_elect),
                    _ => {}
                }
            }
        }
    }
}

impl FixtureCluster {}

#[tokio::test(flavor = "multi_thread")]
async fn test_initial_leader_elect() {
    // install global collector configured based on RUST_LOG env var.
    let collector = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(collector);

    for node_index in 0..3 {
        let task_group = TaskGroup::new();
        let mut cluster = FixtureCluster::make(3, task_group.clone()).await;
        let group_id = 1;
        cluster.make_group(group_id, 0, 3).await;

        cluster
            .check_elect(node_index, node_index + 1, group_id)
            .await;
        
        cluster.transport.stop_all().await.unwrap();
        task_group.stop();
        task_group.joinner().await;
    }
}
