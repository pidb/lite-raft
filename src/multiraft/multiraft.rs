use std::collections::hash_map::HashMap;
use std::marker::PhantomData;
use std::time::Duration;

use prost::encoding::group;
use raft::RawNode;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::interval;

use tracing::trace;

use raft::Config;

use super::config::GroupConfig;
use super::error::Error;
use super::error::RaftGroupError;

use crate::proto::GroupManagerMessage;
use crate::proto::GroupManagerMessageType;
use crate::proto::Message;
use crate::proto::MessageType;
use crate::proto::RaftMessage;
use crate::proto::ReplicaMetadata;
use crate::storage::MultiRaftGroupMemoryStorage;
use crate::storage::MultiRaftStorage;
use crate::storage::RaftStorage;
use crate::storage::RaftStorageImpl;

const NO_GORUP: u64 = 0;

/// node represents a physical node and contains a group of rafts.
pub struct Node {
    node_id: u64,
    group_map: HashMap<u64, ()>,
}

pub struct RaftGroup<S: RaftStorage> {
    group_id: u64,
    replica_id: u64,
    raft_group: RawNode<RaftStorageImpl<S>>,
    // track the nodes which members of the raft consensus group
    node_ids: Vec<u64>,
}

/// MultiRaft represents a group of raft replicas
pub struct MultiRaft<S: RaftStorage, MS: MultiRaftStorage<S>> {
    store_id: u64,
    config: GroupConfig,
    actor_address: MultiRaftActorAddress,
    actor_join_handle: JoinHandle<()>,
    _m1: PhantomData<S>,
    _m2: PhantomData<MS>,
}

impl<S: RaftStorage, MS: MultiRaftStorage<S>> MultiRaft<S, MS> {
    pub fn new(config: GroupConfig, node_id: u64, store_id: u64, storage: MS) -> Self {
        let (actor_join_handle, actor_address) =
            MultiRaftActor::spawn(&config, node_id, store_id, storage);
        Self {
            store_id,
            config,
            actor_address,
            actor_join_handle,
            _m1: PhantomData,
            _m2: PhantomData,
        }
    }

    /// Bootstrap a new raft consensus group.
    pub async fn bootstrap_raft_group(&self, group_id: u64) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let mut msg = GroupManagerMessage::default();
        msg.group_id = group_id;
        msg.replica_id = 0;
        msg.set_msg_type(GroupManagerMessageType::MsgBootstrapGroup);

        if let Err(_error) = self.actor_address.manager_group_tx.send((msg, tx)).await {
            panic!("manager group receiver dropped")
        }

        match rx.await {
            Err(_error) => panic!("sender dopped"),
            Ok(res) => res,
        }
    }
}

/// MultiRaftAddress is used to communicate with MultiRaftActor
pub struct MultiRaftActorAddress {
    raft_message_tx: Sender<RaftMessage>,
    manager_group_tx: Sender<(GroupManagerMessage, oneshot::Sender<Result<(), Error>>)>,
}

pub struct MultiRaftActor<S: RaftStorage, MS: MultiRaftStorage<S>> {
    store_id: u64,
    node_id: u64,
    nodes: HashMap<u64, Node>,
    groups: HashMap<u64, RaftGroup<S>>,
    tick_interval: Duration,
    election_tick: usize,
    heartbeat_tick: usize,
    raft_message_rx: Receiver<RaftMessage>,
    manager_group_rx: Receiver<(GroupManagerMessage, oneshot::Sender<Result<(), Error>>)>,
    storage: MS,
    _m1: PhantomData<S>,
}

impl<S: RaftStorage, MS: MultiRaftStorage<S>> MultiRaftActor<S, MS> {
    // #[tracing::instrument(name = "MultiRaftActor::spawn",  skip(storage))]
    pub fn spawn(
        cfg: &GroupConfig,
        node_id: u64,
        store_id: u64,
        storage: MS,
    ) -> (JoinHandle<()>, MultiRaftActorAddress) {
        let (raft_message_tx, raft_message_rx) = channel(1);
        let (manager_group_tx, manager_group_rx) = channel(1);
        let actor = MultiRaftActor {
            store_id,
            node_id,
            nodes: HashMap::new(),
            groups: HashMap::new(),
            tick_interval: Duration::from_millis(cfg.tick_interval),
            election_tick: cfg.election_tick,
            heartbeat_tick: cfg.heartbeat_tick,
            raft_message_rx,
            manager_group_rx,
            storage,
            _m1: PhantomData,
        };

        let join = tokio::spawn(async move {
            actor.start().await;
        });

        let address = MultiRaftActorAddress {
            raft_message_tx,
            manager_group_tx,
        };

        (join, address)
    }

    /// start actor.
    // #[tracing::instrument(name = "MultiRaftActor::start", skip(self))]
    async fn start(mut self) {
        // Each time ticker expires, the ticks increments,
        // when ticks >= heartbeat_tick triggers the merged heartbeat.
        let mut ticks = 0;
        let mut ticker = interval(self.tick_interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    ticks += 1;
                    if ticks >= self.heartbeat_tick {
                        ticks = 0;
                        self.coalesced_heratbeat()
                    }
                },
                Some(msg) = self.raft_message_rx.recv() => self.handle_raft_message(msg),
                Some((msg, tx)) = self.manager_group_rx.recv() => self.handle_manager_group_message(msg, tx).await,
            }
        }
    }

    fn coalesced_heratbeat(&self) {
        for (node_id, node) in self.nodes.iter() {
            if *node_id == self.node_id {
                continue;
            }

            // coalesced heartbeat to all nodes
            let mut raft_msg = Message::default();
            raft_msg.set_msg_type(MessageType::MsgHeartbeat);
            let msg = RaftMessage {
                group_id: NO_GORUP,
                from_node: self.node_id,
                to_node: *node_id,
                msg: Some(raft_msg),
            };

            // TODO: send
        }
    }

    fn handle_raft_message(&self, msg: RaftMessage) {
        match msg.msg.as_ref().unwrap().msg_type() {
            MessageType::MsgHeartbeat => self.fanout_heartbeat(msg),
            MessageType::MsgHeartbeatResponse => self.fanout_heartbeat_response(msg),
            _ => {}
        }
    }

    fn fanout_heartbeat(&self, msg: RaftMessage) {}

    fn fanout_heartbeat_response(&self, msg: RaftMessage) {}

    #[tracing::instrument(name = "MultiRaftActor::handle_manager_group_message", skip(self))]
    async fn handle_manager_group_message(
        &mut self,
        msg: GroupManagerMessage,
        tx: oneshot::Sender<Result<(), Error>>,
    ) {
        let res = match msg.msg_type() {
            GroupManagerMessageType::MsgBootstrapGroup => self.bootstrap_group(msg.group_id).await,
            GroupManagerMessageType::MsgRemoveGoup => todo!(),
        };

        if let Err(_error) = tx.send(res) {}
    }

    /// Boot a raft consensus group.
    #[tracing::instrument(name = "MultiRaftActor::bootstrap_group", skip(self))]
    async fn bootstrap_group(&mut self, group_id: u64) -> Result<(), Error> {
        if self.groups.contains_key(&group_id) {
            return Err(Error::RaftGroupAlreayExists(group_id));
        }

        let mut replica_id = 0;
        let group_storage = self
            .storage
            .group_storage(group_id, replica_id)
            .await
            .unwrap();
        let initial_raft_state = group_storage.initial_state().unwrap();

        // 扫描 store 中对应这个 group 的 replica 的 id
        let voters = initial_raft_state.conf_state.voters;
        for voter_id in voters.iter() {
            let replica_metadata = self
                .storage
                .replica_metadata(group_id, *voter_id)
                .await
                .unwrap();
            if self.store_id == replica_metadata.store_id {
                if replica_id == 0 {
                    replica_id = *voter_id;
                }
                break;
            }
        }

        if replica_id == 0 {
            return Err(Error::RaftGroupError(RaftGroupError::BootstrapError(
                group_id,
                self.store_id,
            )));
        }

        let applied = 0;
        let raft_cfg = raft::Config {
            id: replica_id,
            applied,
            election_tick: self.election_tick,
            heartbeat_tick: self.heartbeat_tick,
            max_size_per_msg: 1024 * 1024,
            max_inflight_msgs: 256,
            ..Default::default()
        };

        let raft_store = group_storage.clone();
        let raft_group = raft::RawNode::with_default_logger(&raft_cfg, raft_store).unwrap();

        let mut group = RaftGroup {
            group_id,
            replica_id,
            raft_group,
            node_ids: Vec::new(),
        };

        for voter_id in voters.iter() {
            let replica_metadata = self
                .storage
                .replica_metadata(group_id, *voter_id)
                .await
                .unwrap();
            // track the nodes which other members of the raft consensus group
            group.node_ids.push(replica_metadata.node_id);
            self.add_node(replica_metadata.node_id, group_id);
        }

        self.groups.insert(group_id, group);

        Ok(())
    }

    fn add_node(&mut self, node_id: u64, group_id: u64) {
        let node = match self.nodes.get_mut(&node_id) {
            None => {
                self.nodes.insert(
                    node_id,
                    Node {
                        node_id,
                        group_map: HashMap::new(),
                    },
                );
                self.nodes.get_mut(&node_id).unwrap()
            }
            Some(node) => node,
        };

        assert_ne!(group_id, 0);
        node.group_map.insert(group_id, ());
    }

    // async fn create_group(&mut self, group_id: u64, replica_id: u64) -> Result<(), Error> {
    //     if self.groups.contains_key(&group_id) {
    //         return Err(Error::RaftGroupAlreayExists(group_id));
    //     }

    //     let mut replica_id = replica_id;
    //     let group_storage = self
    //         .storage
    //         .group_storage(group_id, replica_id)
    //         .await
    //         .unwrap();
    //     let initial_raft_state = group_storage.initial_state().unwrap();

    //     let voters = initial_raft_state.conf_state.voters;
    //     for voter_id in voters.iter() {
    //         let replica_metadata = self
    //             .storage
    //             .replica_metadata(group_id, *voter_id)
    //             .await
    //             .unwrap();
    //         if self.replica_in_group(&replica_metadata) {
    //             // 如果 replica_id 等于 0, 说明这是一个初始化 group 的请求,
    //             // 应该选择 initial_state 中的 id.
    //             //
    //             // 如果 replica_id 不等于0 并且小于 voter_id, 则说明这是收到其他远程 node 发来的消息
    //             // 并创建 group, 但是 replica_id 发生了不一致.
    //             if replica_id == 0 {
    //                 replica_id = *voter_id;
    //             } else if replica_id < *voter_id {
    //                 return Err(Error::InconsistentReplicaId(
    //                     replica_id,
    //                     *voter_id,
    //                     self.store_id,
    //                 ));
    //             }
    //         }
    //     }

    //     if replica_id == 0 {
    //         return Err(Error::ReplicaNotFound(group_id, self.store_id));
    //     }

    //     Ok(())
    // }

    
}

#[tokio::test(flavor = "multi_thread")]
async fn test_bootstrap_group() {
    tracing_subscriber::fmt::try_init().unwrap();
    let group_id = 1;
    let group_cfg = GroupConfig {
        group_id,
        election_tick: 10,
        heartbeat_tick: 3,
        tick_interval: 100,
    };
    let store_id = 1;
    let node_id = 1;
    let replica_id = 1;

    let storage = MultiRaftGroupMemoryStorage::new(node_id, store_id);
    storage
        .insert_replica_memory_storage_with_conf_state(group_id, replica_id, (vec![1], vec![]))
        .await;

    let multi_raft = MultiRaft::new(group_cfg, node_id, store_id, storage);
    multi_raft.bootstrap_raft_group(1).await.unwrap();

    // tokio::time::sleep(Duration::from_secs(1000)).await;
}
