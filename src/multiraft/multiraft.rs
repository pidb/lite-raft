use std::collections::hash_map::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::time::Duration;

use prost::Message as ProstMessage;
use raft::RawNode;
use raft::Ready;
use smallvec::SmallVec;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::interval;

use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;

use raft::Config;

use super::apply::ApplyActor;
use super::apply::ApplyActorAddress;
use super::apply::ApplyResult;
use super::apply::ApplyTaskResponse;
use super::apply::MembershipChangeResult;
use super::config::GroupConfig;
use super::error::Error;
use super::error::ProposalError;
use super::node::NodeManager;
use super::proposal::GroupProposalQueue;
use super::proposal::Proposal;
use super::proposal::ProposalQueueManager;
use super::proposal::ReadIndexProposal;
use super::raft_ready::WriteReadyHandler;
use super::replica_cache::ReplicaMetadataCache;
use super::transport::MessageInterface;
use super::transport::Transport;

use crate::proto::AppReadIndexRequest;
use crate::proto::AppReadIndexResponse;
use crate::proto::AppWriteRequest;
use crate::proto::AppWriteResponse;
use crate::proto::Message;
use crate::proto::MessageType;
use crate::proto::RaftGroupManagementMessage;
use crate::proto::RaftGroupManagementMessageType;
use crate::proto::RaftMessage;
use crate::proto::ReplicaMetadata;
use crate::storage::transmute_message;

use crate::storage::MultiRaftStorage;
use crate::storage::RaftStorage;
use crate::storage::RaftStorageImpl;
use crate::storage::StorageError;
// use crate::proto::Error;

pub const NO_GORUP: u64 = 0;
const NO_NODE: u64 = 0;

/// node represents a physical node and contains a group of rafts.
pub struct Node {
    node_id: u64,
    group_map: HashMap<u64, ()>,
}

/// Represents a replica of a raft group.
pub struct RaftGroup<RS: RaftStorage> {
    pub group_id: u64,
    pub replica_id: u64,
    pub raft_group: RawNode<RaftStorageImpl<RS>>,
    // track the nodes which members ofq the raft consensus group
    pub node_ids: Vec<u64>,
    pub proposals: GroupProposalQueue,
    pub leader: Option<ReplicaMetadata>,
}

/// MultiRaft represents a group of raft replicas
pub struct MultiRaft<MI, T, RS, MRS>
where
    MI: MessageInterface,
    T: Transport<MI>,
    RS: RaftStorage,
    MRS: MultiRaftStorage<RS>,
{
    store_id: u64,
    config: GroupConfig,
    actor_address: MultiRaftActorAddress,
    actor_join_handle: JoinHandle<()>,
    _m1: PhantomData<MI>,
    _m2: PhantomData<T>,
    _m3: PhantomData<RS>,
    _m4: PhantomData<MRS>,
}

impl<MI, T, RS, MRS> MultiRaft<MI, T, RS, MRS>
where
    MI: MessageInterface,
    T: Transport<MI>,
    RS: RaftStorage,
    MRS: MultiRaftStorage<RS>,
{
    pub fn new(
        config: GroupConfig,
        node_id: u64,
        store_id: u64,
        transport: T,
        storage: MRS,
    ) -> Self {
        let (stop_tx, stop_rx) = watch::channel(false);
        let (actor_join_handle, actor_address) =
            MultiRaftActor::spawn(&config, node_id, store_id, transport, storage, stop_rx);
        Self {
            store_id,
            config,
            actor_address,
            actor_join_handle,
            _m1: PhantomData,
            _m2: PhantomData,
            _m3: PhantomData,
            _m4: PhantomData,
        }
    }

    pub async fn write(&self, request: AppWriteRequest) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        if let Err(_) = self
            .actor_address
            .write_propose_tx
            .send((request, tx))
            .await
        {}

        rx.await.unwrap()
    }

    pub async fn read_index(&self, request: AppReadIndexRequest) -> Result<(), Error> {
         let (tx, rx) = oneshot::channel();
        if let Err(_) = self
            .actor_address
            .read_index_propose_tx
            .send((request, tx))
            .await
        {}

        rx.await.unwrap()
    }

    pub async fn initial_raft_group(&self, msg: RaftGroupManagementMessage) -> Result<(), Error> {
        assert_eq!(
            msg.msg_type(),
            RaftGroupManagementMessageType::MsgInitialGroup
        );
        let (tx, rx) = oneshot::channel();
        if let Err(_error) = self.actor_address.manager_group_tx.send((msg, tx)).await {
            panic!("manager group receiver dropped")
        }

        match rx.await {
            Err(_error) => panic!("sender dopped"),
            Ok(res) => res,
        }
    }

    /// Bootstrap a new raft consensus group.
    pub async fn bootstrap_raft_group(&self, group_id: u64, replica_id: u64) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let mut msg = RaftGroupManagementMessage::default();
        msg.group_id = group_id;
        msg.replica_id = replica_id;
        msg.set_msg_type(RaftGroupManagementMessageType::MsgBootstrapGroup);

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
    write_propose_tx: Sender<(
        AppWriteRequest,
        oneshot::Sender<Result<(), Error>>,
    )>,
    read_index_propose_tx: Sender<(
        AppReadIndexRequest,
        oneshot::Sender<Result<(), Error>>
    )>,
    raft_message_tx: Sender<RaftMessage>,
    manager_group_tx: Sender<(
        RaftGroupManagementMessage,
        oneshot::Sender<Result<(), Error>>,
    )>,
}

pub struct MultiRaftActor<MI, T, RS, MRS>
where
    MI: MessageInterface,
    T: Transport<MI>,
    RS: RaftStorage,
    MRS: MultiRaftStorage<RS>,
{
    store_id: u64,
    node_id: u64,
    //  nodes: HashMap<u64, Node>,
    node_manager: NodeManager,
    groups: HashMap<u64, RaftGroup<RS>>,
    tick_interval: Duration,
    election_tick: usize,
    heartbeat_tick: usize,
    write_propose_rx: Receiver<(
        AppWriteRequest,
        oneshot::Sender<Result<(), Error>>,
    )>,
    read_index_propose_rx: Receiver<(
        AppReadIndexRequest,
        oneshot::Sender<Result<(), Error>>,
    )>,
    raft_message_rx: Receiver<RaftMessage>,
    manager_group_rx: Receiver<(
        RaftGroupManagementMessage,
        oneshot::Sender<Result<(), Error>>,
    )>,

    // write_actor_address: WriteAddress,
    apply_actor_address: ApplyActorAddress,
    storage: MRS,
    transport: T,
    // waiting_ready_groups: VecDeque<HashMap<u64, Ready>>,
    // proposals: ProposalQueueManager,
    replica_metadata_cache: ReplicaMetadataCache<RS, MRS>,
    _m1: PhantomData<RS>,
    _m2: PhantomData<MI>,
}

impl<MI, T, RS, MRS> MultiRaftActor<MI, T, RS, MRS>
where
    MI: MessageInterface,
    T: Transport<MI>,
    RS: RaftStorage,
    MRS: MultiRaftStorage<RS>,
{
    // #[tracing::instrument(name = "MultiRaftActor::spawn",  skip(storage))]
    pub fn spawn(
        cfg: &GroupConfig,
        node_id: u64,
        store_id: u64,
        transport: T,
        storage: MRS,
        stop: watch::Receiver<bool>,
    ) -> (JoinHandle<()>, MultiRaftActorAddress) {
        let (apply_to_tx, apply_to_rx) = channel(1);
        let (raft_message_tx, raft_message_rx) = channel(1);
        let (manager_group_tx, manager_group_rx) = channel(1);

        // let (write_actor_join, write_actor_address) =
        //     WriterActor::spawn(storage.clone(), stop.clone());

        let (apply_actor_join, apply_actor_address) = ApplyActor::spawn(apply_to_tx, stop.clone());

        // create write propose channel
        let (write_propose_tx, write_propose_rx) = channel(1);
        let (read_index_propose_tx, read_index_propose_rx) = channel(1);

        let actor = MultiRaftActor {
            store_id,
            node_id,
            // nodes: HashMap::new(),
            node_manager: NodeManager::new(),

            groups: HashMap::new(),
            tick_interval: Duration::from_millis(cfg.tick_interval),
            election_tick: cfg.election_tick,
            heartbeat_tick: cfg.heartbeat_tick,
            write_propose_rx,
            read_index_propose_rx,
            raft_message_rx,
            manager_group_rx,
            storage,
            transport,
            // write_actor_address,
            apply_actor_address,
            replica_metadata_cache: ReplicaMetadataCache::new(storage.clone()),
            // waiting_ready_groups: VecDeque::default(),
            _m1: PhantomData,
            _m2: PhantomData,
        };

        let main_loop = async move {
            actor.start(stop).await;
        };

        let join = tokio::spawn(main_loop);

        let address = MultiRaftActorAddress {
            raft_message_tx,
            manager_group_tx,
            write_propose_tx,
            read_index_propose_tx,
        };

        (join, address)
    }

    /// start actor.
    // #[tracing::instrument(name = "MultiRaftActor::start", skip(self))]
    async fn start(mut self, mut stop: watch::Receiver<bool>) {
        // Each time ticker expires, the ticks increments,
        // when ticks >= heartbeat_tick triggers the merged heartbeat.
        let mut ticks = 0;
        let mut ticker = interval(self.tick_interval);
        let mut activity_groups = HashSet::new();
        loop {
            tokio::select! {
                // handle stop
                _ = stop.changed() => {
                    if *stop.borrow() {
                        break
                    }
                }

                _ = ticker.tick() => {
                    // tick all groups
                    self.groups.iter_mut().for_each(|(_, group)| {
                        if group.raft_group.tick() {
                            activity_groups.insert(group.group_id);
                        }
                    });

                    ticks += 1;
                    if ticks >= self.heartbeat_tick {
                        ticks = 0;
                        self.coalesced_heratbeat().await;
                    }
                },

                Some(msg) = self.raft_message_rx.recv() =>self.handle_raft_message(msg, &mut activity_groups).await,

                Some((request, tx)) = self.write_propose_rx.recv() => self.handle_write_request(request, tx),

                Some((request, tx)) = self.read_index_propose_rx.recv() => self.handle_read_index_request(request, tx),

                Some((msg, tx)) = self.manager_group_rx.recv() => {
                    self.handle_manager_group_message(msg, tx, &mut activity_groups).await;
                },

                else => {
                    let mut write_ready_handle = WriteReadyHandler{
                        node_id: self.node_id,
                        groups: &mut self.groups,
                        node_mgr: &mut self.node_manager,
                        storage: &self.storage,
                        transport: &self.transport,
                        apply_actor_address: &self.apply_actor_address,
                        _m: PhantomData,
                    };

                    write_ready_handle.on_groups_ready(&activity_groups).await;
                    activity_groups.clear();
                },
            }
        }
    }

    /// The node sends heartbeats to other nodes instead
    /// of all raft groups on that node.
    async fn coalesced_heratbeat(&self) {
        for (node_id, node) in self.node_manager.iter() {
            if *node_id == self.node_id {
                continue;
            }

            debug!(
                "trigger node coalesced heartbeta {} -> {} ",
                self.node_id, *node_id
            );

            // coalesced heartbeat to all nodes. the heartbeat message is node
            // level message so from and to set 0 when sending, and the specific
            // value is set by message receiver.
            let mut raft_msg = Message::default();
            raft_msg.set_msg_type(MessageType::MsgHeartbeat);
            let msg = RaftMessage {
                group_id: NO_GORUP,
                from_node: self.node_id,
                to_node: *node_id,
                msg: Some(raft_msg),
            };

            if let Err(_error) = self.transport.send(msg) {}
        }
    }

    /// Fanout node heartbeat and handle raft messages.
    async fn handle_raft_message(
        &mut self,
        mut msg: RaftMessage,
        activity_groups: &mut HashSet<u64>,
    ) {
        let raft_msg = msg.msg.take().expect("invalid message");
        match raft_msg.msg_type() {
            MessageType::MsgHeartbeat => {
                self.fanout_heartbeat(msg, activity_groups).await;
                return;
            }
            MessageType::MsgHeartbeatResponse => {
                self.fanout_heartbeat_response(msg, activity_groups).await;
                return;
            }
            _ => {}
        }

        // processing messages between replicas from other nodes to self node.
        let group_id = msg.group_id;

        let from_replica = ReplicaMetadata {
            node_id: msg.from_node,
            replica_id: raft_msg.from,
            store_id: 0,
        };

        let to_replica = ReplicaMetadata {
            node_id: msg.to_node,
            replica_id: raft_msg.to,
            store_id: 0,
        };

        self.replica_metadata_cache
            .cache(group_id, from_replica.clone())
            .await;
        self.replica_metadata_cache
            .cache(group_id, to_replica.clone())
            .await;

        if !self.node_manager.contains_node(&from_replica.node_id) {
            self.node_manager.add_node(from_replica.node_id, group_id);
        }

        // if a group exists, try to maintain groups on the node
        if self.groups.contains_key(&group_id)
            && !self.node_manager.contains_node(&to_replica.node_id)
        {
            self.node_manager.add_node(to_replica.node_id, group_id);
        }

        let group = match self.groups.get_mut(&group_id) {
            Some(group) => group,
            None => {
                self.create_raft_group(group_id, to_replica.replica_id)
                    .await
                    .unwrap();
                self.groups.get_mut(&group_id).unwrap()
            }
        };

        group.raft_group.step(transmute_message(raft_msg)).unwrap();
        activity_groups.insert(group_id);
    }

    /// Fanout heartbeats from other nodes to all raft groups on this node.
    async fn fanout_heartbeat(&mut self, msg: RaftMessage, activity_groups: &mut HashSet<u64>) {
        if let Some(from_node) = self.node_manager.get_node(&msg.from_node) {
            let mut fanouted_groups = 0;
            let mut fanouted_followers = 0;
            for (group_id, _) in from_node.group_map.iter() {
                let group = match self.groups.get_mut(group_id) {
                    None => {
                        warn!(
                            "missing group {} at from_node {} fanout heartbeat",
                            *group_id, msg.from_node
                        );
                        continue;
                    }
                    Some(group) => group,
                };

                fanouted_groups += 1;
                activity_groups.insert(*group_id);

                let leader = match group.leader.as_ref() {
                    None => continue,
                    Some(leader) => leader,
                };

                if leader.node_id != msg.from_node || msg.from_node != self.node_id {
                    continue;
                }

                // gets the replica stored in this node.
                let from_replica_id = self
                    .storage
                    .replica_in_node(*group_id, msg.from_node)
                    .await
                    .unwrap()
                    .unwrap();
                let to_replica_id = self
                    .storage
                    .replica_in_node(*group_id, msg.to_node)
                    .await
                    .unwrap()
                    .unwrap();

                fanouted_followers += 1;

                let mut msg = raft::prelude::Message::default();
                msg.set_msg_type(raft::prelude::MessageType::MsgHeartbeat);
                msg.from = from_replica_id;
                msg.to = to_replica_id;
                group.raft_group.step(msg).unwrap();
            }
        } else {
            // In this point, we receive heartbeat message from other nodes,
            // but we don't have the from_node raft group information, so we
            // don't know which raft group's replica we should fanout to.
            warn!(
                "missing raft groups at from_node {} fanout heartbeat",
                msg.from_node
            );
            self.node_manager.add_node(msg.from_node, NO_GORUP);
        }

        let response_msg = {
            let mut raft_msg = Message::default();
            raft_msg.set_msg_type(MessageType::MsgHeartbeatResponse);
            RaftMessage {
                group_id: NO_GORUP,
                from_node: self.node_id,
                to_node: msg.to_node,
                msg: Some(raft_msg),
            }
        };

        self.transport.send(response_msg).unwrap();
    }

    /// Fanout heartbeats response from other nodes to all raft groups on this node.
    async fn fanout_heartbeat_response(
        &mut self,
        msg: RaftMessage,
        activity_groups: &mut HashSet<u64>,
    ) {
        if let Some(node) = self.node_manager.get_node(&msg.from_node) {
            for (group_id, _) in node.group_map.iter() {
                let group = match self.groups.get_mut(group_id) {
                    None => {
                        warn!(
                            "missing group {} at from_node {} fanout heartbeat response",
                            *group_id, msg.from_node
                        );
                        continue;
                    }
                    Some(group) => group,
                };

                activity_groups.insert(*group_id);

                let leader = match group.leader.as_ref() {
                    None => continue,
                    Some(leader) => leader,
                };

                if leader.node_id != msg.from_node || msg.from_node != self.node_id {
                    continue;
                }

                // gets the replica stored in this node.
                let from_replica_id = self
                    .storage
                    .replica_in_node(*group_id, msg.from_node)
                    .await
                    .unwrap()
                    .unwrap();
                let to_replica_id = self
                    .storage
                    .replica_in_node(*group_id, msg.to_node)
                    .await
                    .unwrap()
                    .unwrap();

                let mut msg = raft::prelude::Message::default();
                msg.set_msg_type(raft::prelude::MessageType::MsgHeartbeatResponse);
                msg.from = from_replica_id;
                msg.to = to_replica_id;
                group.raft_group.step(msg).unwrap();
            }
        } else {
            warn!(
                "missing raft groups at from_node {} fanout heartbeat response",
                msg.from_node
            );
            self.node_manager.add_node(msg.from_node, NO_GORUP);
        }
    }

    #[tracing::instrument(name = "MultiRaftActor::handle_manager_group_message", skip(self))]
    async fn handle_manager_group_message(
        &mut self,
        msg: RaftGroupManagementMessage,
        tx: oneshot::Sender<Result<(), Error>>,
        activity_groups: &mut HashSet<u64>,
    ) {
        // let mut activity_groups = vec![];
        let res = match msg.msg_type() {
            RaftGroupManagementMessageType::MsgInitialGroup => {
                activity_groups.insert(msg.group_id);
                self.initial_group(msg).await
            }
            RaftGroupManagementMessageType::MsgBootstrapGroup => {
                activity_groups.insert(msg.group_id);
                self.create_raft_group(msg.group_id, msg.replica_id).await
            }
            RaftGroupManagementMessageType::MsgRemoveGoup => todo!(),
        };

        if let Err(_error) = tx.send(res) {}
    }

    /// Initial the raft consensus group and start a replica in current node.
    async fn initial_group(&mut self, msg: RaftGroupManagementMessage) -> Result<(), Error> {
        assert_eq!(
            msg.msg_type(),
            RaftGroupManagementMessageType::MsgInitialGroup
        );

        if msg.group_id == 0 {
            return Err(Error::BadParameter(format!("bad group_id parameter (0)")));
        }

        if msg.replica_id == 0 {
            return Err(Error::BadParameter(format!("bad replica_id parameter (0)")));
        }

        if self.groups.contains_key(&msg.group_id) {
            return Err(Error::RaftGroupAlreayExists(msg.group_id));
        }

        // get the raft consensus group reated to storage, create if not exists.
        let group_storage = match self
            .storage
            .group_storage(msg.group_id, msg.replica_id)
            .await
            .map_err(|err| Error::Store(err))?
        {
            Some(gs) => gs,
            None => {
                let conf_state = (msg.initial_voters.clone(), vec![]); // TODO: learner
                self.storage
                    .create_group_storage_with_conf_state(msg.group_id, msg.group_id, conf_state)
                    .await
                    .map_err(|err| Error::Store(err))?
            }
        };

        // create raft consensus group with default logger and group storage.
        let applied = 0;
        let raft_cfg = raft::Config {
            id: msg.replica_id,
            applied,
            election_tick: self.election_tick,
            heartbeat_tick: self.heartbeat_tick,
            max_size_per_msg: 1024 * 1024,
            max_inflight_msgs: 256,
            ..Default::default()
        };

        let raft_store = group_storage.clone();
        let raft_group = raft::RawNode::with_default_logger(&raft_cfg, raft_store)
            .map_err(|err| Error::RaftGroup(err))?;

        // add group to node map
        self.node_manager.add_node(self.node_id, msg.group_id);

        // insert raft_group to group map
        let group = RaftGroup {
            group_id: msg.group_id,
            replica_id: msg.replica_id,
            raft_group,
            node_ids: vec![self.node_id],
            proposals: GroupProposalQueue::new(msg.replica_id),
            leader: None,
        };
        self.groups.insert(msg.group_id, group);

        Ok(())
    }

    /// Create a replica of the raft consensus group on this node.
    #[tracing::instrument(name = "MultiRaftActor::bootstrap_group", skip(self))]
    async fn create_raft_group(&mut self, group_id: u64, replica_id: u64) -> Result<(), Error> {
        if group_id == 0 {
            return Err(Error::BadParameter(format!("bad group_id parameter (0)")));
        }

        if replica_id == 0 {
            return Err(Error::BadParameter(format!("bad replica_id parameter (0)")));
        }

        if self.groups.contains_key(&group_id) {
            return Err(Error::RaftGroupAlreayExists(group_id));
        }

        // let mut replica_id = 0;
        // get the raft consensus group reated to storage, create if not exists.
        let group_storage = match self
            .storage
            .group_storage(group_id, replica_id)
            .await
            .map_err(|err| Error::Store(err))?
        {
            Some(gs) => gs,
            None => return Err(Error::Store(StorageError::Unavailable)),
        };

        let rs = group_storage
            .initial_state()
            .map_err(|err| Error::Store(err))?;

        let voters = rs.conf_state.voters;

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
        let raft_group = raft::RawNode::with_default_logger(&raft_cfg, raft_store)
            .map_err(|err| Error::RaftGroup(err))?;

        let mut group = RaftGroup {
            group_id,
            replica_id,
            raft_group,
            node_ids: Vec::new(),
            proposals: GroupProposalQueue::new(replica_id),
            leader: None,
        };

        for voter_id in voters.iter() {
            let replica_metadata = self
                .storage
                .replica_metadata(group_id, *voter_id)
                .await
                .map_err(|err| Error::Store(err))?;

            // at this point, we don't know the infomation about
            // the node which replica.
            let replica_metadata = match replica_metadata {
                None => continue,
                Some(replica_metadata) => replica_metadata,
            };
            if replica_metadata.node_id == NO_NODE {
                continue;
            }

            // track the nodes which other members of the raft consensus group
            group.node_ids.push(replica_metadata.node_id);
            self.node_manager
                .add_node(replica_metadata.node_id, group_id);
        }
        self.groups.insert(group_id, group);

        Ok(())
    }

    // fn add_node(&mut self, node_id: u64, group_id: u64) {
    //     let node = match self.nodes.get_mut(&node_id) {
    //         None => {
    //             self.nodes.insert(
    //                 node_id,
    //                 Node {
    //                     node_id,
    //                     group_map: HashMap::new(),
    //                 },
    //             );
    //             self.nodes.get_mut(&node_id).unwrap()
    //         }
    //         Some(node) => node,
    //     };

    //     if group_id != 0 {
    //         node.group_map.insert(group_id, ());
    //     }
    // }

    fn handle_write_request(
        &mut self,
        request: AppWriteRequest,
        tx: oneshot::Sender<Result<(), Error>>,
    ) {
        let group_id = request.group_id;
        let group = self.groups.get_mut(&group_id).unwrap();
        group.write_propose(request, tx);
    }

    fn handle_read_index_request(
        &mut self,
        request: AppReadIndexRequest,
        tx: oneshot::Sender<Result<(), Error>>
    ) {
        let group_id = request.group_id;
        let group = self.groups.get_mut(&group_id).unwrap();
        group.read_index_propose(request, tx);
    }

    async fn handle_apply_task_response(&mut self, response: ApplyTaskResponse) {
        for (group_id, results) in response.groups {
            let group = match self.groups.get_mut(&group_id) {
                Some(group) => group,
                None => {
                    warn!("group {} removed, skip apply", group_id);
                    continue;
                }
            };

            for res in results {
                match res {
                    ApplyResult::MembershipChange(result) => {
                        MultiRaftActor::<MI, T, RS, MRS>::apply_membership_change(
                            group,
                            &mut self.node_manager,
                            &mut self.replica_metadata_cache,
                            result,
                        )
                    }
                }
            }

            group.raft_group.advance_apply();
        }
    }

    fn apply_membership_change(
        group: &mut RaftGroup<RS>,
        node_mgr: &mut NodeManager,
        replica_metadata_cache: &mut ReplicaMetadataCache<RS, MRS>,
        result: MembershipChangeResult,
    ) {
        for change in result.changes.iter() {
            match change.change_type() {
                crate::proto::ConfChangeType::AddNode => {
                    let replica_metadata = ReplicaMetadata {
                        node_id: change.node_id,
                        replica_id: change.replica_id,
                        store_id: 0, // TODO: remove it
                    };
                    node_mgr.add_node(change.node_id, change.group_id);
                    replica_metadata_cache.cache(change.group_id, replica_metadata);
                }
                crate::proto::ConfChangeType::RemoveNode => unimplemented!(),
                crate::proto::ConfChangeType::AddLearnerNode => unimplemented!(),
            }
        }

        let cs = group
            .raft_group
            .apply_conf_change(&result.conf_change)
            .unwrap();
    }
}

// #[tokio::test(flavor = "multi_thread")]
// async fn test_bootstrap_group() {
//     // tracing_subscriber::fmt::try_init().unwrap();
//     let nodes = 3;
//     let groups = 3;
//     let replicas = 3;
//     let mut multi_rafts = vec![];

//     for node_id in 0..nodes {
//         let store_id = node_id;
//         let storage = MultiRaftGroupMemoryStorage::new(node_id, store_id);
//         let group_cfg = GroupConfig {
//             election_tick: 10,
//             heartbeat_tick: 3,
//             tick_interval: 100,
//         };
//         let multi_raft = MultiRaft::new(group_cfg, node_id, store_id, storage);
//         multi_rafts.push(multi_raft);
//     }

//     let mut next_replica = 1;
//     for (_i, multi_raft) in multi_rafts.iter().enumerate() {
//         for group_id in 1..groups + 1 {
//             let mut msg = RaftGroupManagementMessage::default();
//             msg.set_msg_type(RaftGroupManagementMessageType::MsgInitialGroup);
//             msg.initial_voters = (1..replicas + 1).collect();
//             msg.group_id = group_id;
//             msg.replica_id = next_replica;
//             multi_raft.initial_raft_group(msg).await.unwrap();
//         }
//         next_replica += 1;
//     }

//     tokio::time::sleep(Duration::from_secs(1000)).await;
// }
