use std::collections::hash_map::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::time::Duration;

use prost::Message as ProstMessage;
use raft::LightReady;
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

use super::apply::Apply;
use super::apply::ApplyActor;
use super::apply::ApplyActorAddress;
use super::apply::ApplyResult;
use super::apply::ApplyTask;
use super::apply::ApplyTaskRequest;
use super::apply::ApplyTaskResponse;
use super::apply::MembershipChangeResult;
use super::config::MultiRaftConfig;
use super::error::Error;
use super::error::ProposalError;
use super::event::Event;
use super::event::LeaderElectionEvent;
use super::multiraft::NO_GORUP;
use super::multiraft::NO_NODE;
use super::node::NodeManager;
use super::proposal::GroupProposalQueue;
use super::proposal::Proposal;
use super::proposal::ProposalQueueManager;
use super::proposal::ReadIndexProposal;
use super::raft_group::RaftGroup;
use super::replica_cache::ReplicaCache;
use super::transport;
use super::transport::MessageInterface;
use super::transport::Transport;

use crate::proto::transmute_entries;
use crate::proto::transmute_raft_entries;
use crate::proto::transmute_raft_entries_ref;
use crate::proto::transmute_raft_hard_state;
use crate::proto::transmute_raft_messages;
use crate::proto::transmute_raft_snapshot;
use crate::proto::AppReadIndexRequest;
use crate::proto::AppReadIndexResponse;
use crate::proto::AppWriteRequest;
use crate::proto::AppWriteResponse;
use crate::proto::ConfState;
use crate::proto::Entry;
use crate::proto::Message;
use crate::proto::MessageType;
use crate::proto::RaftGroupManagementMessage;
use crate::proto::RaftGroupManagementMessageType;
use crate::proto::RaftMessage;
use crate::proto::RaftMessageResponse;
use crate::proto::ReplicaDesc;
use crate::proto::Snapshot;
use crate::storage::transmute_message;

use crate::storage::MultiRaftStorage;
use crate::storage::RaftStorage;
use crate::storage::RaftStorageImpl;
use crate::storage::StorageError;
// use crate::proto::Error;

#[derive(Default, Debug)]
pub struct GroupWriteRequest {
    replica_id: u64,
    ready: Option<Ready>,
    light_ready: Option<LightReady>,
}

/// MultiRaftAddress is used to communicate with MultiRaftActor
#[derive(Clone)]
pub struct MultiRaftActorAddress {
    pub write_propose_tx: Sender<(AppWriteRequest, oneshot::Sender<Result<(), Error>>)>,
    pub read_index_propose_tx: Sender<(AppReadIndexRequest, oneshot::Sender<Result<(), Error>>)>,
    pub campagin_tx: Sender<u64>,
    pub raft_message_tx: Sender<(
        RaftMessage,
        oneshot::Sender<Result<RaftMessageResponse, Error>>,
    )>,
    pub manager_group_tx: Sender<(
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
    write_propose_rx: Receiver<(AppWriteRequest, oneshot::Sender<Result<(), Error>>)>,
    read_index_propose_rx: Receiver<(AppReadIndexRequest, oneshot::Sender<Result<(), Error>>)>,
    raft_message_rx: Receiver<(
        RaftMessage,
        oneshot::Sender<Result<RaftMessageResponse, Error>>,
    )>,

    campagin_rx: Receiver<u64>,

    manager_group_rx: Receiver<(
        RaftGroupManagementMessage,
        oneshot::Sender<Result<(), Error>>,
    )>,

    pending_events: Vec<Event>,
    event_tx: Sender<Vec<Event>>,
    // write_actor_address: WriteAddress,
    apply_actor_address: ApplyActorAddress,
    storage: MRS,
    transport: T,
    // waiting_ready_groups: VecDeque<HashMap<u64, Ready>>,
    // proposals: ProposalQueueManager,
    replica_cache: ReplicaCache<RS, MRS>,
    sync_replica_cache: bool,
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
        cfg: &MultiRaftConfig,
        node_id: u64,
        store_id: u64,
        transport: T,
        apply_actor_address: ApplyActorAddress,
        event_tx: Sender<Vec<Event>>,
        storage: MRS,
        stop: watch::Receiver<bool>,
    ) -> (JoinHandle<()>, MultiRaftActorAddress) {
        let (raft_message_tx, raft_message_rx) = channel(1);
        let (campagin_tx, campagin_rx) = channel(1);
        let (manager_group_tx, manager_group_rx) = channel(1);

        // let (write_actor_join, write_actor_address) =
        //     WriterActor::spawn(storage.clone(), stop.clone());

        // create write propose channel
        let (write_propose_tx, write_propose_rx) = channel(1);
        let (read_index_propose_tx, read_index_propose_rx) = channel(1);

        let actor = MultiRaftActor {
            store_id,
            node_id,
            // nodes: HashMap::new(),
            node_manager: NodeManager::new(),
            event_tx,
            groups: HashMap::new(),
            tick_interval: Duration::from_millis(cfg.tick_interval),
            election_tick: cfg.election_tick,
            heartbeat_tick: cfg.heartbeat_tick,
            write_propose_rx,
            read_index_propose_rx,
            campagin_rx,
            raft_message_rx,
            manager_group_rx,
            storage: storage.clone(),
            transport,
            // write_actor_address,
            apply_actor_address,
            sync_replica_cache: true,
            replica_cache: ReplicaCache::new(storage.clone()),
            pending_events: Vec::new(),
            // waiting_ready_groups: VecDeque::default(),
            _m1: PhantomData,
            _m2: PhantomData,
        };

        let main_loop = async move {
            actor.start(stop).await;
        };

        let join = tokio::spawn(main_loop);

        let address = MultiRaftActorAddress {
            campagin_tx,
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
            // handle events
            if !self.pending_events.is_empty() {
                for pending_event in self.pending_events.iter_mut() {
                    match pending_event {
                        Event::LederElection(leader_elect) => {
                            let group = self.groups.get(&leader_elect.group_id).unwrap();
                            if leader_elect.committed_term != group.committed_term
                                && group.committed_term != 0
                            {
                                leader_elect.committed_term = group.committed_term;
                            }
                        }
                        _ => {}
                    }
                }

                let pending_events = std::mem::take(&mut self.pending_events);

                self.event_tx.send(pending_events).await.unwrap();
            }

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

                Some((msg, tx)) = self.raft_message_rx.recv() => self.handle_raft_message(msg, tx, &mut activity_groups).await,

                Some(group_id) = self.campagin_rx.recv() => self.campagin_raft(group_id).await,

                Some((request, tx)) = self.write_propose_rx.recv() => self.handle_write_request(request, tx),

                Some((request, tx)) = self.read_index_propose_rx.recv() => self.handle_read_index_request(request, tx),

                Some((msg, tx)) = self.manager_group_rx.recv() => {
                    self.handle_manager_group_message(msg, tx, &mut activity_groups).await;
                },

                else => {
                    self.on_groups_ready(&activity_groups).await;
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
        tx: oneshot::Sender<Result<RaftMessageResponse, Error>>,
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

        let from_replica = ReplicaDesc {
            node_id: msg.from_node,
            replica_id: raft_msg.from,
        };

        let to_replica = ReplicaDesc {
            node_id: msg.to_node,
            replica_id: raft_msg.to,
        };

        if let Err(err) = self
            .replica_cache
            .cache_replica_desc(group_id, from_replica.clone(), self.sync_replica_cache)
            .await
        {
            tx.send(Err(err)).unwrap();
            return;
        }

        if let Err(err) = self
            .replica_cache
            .cache_replica_desc(group_id, to_replica.clone(), self.sync_replica_cache)
            .await
        {
            tx.send(Err(err)).unwrap();
            return;
        }

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

                if group.leader.node_id == 0
                    || group.leader.node_id != msg.from_node
                    || msg.from_node == self.node_id
                {
                    continue;
                }

                // gets the replica stored in this node.
                let from_replica = match self
                    .storage
                    .replica_for_node(*group_id, msg.from_node)
                    .await
                {
                    Err(err) => {
                        warn!(
                            "find replcia in group {} on from_node {} in current node {} error: {}",
                            group_id, msg.from_node, self.node_id, err
                        );
                        continue;
                    }
                    Ok(val) => match val {
                        None => {
                            warn!("the current node {} that look up replcia not found in group {} on from_node {}", self.node_id, group_id, msg.from_node);
                            continue;
                        }
                        Some(val) => val,
                    },
                };

                let to_replica = match self.storage.replica_for_node(*group_id, msg.to_node).await {
                    Err(err) => {
                        warn!(
                            "find replcia in group {} on to_node {} in current node {} error: {}",
                            group_id, msg.to_node, self.node_id, err
                        );
                        continue;
                    }
                    Ok(val) => match val {
                        None => {
                            warn!("the current node {} that look up replcia not found in group {} on to_node {}", self.node_id, group_id, msg.to_node);
                            continue;
                        }
                        Some(val) => val,
                    },
                };

                fanouted_followers += 1;

                let mut msg = raft::prelude::Message::default();
                msg.set_msg_type(raft::prelude::MessageType::MsgHeartbeat);
                msg.from = from_replica.replica_id;
                msg.to = to_replica.replica_id;
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

                // let leader = match group.leader.as_ref() {
                //     None => continue,
                //     Some(leader) => leader,
                // };

                // TODO: check leader is valid
                if group.leader.node_id != msg.from_node || msg.from_node == self.node_id {
                    continue;
                }

                // gets the replica stored in this node.
                let from_replica = match self
                    .storage
                    .replica_for_node(*group_id, msg.from_node)
                    .await
                {
                    Err(err) => {
                        warn!(
                            "find replcia in group {} on from_node {} in current node {} error: {}",
                            group_id, msg.from_node, self.node_id, err
                        );
                        continue;
                    }
                    Ok(val) => match val {
                        None => {
                            warn!("the current node {} that look up replcia not found in group {} on from_node {}", self.node_id, group_id, msg.from_node);
                            continue;
                        }
                        Some(val) => val,
                    },
                };

                let to_replica = match self.storage.replica_for_node(*group_id, msg.to_node).await {
                    Err(err) => {
                        warn!(
                            "find replcia in group {} on to_node {} in current node {} error: {}",
                            group_id, msg.to_node, self.node_id, err
                        );
                        continue;
                    }
                    Ok(val) => match val {
                        None => {
                            warn!("the current node {} that look up replcia not found in group {} on to_node {}", self.node_id, group_id, msg.to_node);
                            continue;
                        }
                        Some(val) => val,
                    },
                };

                let mut msg = raft::prelude::Message::default();
                msg.set_msg_type(raft::prelude::MessageType::MsgHeartbeatResponse);
                msg.from = from_replica.replica_id;
                msg.to = to_replica.replica_id;
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

    async fn campagin_raft(&mut self, group_id: u64) {
        if let Some(group) = self.groups.get_mut(&group_id) {
            group.raft_group.campaign().unwrap()
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
            RaftGroupManagementMessageType::MsgCreateGroup => {
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
        let gs = self
            .storage
            .group_storage(msg.group_id, msg.replica_id)
            .await
            .map_err(|err| Error::Store(err))?;

        for replica_metadata in msg.replicas.into_iter() {
            if replica_metadata.node_id != NO_NODE {
                self.node_manager
                    .add_node(replica_metadata.node_id, msg.group_id);
                self.replica_cache
                    .cache_replica_desc(msg.group_id, replica_metadata, self.sync_replica_cache)
                    .await
                    .unwrap();
            }
        }

        // set conf state
        // let mut cs = ConfState::default();
        // cs.voters = msg.initial_voters.clone();
        // let _ = gs.set_confstate(cs).map_err(|err| Error::Store(err))?;
        // let snapshot = Snapshot::default();
        // snapshot.mut_metadata().mut_conf_state().voters = msg.initial_voters.clone();

        // let group_storage = match self
        //     .storage
        //     .group_storage(msg.group_id, msg.replica_id)
        //     .await
        //     .map_err(|err| Error::Store(err))?
        // {
        //     Some(gs) => gs,
        //     None => {
        //         let conf_state = (msg.initial_voters.clone(), vec![]); // TODO: learner
        //         self.storage
        //             .create_group_storage_with_conf_state(msg.group_id, msg.group_id, conf_state)
        //             .await
        //             .map_err(|err| Error::Store(err))?
        //     }
        // };

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

        let raft_store = gs.clone();
        let raft_group = raft::RawNode::with_default_logger(&raft_cfg, raft_store)
            .map_err(|err| Error::RaftGroup(err))?;

        // add group to node map
        self.node_manager.add_node(self.node_id, msg.group_id);

        // insert raft_group to group map
        let group = RaftGroup {
            group_id: msg.group_id,
            replica_id: msg.replica_id,
            raft_group,
            committed_term: 0, // TODO: init committed term
            node_ids: vec![self.node_id],
            proposals: GroupProposalQueue::new(msg.replica_id),
            leader: ReplicaDesc::default(),
        };
        self.groups.insert(msg.group_id, group);

        Ok(())
    }

    /// Create a replica of the raft consensus group on this node.
    #[tracing::instrument(name = "MultiRaftActor::bootstrap_group", skip(self))]
    async fn create_raft_group(&mut self, group_id: u64, replica_id: u64) -> Result<(), Error> {
        if self.groups.contains_key(&group_id) {
            return Err(Error::RaftGroupAlreayExists(group_id));
        }

        if group_id == 0 {
            return Err(Error::BadParameter(format!("bad group_id parameter (0)")));
        }

        if replica_id == 0 {
            return Err(Error::BadParameter(format!("bad replica_id parameter (0)")));
        }

        // let mut replica_id = 0;
        // get the raft consensus group reated to storage, create if not exists.
        let group_storage = self
            .storage
            .group_storage(group_id, replica_id)
            .await
            .map_err(|err| Error::Store(err))?;

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
            leader: ReplicaDesc::default(), // TODO: init leader from storage
            committed_term: 0,              // TODO: init committed term from storage
        };

        for voter_id in voters.iter() {
            let replica_desc = self
                .storage
                .replica_desc(group_id, *voter_id)
                .await
                .map_err(|err| Error::Store(err))?
                .unwrap();

            // at this point, we don't know the infomation about
            // the node which replica.
            // let replica_metadata = match replica_metadata {
            //     None => continue,
            //     Some(replica_metadata) => replica_metadata,
            // };
            if replica_desc.node_id == NO_NODE {
                continue;
            }

            // track the nodes which other members of the raft consensus group
            group.node_ids.push(replica_desc.node_id);
            self.node_manager.add_node(replica_desc.node_id, group_id);
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
        tx: oneshot::Sender<Result<(), Error>>,
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
                            &mut self.replica_cache,
                            self.sync_replica_cache,
                            result,
                        )
                        .await;
                    }
                }
            }

            group.raft_group.advance_apply();
        }
    }

    async fn apply_membership_change(
        group: &mut RaftGroup<RS>,
        node_mgr: &mut NodeManager,
        replica_cache: &mut ReplicaCache<RS, MRS>,
        sync_replica_cache: bool,
        result: MembershipChangeResult,
    ) {
        for change in result.changes.iter() {
            match change.change_type() {
                crate::proto::ConfChangeType::AddNode => {
                    // TODO: this call need transfer to user call, and if user call return errored,
                    // the membership change should failed and user need to retry.
                    // we need a channel to provider user notify actor it need call these code.
                    // and we recv the notify can executing these code, if executed failed, we
                    // response to user and membership change is failed.
                    let replica_metadata = ReplicaDesc {
                        node_id: change.node_id,
                        replica_id: change.replica_id,
                    };
                    node_mgr.add_node(change.node_id, change.group_id);
                    replica_cache
                        .cache_replica_desc(change.group_id, replica_metadata, sync_replica_cache)
                        .await
                        .unwrap();
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

    pub(crate) async fn on_groups_ready(&mut self, activity_groups: &HashSet<u64>) {
        // let mut ready_groups = HashMap::new();
        let mut ready_write_groups = HashMap::new();
        let mut apply_task_groups = HashMap::new();
        for group_id in activity_groups.iter() {
            if *group_id == NO_GORUP {
                continue;
            }

            // let group = match self.groups.get_mut(group_id) {
            //     None => {
            //         warn!(
            //             "write is bad, {} group dropped in {} node",
            //             group_id, self.node_id
            //         );
            //         continue;
            //     }
            //     Some(group) => group,
            // };

            if let Some(group) = self.groups.get_mut(group_id) {
                if !group.raft_group.has_ready() {
                    continue;
                }

                let mut group_ready = group.raft_group.ready();

                // we need to know which replica in raft group is ready.
                let replica_id = match self.storage.replica_for_node(*group_id, self.node_id).await
                {
                    Err(error) => {
                        error!(
                            "write is error, got {} group replica  of storage error {}",
                            group_id, error
                        );
                        continue;
                    }
                    Ok(replica_desc) => match replica_desc {
                        Some(replica_desc) => replica_desc.replica_id,
                        None => {
                            // if we can't look up the replica in storage, but the group is ready,
                            // we know that one of the replicas must be ready, so we can repair the
                            // storage to store this replica.
                            let replica_id = group.raft_group.raft.id;
                            // TODO: store replica id
                            replica_id
                        }
                    },
                };

                if let Some(ss) = group_ready.ss() {
                    if ss.leader_id != 0 && ss.leader_id != group.leader.replica_id {
                        let replica_desc = self
                            .replica_cache
                            .replica_desc(*group_id, ss.leader_id)
                            .await
                            .unwrap()
                            .unwrap();
                        group.leader = replica_desc;
                        self.pending_events
                            .push(Event::LederElection(LeaderElectionEvent {
                                group_id: *group_id,
                                leader_id: ss.leader_id,
                                committed_term: group.committed_term,
                            }))
                    }
                }

                // send out messages
                if !group_ready.messages().is_empty() {
                    transport::send_messages(
                        self.node_id,
                        &self.storage,
                        &self.transport,
                        &mut self.node_manager,
                        *group_id,
                        transmute_raft_messages(group_ready.take_messages()),
                    )
                    .await;
                }

                // make apply task if need to apply commit entries
                if !group_ready.committed_entries().is_empty() {
                    let last_term = group_ready.committed_entries()
                        [group_ready.committed_entries().len() - 1]
                        .term;
                    group.maybe_update_committed_term(last_term);

                    let entries = transmute_raft_entries(group_ready.take_committed_entries());
                    let apply =
                        MultiRaftActor::<MI, T, RS, MRS>::create_apply(replica_id, group, entries);

                    apply_task_groups.insert(*group_id, ApplyTask::Apply(apply));
                }

                // make write task if need to write disk.
                ready_write_groups.insert(
                    *group_id,
                    GroupWriteRequest {
                        replica_id,
                        ready: Some(group_ready),
                        light_ready: None,
                    },
                );
            }
        }

        if !apply_task_groups.is_empty() {
            self.apply_actor_address
                .tx
                .send(ApplyTaskRequest {
                    groups: apply_task_groups,
                })
                .await;
        }

        let gwrs = self.handle_write(ready_write_groups).await;
        self.handle_write_finish(gwrs).await;
    }

    async fn handle_write(
        &mut self,
        mut ready_write_groups: HashMap<u64, GroupWriteRequest>,
    ) -> HashMap<u64, GroupWriteRequest> {
        // TODO(yuanchang.xu) Disk write flow control
        // let mut light_readys = HashMap::new();
        for (group_id, group_write_request) in ready_write_groups.iter_mut() {
            let group = self.groups.get_mut(&group_id).unwrap();
            let gs = match self
                .storage
                .group_storage(*group_id, group_write_request.replica_id)
                .await
            {
                Ok(gs) => gs,
                Err(error) => {
                    continue;
                }
            };

            let mut ready = group_write_request.ready.take().unwrap();
            if *ready.snapshot() != raft::prelude::Snapshot::default() {
                let snapshot = ready.snapshot().clone();
                if let Err(_error) = gs.apply_snapshot(transmute_raft_snapshot(snapshot)) {}
            }

            if !ready.entries().is_empty() {
                let entries = ready.take_entries();
                if let Err(_error) = gs.append_entries(transmute_raft_entries_ref(&entries)) {}
            }

            if let Some(hs) = ready.hs() {
                let hs = transmute_raft_hard_state(hs.clone());
                if let Err(_error) = gs.set_hardstate(hs) {}
            }

            if !ready.persisted_messages().is_empty() {
                let persistent_msgs = ready.take_persisted_messages();
                transport::send_messages(
                    self.node_id,
                    &self.storage,
                    &self.transport,
                    &mut self.node_manager,
                    *group_id,
                    transmute_raft_messages(persistent_msgs),
                )
                .await;
            }

            let light_ready = group.raft_group.advance(ready);
            group_write_request.light_ready = Some(light_ready);
        }

        ready_write_groups
    }

    async fn handle_write_finish(&mut self, ready_groups: HashMap<u64, GroupWriteRequest>) {
        let mut apply_task_groups = HashMap::new();
        for (group_id, mut gwr) in ready_groups.into_iter() {
            let mut_group = match self.groups.get_mut(&group_id) {
                None => continue,
                Some(g) => g,
            };

            let mut light_ready = gwr.light_ready.take().unwrap();
            let group_storage = self
                .storage
                .group_storage(group_id, gwr.replica_id)
                .await
                .unwrap();

            if let Some(commit) = light_ready.commit_index() {
                group_storage.set_commit(commit);
            }

            if !light_ready.messages().is_empty() {
                let messages = light_ready.take_messages();
                transport::send_messages(
                    self.node_id,
                    &self.storage,
                    &self.transport,
                    &mut self.node_manager,
                    group_id,
                    transmute_raft_messages(messages),
                )
                .await;
            }

            if !light_ready.committed_entries().is_empty() {
                let last_term =
                    light_ready.committed_entries()[light_ready.committed_entries().len() - 1].term;
                mut_group.maybe_update_committed_term(last_term);

                let entries = transmute_raft_entries(light_ready.take_committed_entries());
                let apply = MultiRaftActor::<MI, T, RS, MRS>::create_apply(
                    gwr.replica_id,
                    mut_group,
                    entries,
                );

                apply_task_groups.insert(group_id, ApplyTask::Apply(apply));
            }
        }

        if !apply_task_groups.is_empty() {
            self.apply_actor_address
                .tx
                .send(ApplyTaskRequest {
                    groups: apply_task_groups,
                })
                .await;
        }
    }

    fn create_apply(replica_id: u64, group: &mut RaftGroup<RS>, entries: Vec<Entry>) -> Apply {
        let current_term = group.raft_group.raft.term;
        let commit_index = group.raft_group.raft.raft_log.committed;
        let mut proposals = VecDeque::new();
        if !proposals.is_empty() {
            for entry in entries.iter() {
                match group
                    .proposals
                    .find_proposal(entry.term, entry.index, current_term)
                {
                    Err(error) => {
                        continue;
                    }
                    Ok(proposal) => match proposal {
                        None => continue,

                        Some(p) => proposals.push_back(p),
                    },
                };
            }
        }

        let entries_size = entries
            .iter()
            .map(|ent| ent.compute_size() as usize)
            .sum::<usize>();
        Apply {
            replica_id,
            group_id: group.group_id,
            term: current_term,
            commit_index,
            commit_term: 0, // TODO: get commit term
            entries,
            entries_size,
            proposals,
        }
    }
}
