use std::collections::hash_map::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use raft::prelude::AdminMessage;
use raft::prelude::AdminMessageType;
use raft::prelude::RaftGroupManagement;
use raft::prelude::RaftGroupManagementType;
use raft::LightReady;
use raft::Ready;
use raft::Storage;

use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::task::JoinError;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::multiraft::error::RaftGroupError;
use crate::util::TaskGroup;

use super::apply_actor::Apply;
use super::apply_actor::ApplyActorReceiver;
use super::apply_actor::ApplyActorSender;
use super::apply_actor::ApplyTask;
use super::apply_actor::ApplyTaskRequest;
use super::apply_actor::ApplyTaskResponse;
use super::config::Config;
use super::error::Error;
use super::event::CallbackEvent;
use super::event::Event;
use super::event::LeaderElectionEvent;
use super::event::MembershipChangeView;
use super::multiraft::NO_GORUP;
use super::multiraft::NO_NODE;
use super::node::NodeManager;
use super::proposal::GroupProposalQueue;
use super::raft_group::RaftGroup;
use super::replica_cache::ReplicaCache;
use super::transport;
use super::transport::Transport;
use super::util;

use raft_proto::prelude::AppReadIndexRequest;
use raft_proto::prelude::AppWriteRequest;
use raft_proto::prelude::ConfChangeType;
use raft_proto::prelude::Entry;
use raft_proto::prelude::Message;
use raft_proto::prelude::MessageType;
use raft_proto::prelude::RaftMessage;
use raft_proto::prelude::RaftMessageResponse;
use raft_proto::prelude::ReplicaDesc;

use super::storage::MultiRaftStorage;

#[derive(Default, Debug)]
pub struct GroupWriteRequest {
    replica_id: u64,
    ready: Option<Ready>,
    light_ready: Option<LightReady>,
}

/// MultiRaftAddress is used to communicate with MultiRaftActor
#[derive(Clone)]
pub struct MultiRaftActorSender {
    pub write_propose_tx: Sender<(AppWriteRequest, oneshot::Sender<Result<(), Error>>)>,
    pub read_index_propose_tx: Sender<(AppReadIndexRequest, oneshot::Sender<Result<(), Error>>)>,
    pub campagin_tx: Sender<u64>,
    pub raft_message_tx: Sender<(
        RaftMessage,
        oneshot::Sender<Result<RaftMessageResponse, Error>>,
    )>,
    pub admin_tx: Sender<(AdminMessage, oneshot::Sender<Result<(), Error>>)>,
}

// pub struct MultiRaftActorContext<RS: Storage, MRS: MultiRaftStorage<RS>> {
//     pub node_manager: NodeManager,
//     pub groups: HashMap<u64, RaftGroup<RS>>,
//     pub replica_cache: ReplicaCache<RS, MRS>,
//     pub sync_replica_cache: bool,
// }

struct Shard {
    stopped: AtomicBool,
}

impl Shard {
    fn new() -> Self {
        Self {
            stopped: AtomicBool::new(false),
        }
    }
}

/// MultiRaftActorContext is used to safely shard the state 
/// of an actor between threads.
pub struct MultiRaftActorContext {
    shard: Arc<Shard>,
}

impl MultiRaftActorContext {
    pub fn new() -> Self {
        Self {
            shard: Arc::new(Shard::new()),
        }
    }

    /// `true` if actor stop.
    pub fn stopped(&self) -> bool {
        self.shard.stopped.load(Ordering::Acquire)
    }
}

impl Clone for MultiRaftActorContext {
    fn clone(&self) -> Self {
        Self {
            shard: self.shard.clone(),
        }
    }
}

pub struct MultiRaftActor {
    join: JoinHandle<()>,
    ctx: MultiRaftActorContext,
}

impl MultiRaftActor {
    /// clone and return `MultiRaftActorContext`.
    pub fn context(&self) -> MultiRaftActorContext {
        self.ctx.clone()
    }

    /// join take onwership to join actor.
    async fn join(self) -> Result<(), JoinError> {
        self.join.await
    }
}

/// spawn a multi raft actor.
pub fn spawn<TR, RS, MRS>(
    config: &Config,
    transport: TR,
    storage: MRS,
    event_tx: Sender<Vec<Event>>,
    callback_event_rx: Receiver<CallbackEvent>,
    apply_actor_tx: ApplyActorSender,
    apply_actor_rx: ApplyActorReceiver,
    task_group: TaskGroup,
) -> (MultiRaftActor, MultiRaftActorSender)
where
    TR: Transport,
    RS: Storage + Send + Sync + Clone + 'static,
    MRS: MultiRaftStorage<RS>,
{
    // write channel
    let (write_propose_tx, write_propose_rx) = channel(1);
    // read_index channel
    let (read_index_propose_tx, read_index_propose_rx) = channel(1);
    // admin cahnnel
    let (admin_tx, admin_rx) = channel(1);
    // cmapgin channel use for harness test.
    let (campagin_tx, campagin_rx) = channel(1);
    // raft_message channel forward RaftMessage.
    let (raft_message_tx, raft_message_rx) = channel(1);

    let ctx = MultiRaftActorContext::new();

    let actor_inner = MultiRaftActorInner {
        ctx: ctx.clone(),
        node_id: config.node_id,
        node_manager: NodeManager::new(),
        event_tx,
        callback_event_rx,
        groups: HashMap::new(),
        tick_interval: Duration::from_millis(config.tick_interval),
        election_tick: config.election_tick,
        heartbeat_tick: config.heartbeat_tick,
        write_propose_rx,
        read_index_propose_rx,
        campagin_rx,
        raft_message_rx,
        admin_rx,
        storage: storage.clone(),
        transport,
        apply_actor_tx,
        apply_actor_rx,
        sync_replica_cache: true,
        replica_cache: ReplicaCache::new(storage.clone()),
        pending_events: Vec::new(),
        task_group: task_group.clone(),
        _m1: PhantomData,
    };

    let main_loop = async move {
        actor_inner.start().await;
    };

    let join = task_group.spawn(main_loop);

    let actor = MultiRaftActor { join, ctx };

    let address = MultiRaftActorSender {
        campagin_tx,
        raft_message_tx,
        admin_tx,
        write_propose_tx,
        read_index_propose_tx,
    };

    (actor, address)
}

struct MultiRaftActorInner<T, RS, MRS>
where
    T: Transport,
    RS: Storage,
    MRS: MultiRaftStorage<RS>,
{
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

    admin_rx: Receiver<(AdminMessage, oneshot::Sender<Result<(), Error>>)>,

    pending_events: Vec<Event>,
    event_tx: Sender<Vec<Event>>,
    callback_event_rx: Receiver<CallbackEvent>,
    // write_actor_address: WriteAddress,
    apply_actor_tx: ApplyActorSender,
    apply_actor_rx: ApplyActorReceiver,
    storage: MRS,
    transport: T,
    // waiting_ready_groups: VecDeque<HashMap<u64, Ready>>,
    // proposals: ProposalQueueManager,
    replica_cache: ReplicaCache<RS, MRS>,
    sync_replica_cache: bool,
    task_group: TaskGroup,
    ctx: MultiRaftActorContext,
    _m1: PhantomData<RS>,
}

impl<T, RS, MRS> MultiRaftActorInner<T, RS, MRS>
where
    T: Transport,
    RS: Storage + Send + Sync + Clone + 'static,
    MRS: MultiRaftStorage<RS>,
{
    #[tracing::instrument(name = "MultiRaftActor::start", skip(self))]
    async fn start(mut self) {
        info!("node ({}) multiraft actor start", self.node_id);
        // Each time ticker expires, the ticks increments,
        // when ticks >= heartbeat_tick triggers the merged heartbeat.
        let mut ticks = 0;
        let mut ticker =
            tokio::time::interval_at(Instant::now() + self.tick_interval, self.tick_interval);
        let mut activity_groups = HashSet::new();

        let mut ready_ticker = tokio::time::interval_at(
            Instant::now() + Duration::from_millis(10),
            Duration::from_millis(10),
        );

        let mut stopper = self.task_group.stopper();
        loop {
            if !self.pending_events.is_empty() {
                info!("send pending evnets");
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
                // Note: see https://github.com/tokio-rs/tokio/discussions/4019 for more
                // information about why mut here.
                _ = &mut stopper => {
                    info!("MultiRaftActor receive stop notify");
                    break
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

                // handle application write request
                Some((request, tx)) = self.write_propose_rx.recv() => self.handle_write_request(request, tx),

                // handle application read index request.
                Some((request, tx)) = self.read_index_propose_rx.recv() => self.handle_read_index_request(request, tx),

                Some((msg, tx)) = self.admin_rx.recv() => {
                    self.handle_admin_request(msg, tx, &mut activity_groups).await;
                },

                // handle apply actor response.
                Some(response) = self.apply_actor_rx.rx.recv() => self.handle_apply_task_response(response).await,

                // handle application callback event.
                Some(msg) = self.callback_event_rx.recv() => {
                    self.handle_callback(msg).await;
                },

                _ = ready_ticker.tick() => {
                    self.on_groups_ready(&activity_groups).await;
                    activity_groups.clear();
                }
            }
        }

        self.ctx.shard.stopped.store(true, Ordering::Release);
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
    #[tracing::instrument(
        name = "MultiRaftActor::handle_raft_message",
        skip(self, tx, activity_groups)
    )]
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
            error!("cachc replica description error: {}", err);
            tx.send(Err(err)).unwrap();
            return;
        }

        if let Err(err) = self
            .replica_cache
            .cache_replica_desc(group_id, to_replica.clone(), self.sync_replica_cache)
            .await
        {
            error!("cachc replica description error: {}", err);
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
                if let Err(err) = self
                    .create_raft_group(group_id, to_replica.replica_id, vec![])
                    .await
                {
                    error!("create raft group error {}", err);
                    panic!("{}", err);
                }
                self.groups.get_mut(&group_id).unwrap()
            }
        };

        if let Err(err) = group.raft_group.step(raft_msg) {
            error!("step raft msg error {}", err);
            panic!("{}", err);
        }
        activity_groups.insert(group_id);
        info!("step raft msg successs");
        tx.send(Ok(RaftMessageResponse {})).unwrap();
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

    #[tracing::instrument(name = "MultiRaftActor::campagin", skip(self))]
    async fn campagin_raft(&mut self, group_id: u64) {
        if let Some(group) = self.groups.get_mut(&group_id) {
            info!("campagin raft group");
            group.raft_group.campaign().unwrap()
        }
    }

    // #[tracing::instrument(
    //     name = "MultiRaftActor::handle_admin_request",
    //     skip(self, msg, tx, activity_groups)
    // )]
    async fn handle_admin_request(
        &mut self,
        msg: AdminMessage,
        tx: oneshot::Sender<Result<(), Error>>,
        activity_groups: &mut HashSet<u64>,
    ) {
        let res = match msg.msg_type() {
            // handle raft group management request
            AdminMessageType::RaftGroup => {
                let msg = msg.raft_group.unwrap();
                self.raft_group_management(msg, activity_groups).await
            }
        };

        if let Err(_error) = tx.send(res) {}
    }

    // #[tracing::instrument(
    //     name = "MultiRaftActor::raft_group_management",
    //     skip(self, activity_groups)
    // )]
    async fn raft_group_management(
        &mut self,
        msg: RaftGroupManagement,
        activity_groups: &mut HashSet<u64>,
    ) -> Result<(), Error> {
        match msg.msg_type() {
            RaftGroupManagementType::MsgCreateGroup => {
                activity_groups.insert(msg.group_id);
                self.create_raft_group(msg.group_id, msg.replica_id, msg.replicas)
                    .await
            }
            RaftGroupManagementType::MsgRemoveGoup => unimplemented!(),
        }
    }

    #[tracing::instrument(name = "MultiRaftActor::create_raft_group", skip(self))]
    async fn create_raft_group(
        &mut self,
        group_id: u64,
        replica_id: u64,
        replicas_desc: Vec<ReplicaDesc>,
    ) -> Result<(), Error> {
        if self.groups.contains_key(&group_id) {
            return Err(Error::RaftGroup(RaftGroupError::Exists(
                self.node_id,
                group_id,
            )));
        }

        if group_id == 0 {
            return Err(Error::BadParameter(format!("bad group_id parameter (0)")));
        }

        if replica_id == 0 {
            return Err(Error::BadParameter(format!("bad replica_id parameter (0)")));
        }

        let group_storage = self.storage.group_storage(group_id, replica_id).await?;

        // if replicas_desc are not empty and are valid data,
        // we know where all replicas of the raft group are located.
        //
        // but, voters of raft and replicas_desc are not one-to-one, because voters
        // can be added in the subsequent way of membership change.
        let applied = 0; // TODO: get applied from stroage
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
            .map_err(|err| Error::Raft(err))?;

        info!(
            "replica ({}) of raft group ({}) is created successfully",
            group_id, replica_id
        );
        let mut group = RaftGroup {
            group_id,
            replica_id,
            raft_group,
            node_ids: Vec::new(),
            proposals: GroupProposalQueue::new(replica_id),
            leader: ReplicaDesc::default(), // TODO: init leader from storage
            committed_term: 0,              // TODO: init committed term from storage
        };

        for replica_desc in replicas_desc.iter() {
            self.replica_cache
                .cache_replica_desc(group_id, replica_desc.clone(), true)
                .await?;
            // track the nodes which other members of the raft consensus group
            group.node_ids.push(replica_desc.node_id);
            self.node_manager.add_node(replica_desc.node_id, group_id);
        }

        // if voters are initialized in storage, we need to read
        // the voter from replica_desc to build the data structure
        let rs = group_storage
            .initial_state()
            .map_err(|err| Error::Raft(err))?;

        let voters = rs.conf_state.voters;
        for voter_id in voters.iter() {
            // at this point, we maybe don't know the infomation about
            // the node which replica. this implies two facts:
            // 1. replicas_desc is empty, and the scheduler does not provide
            //    raft group location information
            // 2. replica_desc information corresponding to voter is not initialized
            //    for the storage
            // if so, we initialized these in subsequent way of raft message handler.
            if let Some(replica_desc) = self.replica_cache.replica_desc(group_id, *voter_id).await?
            {
                if replica_desc.node_id == NO_NODE {
                    continue;
                }
                group.node_ids.push(replica_desc.node_id);
                self.node_manager.add_node(replica_desc.node_id, group_id);
            }
        }
        self.groups.insert(group_id, group);

        Ok(())
    }

    /// Initial the raft consensus group and start a replica in current node.
    // async fn initial_group(&mut self, msg: RaftGroupManagementMessage) -> Result<(), Error> {
    //     assert_eq!(
    //         msg.msg_type(),
    //         RaftGroupManagementMessageType::MsgInitialGroup
    //     );

    //     if msg.group_id == 0 {
    //         return Err(Error::BadParameter(format!("bad group_id parameter (0)")));
    //     }

    //     if msg.replica_id == 0 {
    //         return Err(Error::BadParameter(format!("bad replica_id parameter (0)")));
    //     }

    //     if self.groups.contains_key(&msg.group_id) {
    //         return Err(Error::RaftGroup(RaftGroupError::Exists(
    //             self.node_id,
    //             msg.group_id,
    //         )));
    //     }

    //     // get the raft consensus group reated to storage, create if not exists.
    //     let gs = self
    //         .storage
    //         .group_storage(msg.group_id, msg.replica_id)
    //         .await?;

    //     for replica_metadata in msg.replicas.into_iter() {
    //         if replica_metadata.node_id != NO_NODE {
    //             self.node_manager
    //                 .add_node(replica_metadata.node_id, msg.group_id);
    //             self.replica_cache
    //                 .cache_replica_desc(msg.group_id, replica_metadata, self.sync_replica_cache)
    //                 .await
    //                 .unwrap();
    //         }
    //     }

    //     // set conf state
    //     // let mut cs = ConfState::default();
    //     // cs.voters = msg.initial_voters.clone();
    //     // let _ = gs.set_confstate(cs).map_err(|err| Error::Store(err))?;
    //     // let snapshot = Snapshot::default();
    //     // snapshot.mut_metadata().mut_conf_state().voters = msg.initial_voters.clone();

    //     // let group_storage = match self
    //     //     .storage
    //     //     .group_storage(msg.group_id, msg.replica_id)
    //     //     .await
    //     //     .map_err(|err| Error::Store(err))?
    //     // {
    //     //     Some(gs) => gs,
    //     //     None => {
    //     //         let conf_state = (msg.initial_voters.clone(), vec![]); // TODO: learner
    //     //         self.storage
    //     //             .create_group_storage_with_conf_state(msg.group_id, msg.group_id, conf_state)
    //     //             .await
    //     //             .map_err(|err| Error::Store(err))?
    //     //     }
    //     // };

    //     // create raft consensus group with default logger and group storage.
    //     let applied = 0;
    //     let raft_cfg = raft::Config {
    //         id: msg.replica_id,
    //         applied,
    //         election_tick: self.election_tick,
    //         heartbeat_tick: self.heartbeat_tick,
    //         max_size_per_msg: 1024 * 1024,
    //         max_inflight_msgs: 256,
    //         ..Default::default()
    //     };

    //     let raft_store = gs.clone();
    //     let raft_group = raft::RawNode::with_default_logger(&raft_cfg, raft_store)
    //         .map_err(|err| Error::Raft(err))?;

    //     // add group to node map
    //     self.node_manager.add_node(self.node_id, msg.group_id);

    //     // insert raft_group to group map
    //     let group = RaftGroup {
    //         group_id: msg.group_id,
    //         replica_id: msg.replica_id,
    //         raft_group,
    //         committed_term: 0, // TODO: init committed term
    //         node_ids: vec![self.node_id],
    //         proposals: GroupProposalQueue::new(msg.replica_id),
    //         leader: ReplicaDesc::default(),
    //     };
    //     self.groups.insert(msg.group_id, group);

    //     Ok(())
    // }

    /// Create a replica of the raft consensus group on this node.

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
        for (group_id, results) in response.apply_results {
            let group = match self.groups.get_mut(&group_id) {
                Some(group) => group,
                None => {
                    warn!("group {} removed, skip apply", group_id);
                    continue;
                }
            };
            group.raft_group.advance_apply();
        }
    }

    async fn handle_callback(&mut self, callback_event: CallbackEvent) {
        match callback_event {
            CallbackEvent::None => return,
            CallbackEvent::MembershipChange(view, response_tx) => {
                self.apply_membership_change(view, response_tx).await
            }
        }
    }

    async fn apply_membership_change(
        &mut self,
        view: MembershipChangeView,
        response_tx: oneshot::Sender<Result<(), Error>>,
    ) {
        assert_eq!(
            view.change_request.changes.len(),
            view.conf_change.changes.len()
        );

        let group_id = view.change_request.group_id;
        if let Some(group) = self.groups.get_mut(&group_id) {
            // write storage
            for (i, conf_change) in view.conf_change.changes.iter().enumerate() {
                match conf_change.change_type() {
                    ConfChangeType::AddNode => {
                        let single_change_req = &view.change_request.changes[i];

                        // TODO: this call need transfer to user call, and if user call return errored,
                        // the membership change should failed and user need to retry.
                        // we need a channel to provider user notify actor it need call these code.
                        // and we recv the notify can executing these code, if executed failed, we
                        // response to user and membership change is failed.
                        let replica_desc = ReplicaDesc {
                            node_id: single_change_req.node_id,
                            replica_id: single_change_req.replica_id,
                        };

                        self.node_manager
                            .add_node(single_change_req.node_id, group_id);
                        if let Err(err) = self
                            .replica_cache
                            .cache_replica_desc(group_id, replica_desc, self.sync_replica_cache)
                            .await
                        {
                            response_tx.send(Err(err)).unwrap();
                            return;
                        }
                    }
                    ConfChangeType::RemoveNode => unimplemented!(),
                    ConfChangeType::AddLearnerNode => unimplemented!(),
                }
            }

            match group.raft_group.apply_conf_change(&view.conf_change) {
                Ok(cc) => {
                    response_tx.send(Ok(())).unwrap();
                }
                Err(err) => {
                    response_tx.send(Err(Error::Raft(err))).unwrap();
                }
            }
            return;
        }

        response_tx
            .send(Err(Error::RaftGroup(RaftGroupError::NotFound(
                self.node_id,
                group_id,
            ))))
            .unwrap()
    }

    #[tracing::instrument(name = "MultiRaftActor::on_groups_ready", skip(self))]
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
                        let replica_id = replica_desc.replica_id;
                        group.leader = replica_desc;
                        self.pending_events
                            .push(Event::LederElection(LeaderElectionEvent {
                                group_id: *group_id,
                                leader_id: ss.leader_id,
                                replica_id: replica_id,
                                committed_term: group.committed_term,
                            }))
                    }
                }

                // send out messages
                if !group_ready.messages().is_empty() {
                    info!("handle group ({}) messges", group_id);
                    transport::send_messages(
                        self.node_id,
                        &self.storage,
                        &self.transport,
                        &mut self.node_manager,
                        *group_id,
                        group_ready.take_messages(),
                    )
                    .await;
                }

                // make apply task if need to apply commit entries
                if !group_ready.committed_entries().is_empty() {
                    let last_term = group_ready.committed_entries()
                        [group_ready.committed_entries().len() - 1]
                        .term;
                    group.maybe_update_committed_term(last_term);

                    let entries = group_ready.take_committed_entries();
                    let apply =
                        MultiRaftActorInner::<T, RS, MRS>::create_apply(replica_id, group, entries);

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
            self.apply_actor_tx
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
                // TODO:
                // if let Err(_error) = gs.apply_snapshot(snapshot) {}
            }

            if !ready.entries().is_empty() {
                let entries = ready.take_entries();
                if let Err(_error) = gs.append_entries(&entries) {}
            }

            if let Some(hs) = ready.hs() {
                let hs = hs.clone();
                if let Err(_error) = gs.set_hardstate(hs) {}
            }

            if !ready.persisted_messages().is_empty() {
                info!("handle group ({}) persisted messages", group_id);
                let persistent_msgs = ready.take_persisted_messages();
                transport::send_messages(
                    self.node_id,
                    &self.storage,
                    &self.transport,
                    &mut self.node_manager,
                    *group_id,
                    persistent_msgs,
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
                // group_storage.set_commit(commit);
            }

            if !light_ready.messages().is_empty() {
                let messages = light_ready.take_messages();
                transport::send_messages(
                    self.node_id,
                    &self.storage,
                    &self.transport,
                    &mut self.node_manager,
                    group_id,
                    messages,
                )
                .await;
            }

            if !light_ready.committed_entries().is_empty() {
                let last_term =
                    light_ready.committed_entries()[light_ready.committed_entries().len() - 1].term;
                mut_group.maybe_update_committed_term(last_term);

                let entries = light_ready.take_committed_entries();
                let apply = MultiRaftActorInner::<T, RS, MRS>::create_apply(
                    gwr.replica_id,
                    mut_group,
                    entries,
                );

                apply_task_groups.insert(group_id, ApplyTask::Apply(apply));
            }
        }

        if !apply_task_groups.is_empty() {
            self.apply_actor_tx
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
            .map(|ent| util::compute_entry_size(ent))
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
