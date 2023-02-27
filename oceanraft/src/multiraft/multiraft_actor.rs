use std::collections::hash_map::HashMap;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use raft::prelude::AdminMessage;
use raft::prelude::AdminMessageType;
use raft::prelude::RaftGroupManagement;
use raft::prelude::RaftGroupManagementType;
use raft::Storage;

use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::task::JoinError;
use tokio::task::JoinHandle;

use raft_proto::prelude::AppReadIndexRequest;
use raft_proto::prelude::AppWriteRequest;
use raft_proto::prelude::ConfChangeType;
use raft_proto::prelude::MembershipChangeRequest;
use raft_proto::prelude::Message;
use raft_proto::prelude::MessageType;
use raft_proto::prelude::MultiRaftMessage;
use raft_proto::prelude::MultiRaftMessageResponse;
use raft_proto::prelude::ReplicaDesc;
use raft_proto::prelude::SingleMembershipChange;

use tokio::time::interval_at;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;
use tracing::Level;
use tracing::Span;

use crate::multiraft::error::RaftGroupError;
use crate::multiraft::raft_group::Status;
use crate::util::Stopper;
use crate::util::TaskGroup;

use super::apply_actor::Apply;
use super::apply_actor::ApplyActor;
use super::apply_actor::Request as ApplyRequest;
use super::apply_actor::Response as ApplyResponse;
use super::config::Config;
use super::error::Error;
use super::event::CommitEvent;
use super::event::CommitMembership;
use super::event::Event;
use super::multiraft::NO_GORUP;
use super::multiraft::NO_NODE;
use super::node::NodeManager;
use super::proposal::ProposalQueue;
use super::raft_group::RaftGroup;
use super::raft_group::RaftGroupState;
use super::raft_group::RaftGroupWriteRequest;
use super::replica_cache::ReplicaCache;
use super::response::AppWriteResponse;
use super::storage::MultiRaftStorage;
use super::transport::Transport;
use super::util::Ticker;
use super::StateMachine;

pub(crate) type ResponseCb = Box<dyn FnOnce() -> Result<(), Error> + Send + Sync + 'static>;

pub(crate) fn new_response_callback<T: Send + Sync + 'static>(
    tx: oneshot::Sender<Result<T, Error>>,
    res: Result<T, Error>,
) -> ResponseCb {
    Box::new(move || -> Result<(), Error> {
        tx.send(res)
            .map_err(|_| Error::Internal("receiver dropped".to_string()))
    })
}

pub(crate) fn new_response_error_callback<T: Send + Sync + 'static>(
    tx: oneshot::Sender<Result<T, Error>>,
    err: Error,
) -> ResponseCb {
    Box::new(move || -> Result<(), Error> {
        tx.send(Err(err))
            .map_err(|_| Error::Internal("receiver dropped".to_string()))
    })
}

#[derive(Default)]
struct State {
    stopped: AtomicBool,
}

/// ShardState is used to safely shard the state
/// of an actor between threads.
#[derive(Clone)]
pub struct ShardState {
    state: Arc<State>,
}

impl ShardState {
    /// `true` if actor stop.
    #[inline]
    pub fn stopped(&self) -> bool {
        self.state.stopped.load(Ordering::Acquire)
    }
}

pub struct MultiRaftActor<T, RS, MRS, RSM, RES>
where
    T: Transport,
    RS: Storage,
    MRS: MultiRaftStorage<RS>,
    RSM: StateMachine<RES>,
    RES: AppWriteResponse,
{
    pub shard: ShardState,
    // FIXME: queue should have one per-group.
    pub write_propose_tx: Sender<(AppWriteRequest, oneshot::Sender<Result<RES, Error>>)>,
    pub read_index_propose_tx: Sender<(AppReadIndexRequest, oneshot::Sender<Result<(), Error>>)>,
    pub membership_change_tx:
        Sender<(MembershipChangeRequest, oneshot::Sender<Result<RES, Error>>)>,
    pub campaign_tx: Sender<(u64, oneshot::Sender<Result<(), Error>>)>,
    pub raft_message_tx: Sender<(
        MultiRaftMessage,
        oneshot::Sender<Result<MultiRaftMessageResponse, Error>>,
    )>,
    pub admin_tx: Sender<(RaftGroupManagement, oneshot::Sender<Result<(), Error>>)>,
    apply: ApplyActor<RSM, RES>,
    runtime: Mutex<Option<MultiRaftActorRuntime<T, RS, MRS, RES>>>,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl<T, RS, MRS, RSM, RES> MultiRaftActor<T, RS, MRS, RSM, RES>
where
    T: Transport + Clone,
    RS: Storage + Clone + Sync + Send + 'static,
    MRS: MultiRaftStorage<RS>,
    RES: AppWriteResponse,
    RSM: StateMachine<RES>,
{
    pub fn new(
        cfg: &Config,
        transport: &T,
        storage: &MRS,
        rsm: RSM,
        event_tx: &Sender<Vec<Event>>,
    ) -> Self {
        let state = Arc::new(State::default());

        let (write_propose_tx, write_propose_rx) = channel(cfg.write_proposal_queue_size);
        let (read_index_propose_tx, read_index_propose_rx) = channel(1);
        let (membership_change_tx, membership_change_rx) = channel(1);
        let (admin_tx, admin_rx) = channel(1);
        let (campaign_tx, campaign_rx) = channel(1);
        let (raft_message_tx, raft_message_rx) = channel(10);

        let (callback_tx, callback_rx) = unbounded_channel();

        let (apply_request_tx, apply_request_rx) = unbounded_channel();
        let (apply_response_tx, apply_response_rx) = unbounded_channel();

        let runtime = MultiRaftActorRuntime::<T, RS, MRS, RES> {
            cfg: cfg.clone(),
            // activity_groups: HashSet::new(),
            state: state.clone(),
            node_id: cfg.node_id,
            node_manager: NodeManager::new(),
            event_tx: event_tx.clone(),
            groups: HashMap::new(),
            tick_interval: Duration::from_millis(cfg.tick_interval),
            write_propose_rx,
            read_index_propose_rx,
            membership_change_rx,
            campaign_rx,
            raft_message_rx,
            admin_rx,
            storage: storage.clone(),
            transport: transport.clone(),
            apply_request_tx,
            apply_response_rx,
            commit_rx: callback_rx,
            active_groups: HashSet::new(),
            sync_replica_cache: true,
            replica_cache: ReplicaCache::new(storage.clone()),
            pending_events: Vec::new(),
            response_cbs: Vec::new(),
            _m1: PhantomData,
        };

        let apply = ApplyActor::new(cfg, rsm, apply_request_rx, apply_response_tx, callback_tx);

        Self {
            shard: ShardState { state },
            raft_message_tx,
            write_propose_tx,
            read_index_propose_tx,
            membership_change_tx,
            campaign_tx,
            admin_tx,
            join: Mutex::default(),
            apply,
            runtime: Mutex::new(Some(runtime)),
        }
    }

    pub fn start(&self, task_group: &TaskGroup, ticker: Option<Box<dyn Ticker>>) {
        self.apply.start(task_group);
        let runtime = { self.runtime.lock().unwrap().take().unwrap() };

        let stopper = task_group.stopper();
        let jh = task_group.spawn(async move {
            runtime.main_loop(stopper, ticker).await;
        });
        *self.join.lock().unwrap() = Some(jh);
    }

    #[inline]
    async fn join(&mut self) -> Result<(), JoinError> {
        // TODO join apply

        let jh = { self.join.lock().unwrap().take().unwrap() };
        jh.await
    }
}

pub struct MultiRaftActorRuntime<T, RS, MRS, RES>
where
    T: Transport,
    RS: Storage,
    MRS: MultiRaftStorage<RS>,
    RES: AppWriteResponse,
{
    cfg: Config,
    node_id: u64,
    state: Arc<State>,
    storage: MRS,
    transport: T,
    node_manager: NodeManager,
    replica_cache: ReplicaCache<RS, MRS>,
    sync_replica_cache: bool,
    groups: HashMap<u64, RaftGroup<RS, RES>>,
    tick_interval: Duration,
    raft_message_rx: Receiver<(
        MultiRaftMessage,
        oneshot::Sender<Result<MultiRaftMessageResponse, Error>>,
    )>,
    write_propose_rx: Receiver<(AppWriteRequest, oneshot::Sender<Result<RES, Error>>)>,
    read_index_propose_rx: Receiver<(AppReadIndexRequest, oneshot::Sender<Result<(), Error>>)>,
    membership_change_rx: Receiver<(MembershipChangeRequest, oneshot::Sender<Result<RES, Error>>)>,
    admin_rx: Receiver<(RaftGroupManagement, oneshot::Sender<Result<(), Error>>)>,
    campaign_rx: Receiver<(u64, oneshot::Sender<Result<(), Error>>)>,
    event_tx: Sender<Vec<Event>>,
    commit_rx: UnboundedReceiver<CommitEvent>,
    apply_request_tx: UnboundedSender<(Span, ApplyRequest<RES>)>,
    apply_response_rx: UnboundedReceiver<ApplyResponse>,
    active_groups: HashSet<u64>,
    response_cbs: Vec<ResponseCb>,
    pending_events: Vec<Event>,
    _m1: PhantomData<RS>,
}

impl<T, RS, MRS, RES> MultiRaftActorRuntime<T, RS, MRS, RES>
where
    T: Transport + Clone,
    RS: Storage + Send + Sync + Clone + 'static,
    MRS: MultiRaftStorage<RS>,
    RES: AppWriteResponse,
{
    #[tracing::instrument(
        name = "MultiRaftActorRuntime::main_loop"
        level = Level::TRACE,
        skip_all,
        fields(node_id=self.node_id)
    )]
    async fn main_loop(mut self, mut stopper: Stopper, ticker: Option<Box<dyn Ticker>>) {
        info!("node {}: start multiraft main_loop", self.node_id);

        let mut ticker: Box<dyn Ticker> = ticker.map_or(
            Box::new(interval_at(
                tokio::time::Instant::now() + self.tick_interval,
                self.tick_interval,
            )),
            |t| t,
        );

        let mut ticks = 0;
        loop {
            // let mut pending_response_callbacks = vec![];
            spawn_handle_response_events(
                self.node_id,
                &self.event_tx,
                std::mem::take(&mut self.pending_events),
            );

            tokio::select! {
                // Note: see https://github.com/tokio-rs/tokio/discussions/4019 for more
                // information about why mut here.
                _ = &mut stopper => {
                    self.do_stop();
                    break
                }

                Some((req, tx)) = self.raft_message_rx.recv() => {
                    let res = self.handle_multiraft_message(req).await ;
                    self.response_cbs.push(new_response_callback(tx, res));
                },

                _ = ticker.recv() => {
                    self.groups.iter_mut().for_each(|(id, group)| {
                        if group.raft_group.tick() {
                            self.active_groups.insert(*id);
                        }
                    });
                    ticks += 1;
                    if ticks >= self.cfg.heartbeat_tick {
                        ticks = 0;
                        self.incorporate_heatbeat();
                    }
                },

                Some((req, tx)) = self.write_propose_rx.recv() => {
                    let group_id = req.group_id;
                    self.active_groups.insert(group_id);
                    if let Some(cb) = self.propose_write_request(req, tx) {
                        self.response_cbs.push(cb);
                    }
                },

                Some((req, tx)) = self.membership_change_rx.recv() => {
                    let group_id = req.group_id;
                    self.active_groups.insert(group_id);
                    if let Some(cb) = self.propose_membership_change_request(req, tx) {
                        self.response_cbs.push(cb);
                    }
                },

                Some((req, tx)) = self.read_index_propose_rx.recv() => {
                    let group_id = req.group_id;
                    self.active_groups.insert(group_id);
                    if let Some(cb) = self.propose_read_index_request(req, tx) {
                        self.response_cbs.push(cb);
                    }
                },

                Some(res) = self.apply_response_rx.recv() => {
                    self.handle_apply_response(res).await;
                },

                Some((req, tx)) = self.admin_rx.recv() => {
                     if let Some(cb) = self.handle_raft_group_management(req, tx).await {
                        self.response_cbs.push(cb);
                    }
                },

                Some((group_id, tx)) = self.campaign_rx.recv() => {
                    self.campaign_raft(group_id, tx);
                    self.active_groups.insert(group_id);
                }

                Some(msg) = self.commit_rx.recv() => {
                    self.handle_apply_commit(msg).await;
                },

                else => {},
            }

            if !self.active_groups.is_empty() {
                self.handle_readys().await;
            }

            spawn_handle_response_callbacks(self.response_cbs.drain(..).collect());
        }
    }

    /// The node sends heartbeats to other nodes instead
    /// of all raft groups on that node.
    fn incorporate_heatbeat(&self) {
        for (to_node, _) in self.node_manager.iter() {
            if *to_node == self.node_id {
                continue;
            }

            trace!(
                "node {}: trigger heartbeta {} -> {} ",
                self.node_id,
                self.node_id,
                *to_node
            );

            // coalesced heartbeat to all nodes. the heartbeat message is node
            // level message so from and to set 0 when sending, and the specific
            // value is set by message receiver.
            let mut raft_msg = Message::default();
            raft_msg.set_msg_type(MessageType::MsgHeartbeat);
            if let Err(err) = self.transport.send(MultiRaftMessage {
                group_id: NO_GORUP,
                from_node: self.node_id,
                to_node: *to_node,
                replicas: vec![],
                msg: Some(raft_msg),
            }) {
                error!(
                    "node {}: send heartbeat to {} error: {}",
                    self.node_id, *to_node, err
                )
            }
        }
    }

    async fn handle_multiraft_message(
        &mut self,
        msg: MultiRaftMessage,
    ) -> Result<MultiRaftMessageResponse, Error> {
        match msg.msg.as_ref().expect("invalid msg").msg_type() {
            MessageType::MsgHeartbeat => self.fanout_heartbeat(msg).await,
            MessageType::MsgHeartbeatResponse => self.fanout_heartbeat_response(msg).await,
            _ => self.handle_raft_message(msg).await,
        }
    }

    #[tracing::instrument(
        name = "MultiRaftActorRuntime::handle_raft_message",
        level = Level::TRACE,
        skip_all,
    )]
    async fn handle_raft_message(
        &mut self,
        mut msg: MultiRaftMessage,
    ) -> Result<MultiRaftMessageResponse, Error> {
        let raft_msg = msg.msg.take().unwrap();
        let group_id = msg.group_id;

        // processing messages between replicas from other nodes to self node.
        let from_replica = ReplicaDesc {
            node_id: msg.from_node,
            replica_id: raft_msg.from,
        };

        let to_replica = ReplicaDesc {
            node_id: msg.to_node,
            replica_id: raft_msg.to,
        };

        trace!(
            "node {}: recv msg {:?} from {}: group = {}, from_replica = {}, to_replica = {}",
            self.node_id,
            raft_msg.msg_type(),
            msg.from_node,
            group_id,
            from_replica.replica_id,
            to_replica.replica_id,
        );

        let _ = self
            .replica_cache
            .cache_replica_desc(group_id, from_replica.clone(), self.sync_replica_cache)
            .await?;

        let _ = self
            .replica_cache
            .cache_replica_desc(group_id, to_replica.clone(), self.sync_replica_cache)
            .await?;

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
                info!(
                    "node {}: got message for unknow group {}; creating replica {}",
                    self.node_id, group_id, to_replica.replica_id
                );
                // if we receive initialization messages from a raft replica
                // on another node, this means that a member change has occurred
                // and we should create a new raft replica.
                // Note. MsgHeartbeat && commit = 0 is not initial message because we
                // hijacked in fanout_heartbeat.
                let _ = self
                    .create_raft_group(group_id, to_replica.replica_id, msg.replicas)
                    .await
                    .map_err(|err| {
                        error!(
                            "node {}: create group for replica {} error {}",
                            self.node_id, to_replica.replica_id, err
                        );
                        err
                    })?;

                self.groups.get_mut(&group_id).unwrap()
            }
        };

        // FIX: t30_membership single_step
        if let Err(err) = group.raft_group.step(raft_msg) {
            warn!("node {}: step raf message error: {}", self.node_id, err);
        }
        self.active_groups.insert(group_id);
        Ok(MultiRaftMessageResponse {})
    }

    /// Fanout heartbeats from other nodes to all raft groups on this node.
    async fn fanout_heartbeat(
        &mut self,
        msg: MultiRaftMessage,
    ) -> Result<MultiRaftMessageResponse, Error> {
        let from_node_id = msg.from_node;
        let to_node_id = msg.to_node;
        let mut fanouted_groups = 0;
        let mut fanouted_followers = 0;
        if let Some(from_node) = self.node_manager.get_node(&from_node_id) {
            for (group_id, _) in from_node.group_map.iter() {
                let group = match self.groups.get_mut(group_id) {
                    None => {
                        warn!("node {}: from node {} failed to fanout to group {} because does not exists", self.node_id, from_node_id, *group_id);
                        continue;
                    }
                    Some(group) => group,
                };

                fanouted_groups += 1;
                self.active_groups.insert(*group_id);

                if group.leader.node_id != from_node_id || msg.from_node == self.node_id {
                    trace!("node {}: not fanning out heartbeat to {}, msg is from {} and leader is {:?}", self.node_id, group_id, from_node_id, group.leader);
                    continue;
                }

                if group.is_leader() {
                    warn!("node {}: received a heartbeat from the leader node {}, but replica {} of group {} also leader and there may have been a network partition", self.node_id, from_node_id, *group_id, group.replica_id)
                }

                // gets the replica stored in this node.
                let from_replica = match self
                    .replica_cache
                    .replica_for_node(*group_id, from_node_id)
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

                // FIXME: t30_membership single_step
                let to_replica = match self
                    .replica_cache
                    .replica_for_node(*group_id, to_node_id)
                    .await
                {
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

                let mut step_msg = raft::prelude::Message::default();
                step_msg.set_msg_type(raft::prelude::MessageType::MsgHeartbeat);
                // FIX(test command)
                //
                // Although the heatbeat is not set without affecting correctness, but liveness
                // maybe cannot satisty. such as in test code 1) submit some commands 2) and
                // then wait apply and perform a heartbeat. but due to a heartbeat cannot set commit, so
                // no propose lead to test failed.
                step_msg.commit = group.raft_group.raft.raft_log.committed;
                step_msg.term = group.raft_group.raft.term; // FIX(t30_membership::test_remove)
                step_msg.from = from_replica.replica_id;
                step_msg.to = to_replica.replica_id;
                trace!(
                    "node {}: fanout heartbeat {}.{} -> {}.{}",
                    self.node_id,
                    from_node_id,
                    step_msg.from,
                    to_node_id,
                    step_msg.to
                );
                if let Err(err) = group.raft_group.step(step_msg) {
                    warn!(
                        "node {}: step heatbeat message error: {}",
                        self.node_id, err
                    );
                }
            }
        } else {
            // In this point, we receive heartbeat message from other nodes,
            // but we don't have the from_node raft group information, so we
            // don't know which raft group's replica we should fanout to.
            warn!(
                "missing raft groups at from_node {} fanout heartbeat",
                msg.from_node
            );
            self.node_manager.add_node2(msg.from_node);
        }

        trace!(
            "node {}: fanouted {} groups and followers {}",
            self.node_id,
            fanouted_groups,
            fanouted_followers
        );

        let response_msg = {
            let mut raft_msg = Message::default();
            raft_msg.set_msg_type(MessageType::MsgHeartbeatResponse);
            MultiRaftMessage {
                group_id: NO_GORUP,
                from_node: self.node_id,
                to_node: from_node_id,
                replicas: vec![],
                msg: Some(raft_msg),
            }
        };

        let _ = self.transport.send(response_msg)?;
        Ok(MultiRaftMessageResponse {})
    }

    /// Fanout heartbeats response from other nodes to all raft groups on this node.
    async fn fanout_heartbeat_response(
        &mut self,
        msg: MultiRaftMessage,
    ) -> Result<MultiRaftMessageResponse, Error> {
        if let Some(node) = self.node_manager.get_node(&msg.from_node) {
            for (group_id, _) in node.group_map.iter() {
                let group = match self.groups.get_mut(group_id) {
                    None => {
                        warn!("node {}: from node {} failed to fanout response to group {} because does not exists", self.node_id, msg.from_node, *group_id);
                        continue;
                    }
                    Some(group) => group,
                };

                self.active_groups.insert(*group_id);

                if group.leader.node_id != self.node_id || msg.from_node == self.node_id {
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
                msg.term = group.term();
                msg.from = from_replica.replica_id;
                msg.to = to_replica.replica_id;
                if let Err(err) = group.raft_group.step(msg) {
                    warn!(
                        "node {}: step heatbeat response message error: {}",
                        self.node_id, err
                    );
                }
            }
        } else {
            warn!(
                "missing raft groups at from_node {} fanout heartbeat response",
                msg.from_node
            );
            self.node_manager.add_node2(msg.from_node);
        }
        Ok(MultiRaftMessageResponse {})
    }

    /// if `None` is returned, the write request is successfully committed
    /// to raft, otherwise the callback closure of the error response is
    /// returned.
    ///
    /// Note: Must be called to respond to the client when the loop ends.
    #[tracing::instrument(
        level = Level::TRACE,
        name = "MultiRaftActorRuntime::propose_write_request",
        skip(self, tx))
    ]
    fn propose_write_request(
        &mut self,
        request: AppWriteRequest,
        tx: oneshot::Sender<Result<RES, Error>>,
    ) -> Option<ResponseCb> {
        let group_id = request.group_id;
        match self.groups.get_mut(&group_id) {
            None => {
                warn!(
                    "node {}: proposal failed, group {} does not exists",
                    self.node_id, group_id,
                );
                return Some(new_response_error_callback(
                    tx,
                    Error::RaftGroup(RaftGroupError::Deleted(self.node_id, group_id)),
                ));
            }
            // TODO: handle nodes removed
            Some(group) => group.propose_write(request, tx),
        }
    }

    /// if `None` is returned, the membership change request is successfully committed
    /// to raft, otherwise the callback closure of the error response is
    /// returned.
    ///
    /// Note: Must be called to respond to the client when the loop ends.
    #[tracing::instrument(
        level = Level::TRACE,
        name = "MultiRaftActorRuntime::propose_membership_change_request",
        skip_all,
    )]
    fn propose_membership_change_request(
        &mut self,
        request: MembershipChangeRequest,
        tx: oneshot::Sender<Result<RES, Error>>,
    ) -> Option<ResponseCb> {
        let group_id = request.group_id;
        match self.groups.get_mut(&group_id) {
            None => {
                warn!(
                    "node {}: proposal membership failed, group {} does not exists",
                    self.node_id, group_id,
                );
                return Some(new_response_error_callback(
                    tx,
                    Error::RaftGroup(RaftGroupError::Deleted(self.node_id, group_id)),
                ));
            }
            Some(group) => group.propose_membership_change(request, tx),
        }
    }

    /// if `None` is returned, the read_index request is successfully committed
    /// to raft, otherwise the callback closure of the error response is
    /// returned.
    ///
    /// Note: Must be called to respond to the client when the loop ends.
    #[tracing::instrument(
        level = Level::TRACE,
        name = "MultiRaftActorRuntime::propose_read_index_request",
        skip(self, tx))
    ]
    fn propose_read_index_request(
        &mut self,
        request: AppReadIndexRequest,
        tx: oneshot::Sender<Result<(), Error>>,
    ) -> Option<ResponseCb> {
        let group_id = request.group_id;
        match self.groups.get_mut(&group_id) {
            None => {
                warn!(
                    "node {}: proposal read_index failed, group {} does not exists",
                    self.node_id, group_id,
                );
                return Some(new_response_error_callback(
                    tx,
                    Error::RaftGroup(RaftGroupError::Deleted(self.node_id, group_id)),
                ));
            }
            Some(group) => group.read_index_propose(request, tx),
        }
    }

    #[tracing::instrument(
        level = Level::TRACE,
        name = "MultiRaftActorRuntime::campagin_raft", 
        skip(self, tx)
    )]
    fn campaign_raft(&mut self, group_id: u64, tx: oneshot::Sender<Result<(), Error>>) {
        let res = if let Some(group) = self.groups.get_mut(&group_id) {
            //            self.activity_groups.insert(group_id);
            group.raft_group.campaign().map_err(|err| Error::Raft(err))
        } else {
            warn!(
                "the node({}) campaign group({}) is removed",
                self.node_id, group_id
            );
            Err(Error::RaftGroup(RaftGroupError::NotExist(
                group_id,
                self.node_id,
            )))
        };

        if let Err(_) = tx.send(res) {
            warn!("the node({}) campaign group({}) successfully but the receiver of receive the result is dropped", self.node_id, group_id)
        }
    }

    // #[tracing::instrument(
    //     name = "MultiRaftActorRuntime::handle_admin_request",
    //     level = Level::TRACE,
    //     skip_all,
    // )]
    // async fn handle_admin_request(
    //     &mut self,
    //     msg: AdminMessage,
    //     tx: oneshot::Sender<Result<(), Error>>,
    // ) -> Option<ResponseCb> {
    //     let res = match msg.msg_type() {
    //         // handle raft group management request
    //         AdminMessageType::RaftGroup => {
    //             let msg = msg.raft_group.unwrap();
    //             self.handle_raft_group_management(msg).await
    //         }
    //     };

    //     return Some(new_response_callback(tx, res));
    // }

    #[tracing::instrument(
        name = "MultiRaftActorRuntime::raft_group_management",
        level = Level::TRACE,
        skip_all
    )]
    async fn handle_raft_group_management(
        &mut self,
        msg: RaftGroupManagement,
        tx: oneshot::Sender<Result<(), Error>>,
    ) -> Option<ResponseCb> {
        let res = match msg.msg_type() {
            RaftGroupManagementType::MsgCreateGroup => {
                self.active_groups.insert(msg.group_id);
                self.create_raft_group(msg.group_id, msg.replica_id, msg.replicas)
                    .await
            }
            RaftGroupManagementType::MsgRemoveGoup => {
                // marke delete
                let group_id = msg.group_id;
                let group = match self.groups.get_mut(&group_id) {
                    None => return Some(new_response_callback(tx, Ok(()))),
                    Some(group) => group,
                };

                for proposal in group.proposals.drain(..) {
                    proposal.tx.map(|tx| {
                        tx.send(Err(Error::RaftGroup(RaftGroupError::Deleted(
                            self.node_id,
                            group_id,
                        ))))
                    });
                }

                group.status = Status::Delete;

                // TODO: impl broadcast

                Ok(())
            }
        };

        return Some(new_response_callback(tx, res));
    }

    // #[tracing::instrument(
    //     name = "MultiRaftActorRuntime::create_raft_group",
    //     level = Level::TRACE,
    //     skip(self))
    // ]
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
        let rs = group_storage
            .initial_state()
            .map_err(|err| Error::Raft(err))?;

        // if replicas_desc are not empty and are valid data,
        // we know where all replicas of the raft group are located.
        //
        // but, voters of raft and replicas_desc are not one-to-one, because voters
        // can be added in the subsequent way of membership change.

        // If there is an initial state, there are two cases
        // 1. create a new replica, started by configuration, applied should be 1
        // 2. an already run replica is created again, may have experienced snapshot,
        // applied is the committed log index that keeping the invariant applied <= committed.
        let raft_cfg = raft::Config {
            id: replica_id,
            applied: rs.hard_state.commit,
            election_tick: self.cfg.election_tick,
            heartbeat_tick: self.cfg.heartbeat_tick,
            max_size_per_msg: 1024 * 1024,
            max_inflight_msgs: 256,
            ..Default::default()
        };

        let raft_store = group_storage.clone();
        let raft_group = raft::RawNode::with_default_logger(&raft_cfg, raft_store)
            .map_err(|err| Error::Raft(err))?;

        info!(
            "node {}: raft group_id = {}, replica_id = {} created",
            self.node_id, group_id, replica_id
        );
        let mut group = RaftGroup {
            group_id,
            replica_id,
            raft_group,
            node_ids: Vec::new(),
            proposals: ProposalQueue::new(replica_id),
            leader: ReplicaDesc::default(), // TODO: init leader from storage
            committed_term: 0,              // TODO: init committed term from storage
            state: RaftGroupState::default(),
            status: Status::None,
        };

        for replica_desc in replicas_desc.iter() {
            self.replica_cache
                .cache_replica_desc(group_id, replica_desc.clone(), true)
                .await?;
            // track the nodes which other members of the raft consensus group
            group.add_track_node(replica_desc.node_id);
            self.node_manager.add_node(replica_desc.node_id, group_id);
        }

        // TODO: check voters and replica_descs consistent

        // if voters are initialized in storage, we need to read
        // the voter from replica_desc to build the data structure
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
                group.add_track_node(replica_desc.node_id);
                self.node_manager.add_node(replica_desc.node_id, group_id);
            }
        }
        self.groups.insert(group_id, group);

        Ok(())
    }

    #[allow(unused)]
    async fn remove_raft_group(&mut self, group_id: u64, replica_id: u64) -> Result<(), Error> {
        let mut group = match self.groups.remove(&group_id) {
            None => return Ok(()),
            Some(group) => group,
        };

        for proposal in group.proposals.drain(..) {
            proposal.tx.map(|tx| {
                tx.send(Err(Error::RaftGroup(RaftGroupError::Deleted(
                    self.node_id,
                    group_id,
                ))))
            });
        }

        for node_id in group.node_ids {
            self.node_manager.remove_group(node_id, group_id);
        }

        Ok(())
    }

    #[tracing::instrument(
        level = Level::TRACE,
        name = "MultiRaftActorRuntime::handle_apply_response",
        skip(self))
    ]
    async fn handle_apply_response(&mut self, response: ApplyResponse) {
        let group = match self.groups.get_mut(&response.group_id) {
            Some(group) => group,
            None => {
                warn!("group {} removed, skip apply", response.group_id);
                return;
            }
        };

        group
            .raft_group
            .advance_apply_to(response.apply_state.applied_index);
        // group.raft_group.advance_apply();
        debug!(
            "node {}: group = {} apply state change = {:?}",
            self.node_id, response.group_id, response.apply_state
        );

        group.state.apply_state = response.apply_state;
    }

    async fn handle_apply_commit(&mut self, commit: CommitEvent) {
        match commit {
            CommitEvent::None => return,
            CommitEvent::Membership((commit, tx)) => {
                self.commit_membership_change(commit).await;
                self.response_cbs.push(new_response_callback(tx, Ok(())));
            }
        }
    }

    async fn commit_membership_change(&mut self, view: CommitMembership) -> Result<(), Error> {
        assert_eq!(
            view.change_request.changes.len(),
            view.conf_change.changes.len()
        );

        let group_id = view.change_request.group_id;

        let group = match self.groups.get_mut(&group_id) {
            Some(group) => group,
            None => {
                error!(
                    "node {}: commit membership change failed: group {} deleted",
                    self.node_id, group_id,
                );
                return Err(Error::RaftGroup(RaftGroupError::Deleted(
                    self.node_id,
                    group_id,
                )));
            }
        };

        // apply to inner state
        for (conf_change, change_request) in view
            .conf_change
            .changes
            .iter()
            .zip(view.change_request.changes.iter())
        {
            match conf_change.change_type() {
                ConfChangeType::AddNode => {
                    MultiRaftActorRuntime::<T, RS, MRS, RES>::membership_add(
                        self.node_id,
                        group,
                        change_request,
                        &mut self.node_manager,
                        &mut self.replica_cache,
                    )
                    .await;
                }
                ConfChangeType::RemoveNode => {
                    MultiRaftActorRuntime::<T, RS, MRS, RES>::membership_remove(
                        self.node_id,
                        group,
                        change_request,
                        &mut self.node_manager,
                        &mut self.replica_cache,
                    )
                    .await;
                }
                ConfChangeType::AddLearnerNode => unimplemented!(),
            }
        }

        // apply to raft
        let conf_state = match group.raft_group.apply_conf_change(&view.conf_change) {
            Err(err) => {
                error!(
                    "node {}: commit membership change error: group = {}, err = {}",
                    self.node_id, group_id, err,
                );
                return Err(Error::Raft(err));
            }
            Ok(conf_state) => conf_state,
        };

        let gs = &self
            .storage
            .group_storage(group_id, group.replica_id)
            .await?;
        gs.set_conf_state(conf_state);

        return Ok(());
    }

    async fn membership_add(
        node_id: u64,
        group: &mut RaftGroup<RS, RES>,
        change: &SingleMembershipChange,
        node_manager: &mut NodeManager,
        replica_cache: &mut ReplicaCache<RS, MRS>,
    ) {
        let group_id = group.group_id;
        node_manager.add_group(change.node_id, group_id);

        // TODO: this call need transfer to user call, and if user call return errored,
        // the membership change should failed and user need to retry.
        // we need a channel to provider user notify actor it need call these code.
        // and we recv the notify can executing these code, if executed failed, we
        // response to user and membership change is failed.

        if let Err(err) = replica_cache
            .cache_replica_desc(
                group_id,
                ReplicaDesc {
                    node_id: change.node_id,
                    replica_id: change.replica_id,
                },
                true,
            )
            .await
        {
            warn!(
                "node {}: the membership change request to add replica desc error: {} ",
                change.node_id, err
            );
        }

        group.add_track_node(change.node_id);

        info!(
            "node {}: add membership change applied: node = {}, group = {}, replica = {}",
            node_id, change.node_id, group_id, change.replica_id
        );
    }

    async fn membership_remove(
        node_id: u64,
        group: &mut RaftGroup<RS, RES>,
        change_request: &SingleMembershipChange,
        node_manager: &mut NodeManager,
        replica_cache: &mut ReplicaCache<RS, MRS>,
    ) {
        let group_id = group.group_id;
        let _ = group.remove_pending_proposals();
        let _ = group.remove_track_node(change_request.node_id);
        // TODO: think remove if node has empty group_map.
        let _ = node_manager.remove_group(change_request.node_id, group_id);

        if let Err(err) = replica_cache
            .remove_replica_desc(
                group_id,
                ReplicaDesc {
                    node_id: change_request.node_id,
                    replica_id: change_request.replica_id,
                },
                true,
            )
            .await
        {
            warn!(
                "node {}: the membership change request to add replica desc error: {} ",
                change_request.node_id, err
            );
        }

        info!(
            "node {}: remove membership change applied: node = {}, group = {}, replica = {}",
            node_id, change_request.node_id, group_id, change_request.replica_id
        );
    }

    async fn handle_readys(&mut self) {
        let active_groups = self.active_groups.drain();
        let mut multi_groups_write = HashMap::new();
        let mut applys = HashMap::new();
        // for group_id in self.activity_groups.iter() {
        for group_id in active_groups {
            if group_id == NO_GORUP {
                continue;
            }

            match self.groups.get_mut(&group_id) {
                None => {
                    warn!(
                        "node {}: make group {} activity but dropped",
                        self.node_id, group_id
                    )
                }
                Some(group) => {
                    if !group.raft_group.has_ready() {
                        continue;
                    }
                    group
                        .handle_ready(
                            self.node_id,
                            &self.transport,
                            &self.storage,
                            &mut self.replica_cache,
                            &mut self.node_manager,
                            &mut multi_groups_write,
                            &mut applys,
                            &mut self.pending_events,
                        )
                        .await;
                }
            }
        }
        if !applys.is_empty() {
            self.send_applys(applys);
        }

        let gwrs = self.do_multi_groups_write(multi_groups_write).await;
        self.do_multi_groups_write_finish(gwrs).await;
    }

    // #[tracing::instrument(
    //     level = Level::TRACE,
    //     name = "MultiRaftActorInner::do_multi_groups_write",
    //     skip_all
    // )]
    async fn do_multi_groups_write(
        &mut self,
        mut ready_write_groups: HashMap<u64, RaftGroupWriteRequest>,
    ) -> HashMap<u64, RaftGroupWriteRequest> {
        // TODO(yuanchang.xu) Disk write flow control
        for (group_id, gwr) in ready_write_groups.iter_mut() {
            // TODO: cache storage in related raft group.
            let gs = match self.storage.group_storage(*group_id, gwr.replica_id).await {
                Ok(gs) => gs,
                Err(err) => {
                    error!(
                        "node({}) group({}) ready but got group storage error {}",
                        self.node_id, group_id, err
                    );
                    continue;
                }
            };

            if let Some(group) = self.groups.get_mut(&group_id) {
                // TODO: return LightRaftGroupWrite
                group
                    .handle_ready_write(
                        self.node_id,
                        gwr,
                        &gs,
                        &self.transport,
                        &mut self.replica_cache,
                        &mut self.node_manager,
                    )
                    .await;
            } else {
                // TODO: should process this case
                error!(
                    "node({}) group({}) readyed, but group is dropped",
                    self.node_id, group_id
                );
            }
        }

        ready_write_groups
    }

    // #[tracing::instrument(
    //     level = Level::TRACE,
    //     name = "MultiRaftActorInner::on_multi_groups_write_finish",
    //     skip_all
    // )]
    async fn do_multi_groups_write_finish(
        &mut self,
        ready_groups: HashMap<u64, RaftGroupWriteRequest>,
    ) {
        let mut multi_groups_apply = HashMap::new();
        for (group_id, mut gwr) in ready_groups.into_iter() {
            let mut_group = match self.groups.get_mut(&group_id) {
                None => {
                    error!(
                        "node({}) group({}) ready finish, but group is dropped",
                        self.node_id, group_id
                    );
                    continue;
                }
                Some(g) => g,
            };

            mut_group
                .handle_light_ready(
                    self.node_id,
                    &self.transport,
                    &self.storage,
                    &mut self.replica_cache,
                    &mut self.node_manager,
                    &mut gwr,
                    &mut multi_groups_apply,
                )
                .await;
        }

        if !multi_groups_apply.is_empty() {
            self.send_applys(multi_groups_apply);
        }
    }
    // #[tracing::instrument(
    //     name = "MultiRaftActorInner::handle_response_callbacks"
    //     level = Level::TRACE,
    //     skip_all
    // )]

    fn send_applys(&self, applys: HashMap<u64, Apply<RES>>) {
        let span = tracing::span::Span::current();
        if let Err(_err) = self
            .apply_request_tx
            .send((span.clone(), ApplyRequest::Apply { applys }))
        {
            // FIXME
            warn!("apply actor stopped");
        }
    }

    #[tracing::instrument(
        name = "MultiRaftActorRuntime::do_stop"
        level = Level::TRACE,
        skip_all
    )]
    fn do_stop(mut self) {
        self.state.stopped.store(true, Ordering::Release);
    }
}

fn spawn_handle_response_callbacks(cbs: Vec<ResponseCb>) {
    // let drainner = cbs.drain(..).collect::<Vec<_>>();
    tokio::spawn(async move {
        for cb in cbs {
            if let Err(err) = cb() {
                warn!("{}", err)
            }
        }
    });
}

fn spawn_handle_response_events(node_id: u64, tx: &Sender<Vec<Event>>, events: Vec<Event>) {
    let tx = tx.clone();
    // TODO: add timeout
    let _ = tokio::spawn(async move {
        if let Err(_) = tx.send(events).await {
            error!(
                "node {}: send pending events error, the event receiver dropped",
                node_id
            );
        }
    });
}
