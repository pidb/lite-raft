use std::collections::hash_map::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::thread::current;
use std::time::Duration;

use raft::prelude::AdminMessage;
use raft::prelude::AdminMessageType;
use raft::prelude::RaftGroupManagement;
use raft::prelude::RaftGroupManagementType;
use raft::LightReady;
use raft::Ready;
use raft::SoftState;
use raft::StateRole;
use raft::Storage;

use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::task::JoinError;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use raft_proto::prelude::AppReadIndexRequest;
use raft_proto::prelude::AppWriteRequest;
use raft_proto::prelude::ConfChangeSingle;
use raft_proto::prelude::ConfChangeType;
use raft_proto::prelude::ConfChangeV2;
use raft_proto::prelude::Entry;
use raft_proto::prelude::MembershipChangeRequest;
use raft_proto::prelude::Message;
use raft_proto::prelude::MessageType;
use raft_proto::prelude::RaftMessage;
use raft_proto::prelude::RaftMessageResponse;
use raft_proto::prelude::ReplicaDesc;
use raft_proto::prelude::SingleMembershipChange;

use tokio::time::interval_at;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::span;
use tracing::trace;
use tracing::warn;
use tracing::Instrument;
use tracing::Level;
use tracing::Span;

use crate::multiraft::error::RaftGroupError;
use crate::util::Stopper;
use crate::util::TaskGroup;

use super::apply_actor::Apply;
use super::apply_actor::ApplyActor;
// use super::apply_actor::ApplyActorReceiver;
// use super::apply_actor::ApplyActorSender;
use super::apply_actor::ApplyRequest;
use super::apply_actor::ApplyTask;
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
use super::raft_group::RaftGroupState;
use super::raft_group::RaftGroupWriteRequest;
use super::replica_cache::ReplicaCache;
use super::storage::MultiRaftStorage;
use super::transport;
use super::transport::Transport;
use super::util::Ticker;

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

pub struct MultiRaftActor<T, RS, MRS>
where
    T: Transport,
    RS: Storage,
    MRS: MultiRaftStorage<RS>,
{
    pub shard: ShardState,
    pub write_propose_tx: Sender<(AppWriteRequest, oneshot::Sender<Result<(), Error>>)>,
    pub read_index_propose_tx: Sender<(AppReadIndexRequest, oneshot::Sender<Result<(), Error>>)>,
    pub membership_change_tx: Sender<(MembershipChangeRequest, oneshot::Sender<Result<(), Error>>)>,
    pub campaign_tx: Sender<(u64, oneshot::Sender<Result<(), Error>>)>,
    pub raft_message_tx: Sender<(
        RaftMessage,
        oneshot::Sender<Result<RaftMessageResponse, Error>>,
    )>,
    pub admin_tx: Sender<(AdminMessage, oneshot::Sender<Result<(), Error>>)>,
    apply: ApplyActor,
    runtime: Mutex<Option<MultiRaftActorRuntime<T, RS, MRS>>>,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl<T, RS, MRS> MultiRaftActor<T, RS, MRS>
where
    T: Transport + Clone,
    RS: Storage + Clone + Sync + Send + 'static,
    MRS: MultiRaftStorage<RS>,
{
    pub fn new(cfg: &Config, transport: &T, storage: &MRS, event_tx: &Sender<Vec<Event>>) -> Self {
        let state = Arc::new(State::default());

        let (write_propose_tx, write_propose_rx) = channel(1);
        let (read_index_propose_tx, read_index_propose_rx) = channel(1);
        let (membership_change_tx, membership_change_rx) = channel(1);
        let (admin_tx, admin_rx) = channel(1);
        let (campaign_tx, campaign_rx) = channel(1);
        let (raft_message_tx, raft_message_rx) = channel(1);

        let (callback_tx, callback_rx) = channel(1);
        let (apply_request_tx, apply_request_rx) = channel(1);
        let (apply_response_tx, apply_response_rx) = channel(1);

        let runtime = MultiRaftActorRuntime {
            cfg: cfg.clone(),
            activity_groups: HashSet::new(),
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
            callback_rx,
            // last_tick: Instant::now(),
            // raft_ticks: 0,
            sync_replica_cache: true,
            replica_cache: ReplicaCache::new(storage.clone()),
            pending_events: Vec::new(),
            // pending_response_cbs: Vec::new(),
            _m1: PhantomData,
        };

        let apply = ApplyActor::new(
            cfg,
            apply_request_rx,
            apply_response_tx,
            callback_tx,
            event_tx,
        );

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

pub struct MultiRaftActorRuntime<T, RS, MRS>
where
    T: Transport,
    RS: Storage,
    MRS: MultiRaftStorage<RS>,
{
    cfg: Config,
    node_id: u64,
    state: Arc<State>,
    //  nodes: HashMap<u64, Node>,
    storage: MRS,
    transport: T,
    node_manager: NodeManager,
    replica_cache: ReplicaCache<RS, MRS>,
    sync_replica_cache: bool,
    groups: HashMap<u64, RaftGroup<RS>>,
    activity_groups: HashSet<u64>,
    // last_tick: Instant,
    tick_interval: Duration,
    // raft_ticks: usize,
    // election_tick: usize,
    // heartbeat_tick: usize,
    raft_message_rx: Receiver<(
        RaftMessage,
        oneshot::Sender<Result<RaftMessageResponse, Error>>,
    )>,
    write_propose_rx: Receiver<(AppWriteRequest, oneshot::Sender<Result<(), Error>>)>,
    read_index_propose_rx: Receiver<(AppReadIndexRequest, oneshot::Sender<Result<(), Error>>)>,
    membership_change_rx: Receiver<(MembershipChangeRequest, oneshot::Sender<Result<(), Error>>)>,
    admin_rx: Receiver<(AdminMessage, oneshot::Sender<Result<(), Error>>)>,
    campaign_rx: Receiver<(u64, oneshot::Sender<Result<(), Error>>)>,
    event_tx: Sender<Vec<Event>>,
    callback_rx: Receiver<CallbackEvent>,
    apply_request_tx: Sender<(Span, ApplyRequest)>,
    apply_response_rx: Receiver<ApplyTaskResponse>,
    // write_actor_address: WriteAddress,
    // waiting_ready_groups: VecDeque<HashMap<u64, Ready>>,
    // proposals: ProposalQueueManager,
    // task_group: TaskGroup,
    // pending_response_cbs: Vec<ResponseCb>,
    pending_events: Vec<Event>,

    _m1: PhantomData<RS>,
}

impl<T, RS, MRS> MultiRaftActorRuntime<T, RS, MRS>
where
    T: Transport + Clone,
    RS: Storage + Send + Sync + Clone + 'static,
    MRS: MultiRaftStorage<RS>,
{
    #[tracing::instrument(
        name = "MultiRaftActorRuntime::main_loop"
        level = Level::TRACE,
        skip_all,
        fields(node_id=self.node_id)
    )]
    async fn main_loop(mut self, mut stopper: Stopper, ticker: Option<Box<dyn Ticker>>) {
        info!("node {}: start main_loop", self.node_id);
        // Each time ticker expires, the ticks increments,
        // when ticks >= heartbeat_tick triggers the merged heartbeat.
        // tokio::time::Interval::interval_at(std::time::Instant::now() + self.tick_interval, self.tick_interval)
        let mut ticker: Box<dyn Ticker> = ticker.map_or(
            Box::new(interval_at(
                tokio::time::Instant::now() + self.tick_interval,
                self.tick_interval,
            )),
            |t| t,
        );

        let mut ticks = 0;
      
        // let mut ready_ticker = tokio::time::interval_at(
        //     Instant::now() + Duration::from_millis(1),
        //     Duration::from_millis(1),
        // );

        // self.raft_ticks = 0;
        // self.last_tick = Instant::now();

        // the callback responds to the client at the end of per main_loop.
        loop {
            let mut pending_response_callbacks = vec![];
            self.send_pending_events().await;
            // 1. if `on_groups_ready` is executed, `activity_groups` is empty until loop,
            //    and if the events treats the contained group as active, the group inserted
            //    into `activity_group`.

            tokio::select! {
                // Note: see https://github.com/tokio-rs/tokio/discussions/4019 for more
                // information about why mut here.
                _ = &mut stopper => {
                    self.do_stop();
                    break
                }

                Some((req, tx)) = self.raft_message_rx.recv() => {
                    let response_cb: ResponseCb = match self.handle_message(req).await {
                        Ok(res) => Box::new(move || -> Result<(), Error> {
                            tx.send(Ok(res)).map_err(|_| {
                                Error::Internal(format!(
                                    "response RaftMessageReuqest error, receiver dropped"
                                ))
                            })
                        }),
                        Err(err) => Box::new(move || -> Result<(), Error> {
                            tx.send(Err(err)).map_err(|_| {
                                Error::Internal(format!(
                                    "response RaftMessageReuqest error, receiver dropped"
                                ))
                            })
                        }),
                    };
                    pending_response_callbacks.push(response_cb);
                    // self.pending_response_cbs.push(response_cb);
                },

                _ = ticker.recv() => {
                    self.groups.iter_mut().for_each(|(id, group)| {
                        if group.raft_group.tick() {
                            // self.activity_groups.insert(group.group_id);
                        }
                        self.activity_groups.insert(group.group_id);
                    });

                    ticks += 1;
                    if ticks >= self.cfg.heartbeat_tick {
                        ticks = 0;
                        self.coalesced_heratbeat().await;
                    }
                },

                Some((req, tx)) = self.write_propose_rx.recv() => {
                    if let Some(cb) = self.propose_write_request(req, tx) {
                        // self.pending_response_cbs.push(cb)
                        pending_response_callbacks.push(cb);
                    }
                },

                Some((req, tx)) = self.membership_change_rx.recv() => {
                    self.activity_groups.insert(req.group_id);
                    if let Some(cb) = self.propose_membership_change_request(req, tx) {
                        // self.pending_response_cbs.push(cb)
                        pending_response_callbacks.push(cb);
                    }
                },

                Some((req, tx)) = self.read_index_propose_rx.recv() => {
                    if let Some(cb) = self.propose_read_index_request(req, tx) {
                        // self.pending_response_cbs.push(cb)
                        pending_response_callbacks.push(cb);
                    }
                },

                Some(res) = self.apply_response_rx.recv() => {
                    self.handle_apply_response(res).await;
                },

                Some((req, tx)) = self.admin_rx.recv() => {
                     if let Some(cb) = self.handle_admin_request(req, tx).await {
                        // self.pending_response_cbs.push(cb)
                        pending_response_callbacks.push(cb);
                    }
                },

                Some((group_id, tx)) = self.campaign_rx.recv() => {
                    self.campaign_raft(group_id, tx)
                }

                Some(msg) = self.callback_rx.recv() => {
                    self.handle_callback(msg).await;
                },

                // _ = ready_ticker.tick() => {
                //     if !self.activity_groups.is_empty() {
                //         self.handle_readys().await;
                //         self.activity_groups.clear();
                //         // TODO: shirk_to_fit
                //     }
                // },
                else => {},
            }

            if !self.activity_groups.is_empty() {
                self.handle_readys().await;
                self.activity_groups.clear();
                // TODO: shirk_to_fit
            }

            handle_response_callbacks(pending_response_callbacks);
        }
    }

    /// The node sends heartbeats to other nodes instead
    /// of all raft groups on that node.
    async fn coalesced_heratbeat(&self) {
        for (node_id, _) in self.node_manager.iter() {
            if *node_id == self.node_id {
                continue;
            }

            // debug!(
            //     "trigger node coalesced heartbeta {} -> {} ",
            //     self.node_id, *node_id
            // );

            // coalesced heartbeat to all nodes. the heartbeat message is node
            // level message so from and to set 0 when sending, and the specific
            // value is set by message receiver.
            let mut raft_msg = Message::default();
            // FIXME: should add commit information...
            raft_msg.set_msg_type(MessageType::MsgHeartbeat);
            let msg = RaftMessage {
                group_id: NO_GORUP,
                from_node: self.node_id,
                to_node: *node_id,
                replicas: vec![],
                msg: Some(raft_msg),
            };

            if let Err(_error) = self.transport.send(msg) {}
        }
    }

    async fn handle_message(&mut self, msg: RaftMessage) -> Result<RaftMessageResponse, Error> {
        match msg.msg.as_ref().unwrap().msg_type() {
            MessageType::MsgHeartbeat => self.fanout_heartbeat(msg).await,
            MessageType::MsgHeartbeatResponse => self.fanout_heartbeat_response(msg).await,
            _ => self.handle_raft_message(msg).await,
        }
    }

    #[tracing::instrument(
        name = "MultiRaftActorRuntime::handle_raft_message",
        level = Level::TRACE,
        skip(self),
    )]
    async fn handle_raft_message(
        &mut self,
        mut msg: RaftMessage,
    ) -> Result<RaftMessageResponse, Error> {
        let raft_msg = msg.msg.take().expect("invalid message");
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
                trace!(
                    "node {}: got message for unknow group {}; creating replica {}",
                    self.node_id,
                    group_id,
                    to_replica.replica_id
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
                        error!("create raft group error {}", err);
                        err
                    })?;

                self.groups.get_mut(&group_id).unwrap()
            }
        };

        // FIXME: t30_membership single_step
        group.raft_group.step(raft_msg).unwrap();
        self.activity_groups.insert(group_id);
        Ok(RaftMessageResponse {})
    }

    /// Fanout heartbeats from other nodes to all raft groups on this node.
    async fn fanout_heartbeat(&mut self, msg: RaftMessage) -> Result<RaftMessageResponse, Error> {
        let from_node_id = msg.from_node;
        let to_node_id = msg.to_node;
        if let Some(from_node) = self.node_manager.get_node(&from_node_id) {
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
                self.activity_groups.insert(*group_id);

                // if group.leader.node_id == 0
                // || group.leader.node_id != msg.from_node
                if group.leader.node_id != from_node_id || msg.from_node == self.node_id {
                    trace!("node {}: not fanning out heartbeat to {}, msg is from {} and leader is {:?}", self.node_id, group_id, from_node_id, group.leader);
                    continue;
                }

                // gets the replica stored in this node.
                // FIXME: t30_membership single_step
                // let from_replica = match self
                //     .storage
                //     .replica_for_node(*group_id, msg.from_node)
                //     .await
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
                // let to_replica = match self.storage.replica_for_node(*group_id, msg.to_node).await {
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
                // group
                //     .raft_group
                //     .raft
                //     .prs()
                //     .iter()
                //     .for_each(|(id, _)| println!("prs = {}", *id));
                // let (_, pr) = group
                //     .raft_group
                //     .raft
                //     .prs()
                //     .iter()
                //     .find(|&(id, _)| {
                //         println!("id = {}", *id);
                //         *id == from_replica.replica_id
                //     })
                //     .unwrap();
                // msg.commit = std::cmp::min(group.raft_group.raft.raft_log.committed, pr.matched);
                debug!(
                    "node {}: group.raft_group.raft.raft_log.committed = {}",
                    self.node_id, group.raft_group.raft.raft_log.committed
                );
                step_msg.term = group.raft_group.raft.term; // FIX(t30_membership::test_remove)
                step_msg.commit = group.raft_group.raft.raft_log.committed;
                step_msg.from = from_replica.replica_id;
                step_msg.to = to_replica.replica_id;
                debug!(
                    "fanouting {}.{} -> {}.{} msg  = {:?}",
                    from_node_id, step_msg.from, to_node_id, step_msg.to, step_msg
                );
                // group.raft_group.step(msg).unwrap();
                // FIXME: t30_membership single_step
                group.raft_group.step(step_msg).unwrap();
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
                to_node: from_node_id,
                replicas: vec![],
                msg: Some(raft_msg),
            }
        };

        self.transport.send(response_msg).unwrap();
        Ok(RaftMessageResponse {})
    }

    /// Fanout heartbeats response from other nodes to all raft groups on this node.
    async fn fanout_heartbeat_response(
        &mut self,
        msg: RaftMessage,
    ) -> Result<RaftMessageResponse, Error> {
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

                self.activity_groups.insert(*group_id);

                // TODO: check leader is valid
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
                msg.from = from_replica.replica_id;
                msg.to = to_replica.replica_id;
                debug!("step msg = {:?}", msg);

                // group.raft_group.step(msg).unwrap();
                // FIXME: t30_membership single_step
                group.raft_group.step(msg).unwrap();
            }
        } else {
            warn!(
                "missing raft groups at from_node {} fanout heartbeat response",
                msg.from_node
            );
            self.node_manager.add_node(msg.from_node, NO_GORUP);
        }
        Ok(RaftMessageResponse {})
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
        tx: oneshot::Sender<Result<(), Error>>,
    ) -> Option<ResponseCb> {
        let group_id = request.group_id;
        match self.groups.get_mut(&group_id) {
            None => {
                // TODO: process group deleted, we need remove pending proposals.
                warn!(
                    "client dropped before returning the write response, request = {:?}",
                    request
                );
                return Some(new_response_error_callback(
                    tx,
                    Error::RaftGroup(RaftGroupError::NotExist(self.node_id, group_id)),
                ));
            }
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
        skip(self, tx))
    ]
    fn propose_membership_change_request(
        &mut self,
        request: MembershipChangeRequest,
        tx: oneshot::Sender<Result<(), Error>>,
    ) -> Option<ResponseCb> {
        let group_id = request.group_id;
        match self.groups.get_mut(&group_id) {
            None => {
                // TODO: process group deleted, we need remove pending proposals.
                warn!(
                    "client dropped before returning the write response, request = {:?}",
                    request
                );
                return Some(new_response_error_callback(
                    tx,
                    Error::RaftGroup(RaftGroupError::NotExist(self.node_id, group_id)),
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
                // TODO: process group deleted, we need remove pending proposals.
                warn!(
                    "client dropped before returning the read_index response, request = {:?}",
                    request
                );
                return Some(new_response_error_callback(
                    tx,
                    Error::RaftGroup(RaftGroupError::NotExist(self.node_id, group_id)),
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
            self.activity_groups.insert(group_id);
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

    // async fn handle_admin(&mut self) {
    //     let (msg, tx) = match self.admin_rx.try_recv() {
    //         Ok(msg) => msg,
    //         Err(TryRecvError::Empty) => return,
    //         Err(TryRecvError::Disconnected) => {
    //             error!("write_propose sender of channel dropped");
    //             return;
    //         }
    //     };
    //     if let Some(cb) = self.handle_admin_request(msg, tx).await {
    //         self.pending_response_cbs.push(cb)
    //     }
    // }

    #[tracing::instrument(
        name = "MultiRaftActorRuntime::handle_admin_request",
        level = Level::INFO,
        skip(self, tx)
    )]
    async fn handle_admin_request(
        &mut self,
        msg: AdminMessage,
        tx: oneshot::Sender<Result<(), Error>>,
    ) -> Option<ResponseCb> {
        let res = match msg.msg_type() {
            // handle raft group management request
            AdminMessageType::RaftGroup => {
                let msg = msg.raft_group.unwrap();
                self.handle_raft_group_management(msg).await
            }
        };

        return Some(new_response_callback(tx, res));
    }

    #[tracing::instrument(
        name = "MultiRaftActorRuntime::raft_group_management",
        level = Level::INFO,
        skip(self)
    )]
    async fn handle_raft_group_management(
        &mut self,
        msg: RaftGroupManagement,
    ) -> Result<(), Error> {
        match msg.msg_type() {
            RaftGroupManagementType::MsgCreateGroup => {
                self.activity_groups.insert(msg.group_id);
                self.create_raft_group(msg.group_id, msg.replica_id, msg.replicas)
                    .await
            }
            RaftGroupManagementType::MsgRemoveGoup => unimplemented!(),
        }
    }

    #[tracing::instrument(
        name = "MultiRaftActorRuntime::create_raft_group", 
        level = Level::INFO,
        skip(self))
    ]
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
        let applied = 0; // TODO: get applied from stroage
        let raft_cfg = raft::Config {
            id: replica_id,
            applied,
            election_tick: self.cfg.election_tick,
            heartbeat_tick: self.cfg.heartbeat_tick,
            // max_size_per_msg: 1024 * 1024,
            // max_inflight_msgs: 256,
            ..Default::default()
        };

        let raft_store = group_storage.clone();
        let raft_group = raft::RawNode::with_default_logger(&raft_cfg, raft_store)
            .map_err(|err| Error::Raft(err))?;

        println!(
            "node {}: create raft group_id = {}, replica_id = {}",
            self.node_id, group_id, replica_id
        );
        let mut group = RaftGroup {
            group_id,
            replica_id,
            raft_group,
            node_ids: Vec::new(),
            proposals: GroupProposalQueue::new(replica_id),
            leader: ReplicaDesc::default(), // TODO: init leader from storage
            committed_term: 0,              // TODO: init committed term from storage
            state: RaftGroupState::default(),
            batch_apply: self.cfg.batch_apply,
            batch_size: self.cfg.batch_size,
            pending_apply: None,
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

    async fn send_pending_events(&mut self) {
        if !self.pending_events.is_empty() {
            println!("send pending events = {:?}", self.pending_events);
            // updating all valid LederElections is required because the lastest commited_term
            // of the follower cannot be updated until after the leader committed.
            let pending_events = std::mem::take(&mut self.pending_events);
            // TODO: move to leader committed event
            //     .into_iter()
            //     .filter_map(|mut pending_event| {
            //         match pending_event {
            //             Event::LederElection(ref mut leader_elect) => {
            //                 if let Some(group) = self.groups.get(&leader_elect.group_id) {
            //                     if leader_elect.committed_term != group.committed_term
            //                         && group.committed_term != 0
            //                     {
            //                         leader_elect.committed_term = group.committed_term;
            //                     }
            //                     Some(pending_event)
            //                 } else {
            //                     // group is removed, but event incoming, so ignore it.
            //                     None
            //                 }
            //             }
            //             _ => Some(pending_event),
            //         }
            //     })
            //     .collect::<Vec<_>>();

            self.event_tx.send(pending_events).await; // FIXME: handle error
        }
    }

    #[tracing::instrument(
        level = Level::TRACE,
        name = "MultiRaftActorRuntime::handle_apply_response",
        skip(self))
    ]
    async fn handle_apply_response(&mut self, response: ApplyTaskResponse) {
        for (group_id, apply_result) in response.apply_results {
            let group = match self.groups.get_mut(&group_id) {
                Some(group) => group,
                None => {
                    warn!("group {} removed, skip apply", group_id);
                    continue;
                }
            };

            // set apply state
            group.state.apply_state = apply_result.apply_state;
            println!(
                "node {}: apply state change = {:?}, group = {}",
                self.node_id, group.state.apply_state, group_id
            );
            group.raft_group.advance_apply();
        }
    }

    async fn handle_callback(&mut self, callback_event: CallbackEvent) {
        trace!("handle callback = {:?}", callback_event);
        match callback_event {
            CallbackEvent::None => return,
            CallbackEvent::MembershipChange(view, response_tx) => {
                let res = self.apply_membership_change_view(view).await;
                response_tx.send(res).unwrap();
            }
        }
    }

    async fn apply_membership_change_view(
        &mut self,
        view: MembershipChangeView,
    ) -> Result<(), Error> {
        assert_eq!(
            view.change_request.changes.len(),
            view.conf_change.changes.len()
        );

        let group_id = view.change_request.group_id;

        let group = self.groups.get_mut(&group_id).map_or(
            Err(Error::RaftGroup(RaftGroupError::NotExist(
                self.node_id,
                group_id,
            ))),
            |group| Ok(group),
        )?;

        for (conf_change, change_request) in view
            .conf_change
            .changes
            .iter()
            .zip(view.change_request.changes.iter())
        {
            match conf_change.change_type() {
                ConfChangeType::AddNode => {
                    MultiRaftActorRuntime::<T, RS, MRS>::membership_add(
                        self.node_id,
                        group_id,
                        change_request,
                        &mut self.node_manager,
                        &mut self.replica_cache,
                    )
                    .await?
                }
                ConfChangeType::RemoveNode => {
                    MultiRaftActorRuntime::<T, RS, MRS>::membership_remove(
                        self.node_id,
                        group,
                        change_request,
                        &mut self.node_manager,
                        &mut self.replica_cache,
                    )
                    .await?
                }
                ConfChangeType::AddLearnerNode => unimplemented!(),
            }
        }

        // apply to raft
        let conf_state = group
            .raft_group
            .apply_conf_change(&view.conf_change)
            .map_err(|raft_err| Error::Raft(raft_err))?;

        let gs = &self
            .storage
            .group_storage(group_id, group.replica_id)
            .await?;
        gs.set_conf_state(conf_state);

        return Ok(());
    }

    async fn membership_add(
        node_id: u64,
        group_id: u64,
        change: &SingleMembershipChange,
        node_manager: &mut NodeManager,
        replica_cache: &mut ReplicaCache<RS, MRS>,
    ) -> Result<(), Error> {
        let (node_id, replica_id) = (change.node_id, change.replica_id);
        node_manager.add_group(node_id, group_id);

        // TODO: this call need transfer to user call, and if user call return errored,
        // the membership change should failed and user need to retry.
        // we need a channel to provider user notify actor it need call these code.
        // and we recv the notify can executing these code, if executed failed, we
        // response to user and membership change is failed.
        let replica_desc = ReplicaDesc {
            node_id,
            replica_id,
        };
        if let Err(err) = replica_cache
            .cache_replica_desc(group_id, replica_desc, true)
            .await
        {
            warn!(
                "node {}: the membership change request to add replica desc error: {} ",
                node_id, err
            );
        }

        Ok(())
    }

    async fn membership_remove(
        node_id: u64,
        group: &mut RaftGroup<RS>,
        change_request: &SingleMembershipChange,
        node_manager: &mut NodeManager,
        _replica_cache: &mut ReplicaCache<RS, MRS>,
    ) -> Result<(), Error> {
        let _ = group.remove_pending_proposals();
        let _ = group.remove_track_node(change_request.node_id);
        println!(
            "node {}: remove group = {} tracked node = {}",
            node_id, group.group_id, change_request.node_id
        );
        let _ = node_manager.remove_group(change_request.node_id, group.group_id);
        println!(
            "node {}: remove node = {} of group = {} on node manager",
            node_id, change_request.node_id, group.group_id,
        );

        // TODO: remove replica desc cache
        Ok(())
    }

    async fn handle_readys(&mut self) {
        let mut multi_groups_write = HashMap::new();
        let mut applys = HashMap::new();
        for group_id in self.activity_groups.iter() {
            if *group_id == NO_GORUP {
                continue;
            }

            if let Some(group) = self.groups.get_mut(group_id) {
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
        if !applys.is_empty() {
            self.send_applys(applys).await;
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
            self.send_applys(multi_groups_apply).await;
        }
    }
    // #[tracing::instrument(
    //     name = "MultiRaftActorInner::handle_response_callbacks"
    //     level = Level::TRACE,
    //     skip_all
    // )]

    #[tracing::instrument(
        name = "MultiRaftActorRuntime::send_applys",
        level = Level::TRACE,
        skip_all,
    )]
    async fn send_applys(&self, applys: HashMap<u64, ApplyTask>) {
        let span = tracing::span::Span::current();
        if let Err(_err) = self
            .apply_request_tx
            .send((span.clone(), ApplyRequest { groups: applys }))
            // .instrument(tracing::trace_span!("multi raft group apply"))
            .await
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

fn handle_response_callbacks(mut cbs: Vec<ResponseCb>) {
    let drainner = cbs.drain(..).collect::<Vec<_>>();
    tokio::spawn(async move {
        drainner.into_iter().for_each(|cb| {
            if let Err(err) = cb() {
                warn!("{}", err)
            }
        });
    });
    // self.pending_response_cbs.drain(..).for_each(|cb| {
    //     if let Err(err) = cb() {
    //         error!("{}", err)
    //     }
    // });
}
