use std::collections::hash_map::HashMap;
use std::collections::hash_map::Iter;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::time::Duration;

use raft::Storage;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;
use tracing::Level;
use tracing::Span;

use crate::prelude::ConfChangeType;
use crate::prelude::Message;
use crate::prelude::MessageType;
use crate::prelude::MultiRaftMessage;
use crate::prelude::MultiRaftMessageResponse;
use crate::prelude::ReplicaDesc;
use crate::prelude::SingleMembershipChange;
use crate::util::Stopper;
use crate::util::TaskGroup;

use super::apply::ApplyActor;
use super::config::Config;
use super::error::ChannelError;
use super::error::Error;
use super::error::RaftGroupError;
use super::event::Event;
use super::event::EventChannel;
use super::group::RaftGroup;
use super::group::RaftGroupState;
use super::group::RaftGroupWriteRequest;
use super::group::Status;
use super::msg::ApplyCommitMessage;
use super::msg::ApplyData;
use super::msg::ApplyMessage;
use super::msg::ApplyResultMessage;
use super::msg::CommitMembership;
use super::msg::GroupData;
use super::msg::GroupOp;
use super::msg::ManageMessage;
use super::msg::ProposeMessage;
use super::multiraft::NO_GORUP;
use super::multiraft::NO_NODE;
use super::proposal::ProposalQueue;
use super::proposal::ReadIndexQueue;
use super::replica_cache::ReplicaCache;
use super::response::AppWriteResponse;
use super::rsm::StateMachine;
use super::storage::MultiRaftStorage;
use super::transport::Transport;
use super::util::Ticker;

/// Shrink queue if queue capacity more than and len less than
/// this value.
const SHRINK_CACHE_CAPACITY: usize = 64;

pub(crate) type ResponseCallback = Box<dyn FnOnce() -> Result<(), Error> + Send + Sync + 'static>;

pub(crate) struct ResponseCallbackQueue {
    cbs: VecDeque<ResponseCallback>,
}

impl ResponseCallbackQueue {
    pub(crate) fn new() -> Self {
        Self {
            cbs: VecDeque::new(),
        }
    }

    pub(crate) fn new_callback<T: Send + Sync + 'static>(
        tx: oneshot::Sender<Result<T, Error>>,
        res: Result<T, Error>,
    ) -> ResponseCallback {
        Box::new(move || -> Result<(), Error> {
            tx.send(res).map_err(|_| {
                Error::Channel(ChannelError::ReceiverClosed(
                    "channel receiver closed for response client".to_owned(),
                ))
            })
        })
    }

    pub(crate) fn new_error_callback<T: Send + Sync + 'static>(
        tx: oneshot::Sender<Result<T, Error>>,
        err: Error,
    ) -> ResponseCallback {
        Box::new(move || -> Result<(), Error> {
            tx.send(Err(err)).map_err(|_| {
                Error::Channel(ChannelError::ReceiverClosed(
                    "channel receiver closed for response client".to_owned(),
                ))
            })
        })
    }

    #[inline]
    pub(crate) fn push_back(&mut self, cb: ResponseCallback) {
        self.cbs.push_back(cb);
    }

    fn try_gc(&mut self) {
        // TODO: think move the shrink_to_fit operation  to background task?
        if self.cbs.capacity() > SHRINK_CACHE_CAPACITY && self.cbs.len() < SHRINK_CACHE_CAPACITY {
            self.cbs.shrink_to_fit();
        }
    }

    pub(crate) fn flush(&mut self) {
        let cbs = self.cbs.drain(..).collect::<Vec<_>>();
        self.try_gc();
        tokio::spawn(async move {
            for cb in cbs {
                if let Err(err) = cb() {
                    warn!("{}", err)
                }
            }
        });
    }
}

/// Node represents a physical node and contains a group of rafts.
#[derive(Debug)]
pub struct Node {
    pub node_id: u64,
    pub group_map: HashMap<u64, ()>,
}

pub struct NodeManager {
    pub nodes: HashMap<u64, Node>,
}

impl NodeManager {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    #[inline]
    pub fn iter(&self) -> Iter<'_, u64, Node> {
        self.nodes.iter()
    }

    #[inline]
    pub fn contains_node(&self, node_id: &u64) -> bool {
        self.nodes.contains_key(node_id)
    }

    #[inline]
    pub fn get_node(&self, node_id: &u64) -> Option<&Node> {
        self.nodes.get(node_id)
    }

    pub fn add_node(&mut self, node_id: u64) {
        if self.nodes.get_mut(&node_id).is_none() {
            self.nodes.insert(
                node_id,
                Node {
                    node_id,
                    group_map: HashMap::new(),
                },
            );
        }
    }

    pub(crate) fn add_group(&mut self, node_id: u64, group_id: u64) {
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

    pub fn remove_group(&mut self, node_id: u64, group_id: u64) {
        let node = match self.nodes.get_mut(&node_id) {
            None => return,
            Some(node) => node,
        };

        node.group_map.remove(&group_id);
    }
}

pub struct NodeActor<RES: AppWriteResponse> {
    // TODO: queue should have one per-group.
    pub propose_tx: Sender<ProposeMessage<RES>>,
    pub campaign_tx: Sender<(u64, oneshot::Sender<Result<(), Error>>)>,
    pub raft_message_tx: Sender<(
        MultiRaftMessage,
        oneshot::Sender<Result<MultiRaftMessageResponse, Error>>,
    )>,
    pub manage_tx: Sender<ManageMessage>,
    #[allow(unused)]
    apply: ApplyActor,
}

impl<RES: AppWriteResponse> NodeActor<RES> {
    pub fn spawn<TR, RS, MRS, RSM, TK>(
        cfg: &Config,
        transport: &TR,
        storage: &MRS,
        rsm: RSM,
        event_bcast: &EventChannel,
        task_group: &TaskGroup,
        ticker: Option<TK>,
    ) -> Self
    where
        TR: Transport + Clone,
        RS: Storage + Clone + Sync + Send + 'static,
        MRS: MultiRaftStorage<RS>,
        RSM: StateMachine<RES>,
        TK: Ticker,
    {
        let (propose_tx, propose_rx) = channel(cfg.proposal_queue_size);
        let (manage_tx, manage_rx) = channel(1);
        let (campaign_tx, campaign_rx) = channel(1);
        let (raft_message_tx, raft_message_rx) = channel(10);

        let (commit_tx, commit_rx) = unbounded_channel();

        let (apply_request_tx, apply_request_rx) = unbounded_channel();
        let (apply_response_tx, apply_response_rx) = unbounded_channel();

        let apply = ApplyActor::spawn(
            cfg,
            rsm,
            apply_request_rx,
            apply_response_tx,
            commit_tx,
            task_group,
        );

        let worker = NodeWorker::<TR, RS, MRS, RES>::new(
            cfg,
            transport,
            storage,
            propose_rx,
            campaign_rx,
            raft_message_rx,
            apply_request_tx,
            apply_response_rx,
            manage_rx,
            event_bcast,
            commit_rx,
        );

        let stopper = task_group.stopper();
        task_group.spawn(async move {
            worker.main_loop(stopper, ticker).await;
        });

        Self {
            raft_message_tx,
            propose_tx,
            campaign_tx,
            manage_tx,
            apply,
        }
    }
}

pub struct NodeWorker<TR, RS, MRS, RES>
where
    TR: Transport,
    RS: Storage,
    MRS: MultiRaftStorage<RS>,
    RES: AppWriteResponse,
{
    cfg: Config,
    node_id: u64,
    storage: MRS,
    transport: TR,
    node_manager: NodeManager,
    replica_cache: ReplicaCache<RS, MRS>,
    groups: HashMap<u64, RaftGroup<RS, RES>>,
    active_groups: HashSet<u64>,
    pending_responses: ResponseCallbackQueue,
    event_chan: EventChannel,
    multiraft_message_rx: Receiver<(
        MultiRaftMessage,
        oneshot::Sender<Result<MultiRaftMessageResponse, Error>>,
    )>,
    propose_rx: Receiver<ProposeMessage<RES>>,
    manage_rx: Receiver<ManageMessage>,
    campaign_rx: Receiver<(u64, oneshot::Sender<Result<(), Error>>)>,
    commit_rx: UnboundedReceiver<ApplyCommitMessage>,
    apply_tx: UnboundedSender<(Span, ApplyMessage<RES>)>,
    apply_result_rx: UnboundedReceiver<ApplyResultMessage>,
}

impl<TR, RS, MRS, RES> NodeWorker<TR, RS, MRS, RES>
where
    TR: Transport + Clone,
    RS: Storage + Send + Sync + Clone + 'static,
    MRS: MultiRaftStorage<RS>,
    RES: AppWriteResponse,
{
    fn new(
        cfg: &Config,
        transport: &TR,
        storage: &MRS,
        propose_rx: Receiver<ProposeMessage<RES>>,
        campaign_rx: Receiver<(u64, oneshot::Sender<Result<(), Error>>)>,
        raft_message_rx: Receiver<(
            MultiRaftMessage,
            oneshot::Sender<Result<MultiRaftMessageResponse, Error>>,
        )>,
        apply_request_tx: UnboundedSender<(Span, ApplyMessage<RES>)>,
        apply_response_rx: UnboundedReceiver<ApplyResultMessage>,
        manage_rx: Receiver<ManageMessage>,
        event_chan: &EventChannel,
        commit_rx: UnboundedReceiver<ApplyCommitMessage>,
    ) -> Self {
        NodeWorker::<TR, RS, MRS, RES> {
            cfg: cfg.clone(),
            node_id: cfg.node_id,
            node_manager: NodeManager::new(),
            groups: HashMap::new(),
            propose_rx: propose_rx,
            campaign_rx,
            multiraft_message_rx: raft_message_rx,
            manage_rx,
            storage: storage.clone(),
            transport: transport.clone(),
            apply_tx: apply_request_tx,
            apply_result_rx: apply_response_rx,
            commit_rx,
            active_groups: HashSet::new(),
            replica_cache: ReplicaCache::new(storage.clone()),
            event_chan: event_chan.clone(),
            pending_responses: ResponseCallbackQueue::new(),
        }
    }

    #[tracing::instrument(
        name = "NodeActor::main_loop"
        level = Level::TRACE,
        skip_all,
        fields(node_id=self.node_id)
    )]
    async fn main_loop<TK>(mut self, mut stopper: Stopper, ticker: Option<TK>)
    where
        TK: Ticker,
    {
        info!("node {}: start multiraft main_loop", self.node_id);

        let tick_interval = Duration::from_millis(self.cfg.tick_interval);

        let mut ticker = ticker.map_or(
            TK::new(std::time::Instant::now() + tick_interval, tick_interval),
            |t| t,
        );

        let mut ticks = 0;
        loop {
            self.event_chan.flush();

            tokio::select! {
                // Note: see https://github.com/tokio-rs/tokio/discussions/4019 for more
                // information about why mut here.
                _ = &mut stopper => {
                    self.do_stop();
                    break
                }

                Some((req, tx)) = self.multiraft_message_rx.recv() => {
                    let res = self.handle_multiraft_message(req).await ;
                    self.pending_responses.push_back(ResponseCallbackQueue::new_callback(tx, res));
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
                        self.incorporate_heatbeats();
                    }
                },

                Some(req) = self.propose_rx.recv() => if let Some(cb) = self.handle_propose(req) {
                    self.pending_responses.push_back(cb);
                },

                Some(res) = self.apply_result_rx.recv() =>  self.handle_apply_result(res).await,

                Some(msg) = self.manage_rx.recv() => if let Some(cb) = self.handle_manage_message(msg).await {
                    self.pending_responses.push_back(cb);
                },

                Some((group_id, tx)) = self.campaign_rx.recv() => {
                    self.campaign_raft(group_id, tx);
                    self.active_groups.insert(group_id);
                }

                Some(msg) = self.commit_rx.recv() => self.handle_apply_commit(msg).await,

                else => {},
            }

            if !self.active_groups.is_empty() {
                self.handle_readys().await;
                /* here is active groups already drained */
            }

            self.pending_responses.flush();
        }
    }

    /// The node sends heartbeats to other nodes instead
    /// of all raft groups on that node.
    fn incorporate_heatbeats(&self) {
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
        let rmsg = msg.msg.as_ref().expect("invalid msg");
        // for a heartbeat message, fanout is executed only if context in
        // the heartbeat message is empty.
        match rmsg.msg_type() {
            MessageType::MsgHeartbeat if rmsg.context.is_empty() => {
                self.fanout_heartbeat(msg).await
            }
            MessageType::MsgHeartbeatResponse if rmsg.context.is_empty() => {
                self.fanout_heartbeat_response(msg).await
            }
            _ => self.handle_raft_message(msg).await,
        }
    }

    #[tracing::instrument(
        name = "NodeActor::handle_raft_message",
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
            .cache_replica_desc(group_id, from_replica.clone(), self.cfg.replica_sync)
            .await?;

        let _ = self
            .replica_cache
            .cache_replica_desc(group_id, to_replica.clone(), self.cfg.replica_sync)
            .await?;

        if !self.node_manager.contains_node(&from_replica.node_id) {
            self.node_manager.add_group(from_replica.node_id, group_id);
        }

        // if a group exists, try to maintain groups on the node
        if self.groups.contains_key(&group_id)
            && !self.node_manager.contains_node(&to_replica.node_id)
        {
            self.node_manager.add_group(to_replica.node_id, group_id);
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
            self.node_manager.add_node(msg.from_node);
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
            self.node_manager.add_node(msg.from_node);
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
        name = "NodeActor::handle_propose",
        skip_all
    )]
    fn handle_propose(&mut self, msg: ProposeMessage<RES>) -> Option<ResponseCallback> {
        match msg {
            ProposeMessage::WriteData(data) => {
                let group_id = data.group_id;
                match self.groups.get_mut(&group_id) {
                    None => {
                        warn!(
                            "node {}: proposal failed, group {} does not exists",
                            self.node_id, group_id,
                        );
                        return Some(ResponseCallbackQueue::new_error_callback(
                            data.tx,
                            Error::RaftGroup(RaftGroupError::Deleted(self.node_id, group_id)),
                        ));
                    }
                    Some(group) => {
                        self.active_groups.insert(group_id);
                        group.propose_write(data)
                    }
                }
            }
            ProposeMessage::MembershipData(request, tx) => {
                let group_id = request.group_id;
                match self.groups.get_mut(&group_id) {
                    None => {
                        warn!(
                            "node {}: proposal membership failed, group {} does not exists",
                            self.node_id, group_id,
                        );
                        return Some(ResponseCallbackQueue::new_error_callback(
                            tx,
                            Error::RaftGroup(RaftGroupError::Deleted(self.node_id, group_id)),
                        ));
                    }
                    Some(group) => {
                        self.active_groups.insert(group_id);
                        group.propose_membership_change(request, tx)
                    }
                }
            }
            ProposeMessage::ReadIndexData(read_data) => {
                let group_id = read_data.group_id;
                match self.groups.get_mut(&group_id) {
                    None => {
                        warn!(
                            "node {}: proposal read_index failed, group {} does not exists",
                            self.node_id, group_id,
                        );
                        return Some(ResponseCallbackQueue::new_error_callback(
                            read_data.tx,
                            Error::RaftGroup(RaftGroupError::Deleted(self.node_id, group_id)),
                        ));
                    }
                    Some(group) => {
                        self.active_groups.insert(group_id);
                        group.read_index_propose(read_data)
                    }
                }
            }
        }
    }

    #[tracing::instrument(
        level = Level::TRACE,
        name = "NodeActor::campagin_raft", 
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

    #[tracing::instrument(
        name = "NodeActor::handle_admin_message",
        level = Level::TRACE,
        skip_all,
    )]
    async fn handle_manage_message(&mut self, msg: ManageMessage) -> Option<ResponseCallback> {
        match msg {
            // handle raft group management request
            ManageMessage::GroupData(data) => self.handle_group_manage(data).await,
        }
    }

    #[tracing::instrument(
        name = "MultiRaftActorRuntime::raft_group_management",
        level = Level::TRACE,
        skip_all
    )]
    async fn handle_group_manage(&mut self, data: GroupData) -> Option<ResponseCallback> {
        let tx = data.tx;
        let res = match data.op {
            GroupOp::Create => {
                self.active_groups.insert(data.group_id);
                self.create_raft_group(
                    data.group_id,
                    data.replica_id,
                    data.replicas.map_or(vec![], |rs| rs),
                )
                .await
            }
            GroupOp::Remove => {
                // marke delete
                let group_id = data.group_id;
                let group = match self.groups.get_mut(&group_id) {
                    None => return Some(ResponseCallbackQueue::new_callback(tx, Ok(()))),
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

        return Some(ResponseCallbackQueue::new_callback(tx, res));
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
            return Err(Error::BadParameter(
                "group id must be more than 0".to_owned(),
            ));
        }

        if replica_id == 0 {
            return Err(Error::BadParameter(
                "replica id must be more than 0".to_owned(),
            ));
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
            max_size_per_msg: self.cfg.max_size_per_msg,
            max_inflight_msgs: self.cfg.max_inflight_msgs,
            batch_append: self.cfg.batch_append,
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
            node_id: self.cfg.node_id,
            group_id,
            replica_id,
            raft_group,
            node_ids: Vec::new(),
            proposals: ProposalQueue::new(replica_id),
            leader: ReplicaDesc::default(), // TODO: init leader from storage
            committed_term: 0,              // TODO: init committed term from storage
            state: RaftGroupState::default(),
            status: Status::None,
            read_index_queue: ReadIndexQueue::new(),
        };

        for replica_desc in replicas_desc.iter() {
            self.replica_cache
                .cache_replica_desc(group_id, replica_desc.clone(), true)
                .await?;
            // track the nodes which other members of the raft consensus group
            group.add_track_node(replica_desc.node_id);
            self.node_manager.add_group(replica_desc.node_id, group_id);
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
                self.node_manager.add_group(replica_desc.node_id, group_id);
            }
        }
        self.groups.insert(group_id, group);

        self.event_chan.push(Event::GroupCreate {
            group_id,
            replica_id,
        });
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
        name = "NodeActor::handle_apply_result",
        skip(self))
    ]
    async fn handle_apply_result(&mut self, response: ApplyResultMessage) {
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
        debug!(
            "node {}: group = {} apply state change = {:?}",
            self.node_id, response.group_id, response.apply_state
        );

        group.state.apply_state = response.apply_state;
    }

    async fn handle_apply_commit(&mut self, commit: ApplyCommitMessage) {
        match commit {
            ApplyCommitMessage::None => return,
            ApplyCommitMessage::Membership((commit, tx)) => {
                let res = self.commit_membership_change(commit).await;
                self.pending_responses
                    .push_back(ResponseCallbackQueue::new_callback(tx, res))
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
                    NodeWorker::<TR, RS, MRS, RES>::membership_add(
                        self.node_id,
                        group,
                        change_request,
                        &mut self.node_manager,
                        &mut self.replica_cache,
                    )
                    .await;
                }
                ConfChangeType::RemoveNode => {
                    NodeWorker::<TR, RS, MRS, RES>::membership_remove(
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
        assert_eq!(change.change_type(), ConfChangeType::AddNode);
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
        let drainned = self.active_groups.drain();
        let mut multi_groups_write = HashMap::new();
        let mut applys = HashMap::new();
        for group_id in drainned {
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
                            // &mut self.pending_events,
                            &mut self.event_chan,
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

    fn send_applys(&self, applys: HashMap<u64, ApplyData<RES>>) {
        let span = tracing::span::Span::current();
        if let Err(_err) = self
            .apply_tx
            .send((span.clone(), ApplyMessage::Apply { applys }))
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
    fn do_stop(self) {
        info!("node {}: node actor stopped now", self.node_id);
    }
}

#[cfg(test)]
mod tests {

    use super::NodeWorker;
    use crate::memstore::MultiRaftMemoryStorage;
    use crate::memstore::RaftMemStorage;
    use crate::multiraft::proposal::ProposalQueue;
    use crate::multiraft::proposal::ReadIndexQueue;

    use crate::multiraft::group::RaftGroup;
    use crate::multiraft::group::RaftGroupState;
    use crate::multiraft::group::Status;

    use crate::multiraft::replica_cache::ReplicaCache;
    use crate::multiraft::transport::LocalTransport;
    use crate::multiraft::Error;
    use crate::multiraft::MultiRaftMessageSenderImpl;
    use raft_proto::prelude::ReplicaDesc;
    use raft_proto::prelude::SingleMembershipChange;

    use super::NodeManager;

    type TestMultiRaftActorRuntime = NodeWorker<
        LocalTransport<MultiRaftMessageSenderImpl>,
        RaftMemStorage,
        MultiRaftMemoryStorage,
        (),
    >;
    fn new_raft_group(
        node_id: u64,
        group_id: u64,
        replica_id: u64,
        store: &RaftMemStorage,
    ) -> Result<RaftGroup<RaftMemStorage, ()>, Error> {
        let raft_cfg = raft::Config {
            id: replica_id,
            ..Default::default()
        };

        let raft_group = raft::RawNode::with_default_logger(&raft_cfg, store.clone())
            .map_err(|err| Error::Raft(err))?;

        Ok(RaftGroup {
            node_id,
            group_id,
            replica_id,
            raft_group,
            node_ids: vec![node_id],
            proposals: ProposalQueue::new(replica_id),
            leader: ReplicaDesc::default(), // TODO: init leader from storage
            committed_term: 0,              // TODO: init committed term from storage
            state: RaftGroupState::default(),
            status: Status::None,
            read_index_queue: ReadIndexQueue::new(),
        })
    }

    #[tokio::test]
    async fn test_membership_add_remove() {
        // let cfg = Config {
        //     node_id: 1,
        //     batch_append: false,
        //     election_tick: 2,
        //     heartbeat_tick: 1,
        //     max_size_per_msg: 0,
        //     max_inflight_msgs: 256,
        //     tick_interval: 3_600_000, // hour ms
        //     batch_apply: false,
        //     batch_size: 0,
        //     write_proposal_queue_size: 1000,
        // };

        // let transport = LocalTransport::new();
        // let (event_tx, _) = channel(1);
        // let mut worker = TestMultiRaftActorRuntime::new(&cfg, &transport, &storage, &event_tx);
        let raft_store = RaftMemStorage::new();
        let mut node_manager = NodeManager::new();
        let storage = MultiRaftMemoryStorage::new(1);
        let mut replica_cache = ReplicaCache::new(storage);
        let mut raft_group = new_raft_group(1, 1, 1, &raft_store).unwrap();
        let group_id = 1;

        // add five changes
        let mut changes = vec![];
        let first_node_id = 2;
        for i in 0..4 {
            let node_id = first_node_id + i;
            let replica_id = first_node_id + i;
            let mut change = SingleMembershipChange::default();
            change.node_id = node_id;
            change.replica_id = replica_id;
            change.set_change_type(raft::prelude::ConfChangeType::AddNode);
            TestMultiRaftActorRuntime::membership_add(
                1,
                &mut raft_group,
                &change,
                &mut node_manager,
                &mut replica_cache,
            )
            .await;
            changes.push(change);
        }

        for change in changes.iter() {
            let node = node_manager.get_node(&change.node_id).unwrap();
            assert_eq!(node.group_map.contains_key(&group_id), true);

            let rep = replica_cache
                .replica_desc(group_id, change.replica_id)
                .await
                .unwrap()
                .unwrap();

            assert_eq!(
                rep,
                ReplicaDesc {
                    node_id: change.node_id,
                    replica_id: change.replica_id,
                }
            );
        }

        assert_eq!(raft_group.node_ids, vec![1, 2, 3, 4, 5]);

        let mut changes = vec![];
        let first_node_id = 2;
        for i in 0..4 {
            let node_id = first_node_id + i;
            let replica_id = first_node_id + i;
            let mut change = SingleMembershipChange::default();
            change.node_id = node_id;
            change.replica_id = replica_id;
            change.set_change_type(raft::prelude::ConfChangeType::RemoveNode);
            TestMultiRaftActorRuntime::membership_remove(
                1,
                &mut raft_group,
                &change,
                &mut node_manager,
                &mut replica_cache,
            )
            .await;
            changes.push(change);
        }

        for change in changes.iter() {
            // TODO: if node group is empty, should remove node item from map
            let node = node_manager.get_node(&change.node_id).unwrap();
            assert_eq!(node.group_map.contains_key(&group_id), false);

            assert_eq!(
                replica_cache
                    .replica_desc(group_id, change.replica_id)
                    .await
                    .unwrap()
                    .is_none(),
                true
            );
        }

        assert_eq!(raft_group.node_ids, vec![1]);
    }
}
