use std::cmp;
use std::collections::hash_map::HashMap;
use std::collections::hash_map::Iter;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use raft::prelude::ConfState;
use raft::StateRole;
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

use crate::multiraft::ProposeResponse;
use crate::multiraft::NO_LEADER;
use crate::prelude::ConfChangeType;
use crate::prelude::GroupMetadata;
use crate::prelude::Message;
use crate::prelude::MessageType;
use crate::prelude::MultiRaftMessage;
use crate::prelude::MultiRaftMessageResponse;
use crate::prelude::ReplicaDesc;

use super::apply::ApplyActor;
use super::config::Config;
use super::error::ChannelError;
use super::error::Error;
use super::error::RaftGroupError;
use super::event::Event;
use super::event::EventChannel;
use super::group::RaftGroup;
use super::group::RaftGroupWriteRequest;
use super::group::Status;
use super::msg::ApplyCommitMessage;
use super::msg::ApplyData;
use super::msg::ApplyMessage;
use super::msg::ApplyResultMessage;
use super::msg::CommitMembership;
use super::msg::ManageMessage;
use super::msg::ProposeMessage;
use super::msg::QueryGroup;
use super::multiraft::NO_GORUP;
use super::multiraft::NO_NODE;
use super::proposal::ProposalQueue;
use super::proposal::ReadIndexQueue;
use super::replica_cache::ReplicaCache;
use super::rsm::StateMachine;
use super::state::GroupState;
use super::state::GroupStates;
use super::storage::MultiRaftStorage;
use super::storage::RaftStorage;
use super::tick::Ticker;
use super::transport::Transport;
use super::ProposeData;
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
            None => self.nodes.entry(node_id).or_insert(Node {
                node_id,
                group_map: HashMap::new(),
            }),
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

pub struct NodeActor<W, R>
where
    W: ProposeData,
    R: ProposeResponse,
{
    // TODO: queue should have one per-group.
    pub propose_tx: Sender<ProposeMessage<W, R>>,
    pub campaign_tx: Sender<(u64, oneshot::Sender<Result<(), Error>>)>,
    pub raft_message_tx: Sender<(
        MultiRaftMessage,
        oneshot::Sender<Result<MultiRaftMessageResponse, Error>>,
    )>,
    pub manage_tx: Sender<ManageMessage>,
    pub query_group_tx: UnboundedSender<QueryGroup>,
    #[allow(unused)]
    apply: ApplyActor,
}

impl<W, R> NodeActor<W, R>
where
    W: ProposeData,
    R: ProposeResponse,
{
    pub fn spawn<TR, RS, MRS, RSM>(
        cfg: &Config,
        transport: &TR,
        storage: &MRS,
        rsm: RSM,
        event_bcast: &EventChannel,
        ticker: Option<Box<dyn Ticker>>,
        states: GroupStates,
        stopped: Arc<AtomicBool>,
    ) -> Self
    where
        TR: Transport + Clone,
        RS: RaftStorage,
        MRS: MultiRaftStorage<RS>,
        RSM: StateMachine<W, R>,
    {
        let (propose_tx, propose_rx) = channel(cfg.proposal_queue_size);
        let (manage_tx, manage_rx) = channel(1);
        let (campaign_tx, campaign_rx) = channel(1);
        let (raft_message_tx, raft_message_rx) = channel(10);

        let (commit_tx, commit_rx) = unbounded_channel();

        let (apply_request_tx, apply_request_rx) = unbounded_channel();
        let (apply_response_tx, apply_response_rx) = unbounded_channel();
        let (group_query_tx, group_query_rx) = unbounded_channel();
        let apply = ApplyActor::spawn(
            cfg,
            rsm,
            storage.clone(),
            states.clone(),
            apply_request_rx,
            apply_response_tx,
            commit_tx,
            stopped.clone(),
        );

        let mut worker = NodeWorker::<TR, RS, MRS, W, R>::new(
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
            group_query_rx,
            states,
        );

        tokio::spawn(async move {
            worker.restore().await;
            worker.main_loop(ticker, stopped).await;
        });

        Self {
            query_group_tx: group_query_tx,
            raft_message_tx,
            propose_tx,
            campaign_tx,
            manage_tx,
            apply,
        }
    }
}

pub struct NodeWorker<TR, RS, MRS, W, R>
where
    TR: Transport,
    RS: RaftStorage,
    MRS: MultiRaftStorage<RS>,
    W: ProposeData,
    R: ProposeResponse,
{
    pub(crate) cfg: Config,
    pub(crate) node_id: u64,
    pub(crate) storage: MRS,
    pub(crate) transport: TR,
    pub(crate) node_manager: NodeManager,
    pub(crate) replica_cache: ReplicaCache<RS, MRS>,
    pub(crate) groups: HashMap<u64, RaftGroup<RS, R>>,
    pub(crate) active_groups: HashSet<u64>,
    pub(crate) pending_responses: ResponseCallbackQueue,
    pub(crate) event_chan: EventChannel,
    pub(crate) multiraft_message_rx: Receiver<(
        MultiRaftMessage,
        oneshot::Sender<Result<MultiRaftMessageResponse, Error>>,
    )>,
    pub(crate) propose_rx: Receiver<ProposeMessage<W, R>>,
    pub(crate) manage_rx: Receiver<ManageMessage>,
    pub(crate) campaign_rx: Receiver<(u64, oneshot::Sender<Result<(), Error>>)>,
    pub(crate) commit_rx: UnboundedReceiver<ApplyCommitMessage>,
    pub(crate) apply_tx: UnboundedSender<(Span, ApplyMessage<R>)>,
    pub(crate) apply_result_rx: UnboundedReceiver<ApplyResultMessage>,
    pub(crate) query_group_rx: UnboundedReceiver<QueryGroup>,
    pub(crate) shared_states: GroupStates,
}

impl<TR, RS, MRS, WD, RES> NodeWorker<TR, RS, MRS, WD, RES>
where
    TR: Transport + Clone,
    RS: RaftStorage,
    MRS: MultiRaftStorage<RS>,
    WD: ProposeData,
    RES: ProposeResponse,
{
    fn new(
        cfg: &Config,
        transport: &TR,
        storage: &MRS,
        propose_rx: Receiver<ProposeMessage<WD, RES>>,
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
        group_query_rx: UnboundedReceiver<QueryGroup>,
        shared_states: GroupStates,
    ) -> Self {
        NodeWorker::<TR, RS, MRS, WD, RES> {
            cfg: cfg.clone(),
            node_id: cfg.node_id,
            node_manager: NodeManager::new(),
            groups: HashMap::new(),
            propose_rx,
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
            shared_states,
            query_group_rx: group_query_rx,
        }
    }

    /// Restore the node from storage.
    /// TODO: add unit test
    async fn restore(&mut self) {
        // TODO: load all replica desc to recreate node manager.
        // TODO: use group_iter
        let gs_metas = self.storage.scan_group_metadata().await.unwrap();

        for gs_meta in gs_metas.iter() {
            // TODO: check group metadta status to detect whether deleted.
            if gs_meta.deleted || gs_meta.node_id != self.node_id {
                continue;
            }

            // TODO: cache optimize
            let gs = self
                .storage
                .group_storage(gs_meta.group_id, gs_meta.replica_id)
                .await
                .unwrap();
            let rs = gs.initial_state().unwrap();
            if !rs.initialized() {
                continue;
            }

            self.node_manager
                .add_group(gs_meta.node_id, gs_meta.group_id);

            let replica_descs: Vec<ReplicaDesc> = self
                .storage
                .scan_group_replica_desc(gs_meta.group_id)
                .await
                .unwrap();
            // if empty voters and conf state uninitialized, don't restore
            self.create_raft_group(
                gs_meta.group_id,
                gs_meta.replica_id,
                replica_descs,
                None,
                None,
            )
            .await
            .unwrap();
            // TODO: move track group node here.
        }
    }

    #[tracing::instrument(
        name = "NodeActor::main_loop"
        level = Level::TRACE,
        skip_all,
        fields(node_id=self.node_id)
    )]
    async fn main_loop(mut self, ticker: Option<Box<dyn Ticker>>, stopped: Arc<AtomicBool>) {
        info!("node {}: start multiraft main_loop", self.node_id);

        // create default ticker if ticker is None.
        let tick_interval = Duration::from_millis(self.cfg.tick_interval);
        let mut ticker = ticker.map_or(
            Box::new(tokio::time::interval_at(
                tokio::time::Instant::now() + tick_interval,
                tick_interval,
            )) as Box<dyn Ticker>,
            |t| t,
        );

        let mut ticks = 0;
        loop {
            if stopped.load(std::sync::atomic::Ordering::SeqCst) {
                self.do_stop();
                break;
            }

            self.event_chan.flush();
            tokio::select! {
                // Note: see https://github.com/tokio-rs/tokio/discussions/4019 for more
                // information about why mut here.

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
                        self.merge_heartbeats();
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

                Some(msg) = self.query_group_rx.recv() => self.handle_query_group(msg),

                else => {},
            }

            if !self.active_groups.is_empty() {
                self.handle_readys().await;
                /* here is active groups already drained */
            }

            self.pending_responses.flush();
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
        if !self.groups.contains_key(&msg.group_id) {
            let msg = msg.clone();
            let raft_msg = msg.msg.as_ref().expect("why message missing raft msg");
            // TODO: if group mark deleted, we need return error
            let _ = self
                .create_raft_group(
                    msg.group_id,
                    raft_msg.to,
                    msg.replicas.clone(),
                    None,
                    Some(msg.clone()),
                )
                .await
                .map_err(|err| {
                    error!(
                        "node {}: create group for replica {} error {}",
                        self.node_id, raft_msg.to, err
                    );
                    err
                })?;
        }

        let raft_msg = msg
            .msg
            .take()
            .expect("invalid message, raft Message should not be none.");

        let group_id = msg.group_id;
        let from_replica = ReplicaDesc {
            group_id,
            node_id: msg.from_node,
            replica_id: raft_msg.from,
        };
        let to_replica = ReplicaDesc {
            group_id,
            node_id: msg.to_node,
            replica_id: raft_msg.to,
        };

        // processing messages between replicas from other nodes to self node.
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
        // if self.groups.contains_key(&group_id)
        //     && !self.node_manager.contains_node(&to_replica.node_id)
        // {
        //     self.node_manager.add_group(to_replica.node_id, group_id);
        // }

        let group = self
            .groups
            .get_mut(&group_id)
            .expect("unreachable: group always initialize or return error in the previouse code");

        if let Err(err) = group.raft_group.step(raft_msg) {
            warn!("node {}: step raf message error: {}", self.node_id, err);
        }
        self.active_groups.insert(group_id);
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
    fn handle_propose(&mut self, msg: ProposeMessage<WD, RES>) -> Option<ResponseCallback> {
        match msg {
            ProposeMessage::Write(data) => {
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
            ProposeMessage::Membership(request) => {
                let group_id = request.group_id;
                match self.groups.get_mut(&group_id) {
                    None => {
                        warn!(
                            "node {}: proposal membership failed, group {} does not exists",
                            self.node_id, group_id,
                        );
                        return Some(ResponseCallbackQueue::new_error_callback(
                            request.tx,
                            Error::RaftGroup(RaftGroupError::Deleted(self.node_id, group_id)),
                        ));
                    }
                    Some(group) => {
                        self.active_groups.insert(group_id);
                        group.propose_membership_change(request)
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
            // ManageMessage::GroupData(data) => self.handle_group_manage(data).await,
            ManageMessage::CreateGroup(request, tx) => {
                self.active_groups.insert(request.group_id);
                let res = self
                    .create_raft_group(
                        request.group_id,
                        request.replica_id,
                        request.replicas,
                        Some(request.applied_hint),
                        None,
                    )
                    .await;
                return Some(ResponseCallbackQueue::new_callback(tx, res));
            }
            ManageMessage::RemoveGroup(request, tx) => {
                // marke delete
                let group_id = request.group_id;
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

                let replica_id = group.replica_id;
                match self
                    .storage
                    .get_group_metadata(group_id, replica_id)
                    .await
                    .unwrap()
                {
                    None => {
                        self.storage
                            .set_group_metadata(GroupMetadata {
                                group_id,
                                replica_id,
                                node_id: self.node_id,
                                create_timestamp: 0,
                                leader_id: group.leader.replica_id,
                                deleted: true,
                            })
                            .await
                            .unwrap();
                    }
                    Some(mut meta) => {
                        if !meta.deleted {
                            meta.deleted = true;
                            self.storage.set_group_metadata(meta).await.unwrap();
                        }
                    }
                }

                // TODO: impl broadcast
                return Some(ResponseCallbackQueue::new_callback(tx, Ok(())));
            }
        }
    }

    // #[tracing::instrument(
    //     name = "MultiRaftActorRuntime::raft_group_management",
    //     level = Level::TRACE,
    //     skip_all
    // )]
    // async fn handle_group_manage(&mut self, data: GroupData) -> Option<ResponseCallback> {
    //     let tx = data.tx;
    //     let res = match data.op {
    //         GroupOp::Create => {
    //             self.active_groups.insert(data.group_id);
    //             self.create_raft_group(
    //                 data.group_id,
    //                 data.replica_id,
    //                 data.replicas.map_or(vec![], |rs| rs),
    //                 0,
    //             )
    //             .await
    //         }
    //         GroupOp::Remove => {
    //             // marke delete
    //             let group_id = data.group_id;
    //             let group = match self.groups.get_mut(&group_id) {
    //                 None => return Some(ResponseCallbackQueue::new_callback(tx, Ok(()))),
    //                 Some(group) => group,
    //             };

    //             for proposal in group.proposals.drain(..) {
    //                 proposal.tx.map(|tx| {
    //                     tx.send(Err(Error::RaftGroup(RaftGroupError::Deleted(
    //                         self.node_id,
    //                         group_id,
    //                     ))))
    //                 });
    //             }

    //             group.status = Status::Delete;

    //             // TODO: impl broadcast

    //             Ok(())
    //         }
    //     };

    //     return Some(ResponseCallbackQueue::new_callback(tx, res));
    // }

    // #[tracing::instrument(
    //     name = "MultiRaftActorRuntime::create_raft_group",
    //     level = Level::TRACE,
    //     skip(self))
    // ]

    /// # Parameters
    /// - `msg`: If msg is Some, the raft group is initialized with a message
    /// from the leader. If `msg` is the leader msg (such as MsgAppend etc.),
    /// the internal state of the raft replica is initialized for `leader`,
    /// `Node manager`, etc. which allows the replica to `fanout_heartbeat`
    /// messages from the leader node.Without this initialization, the new
    /// raft replica may fail to receive the leader's heartbeat and initiate
    /// a new election distrubed.
    async fn create_raft_group(
        &mut self,
        group_id: u64,
        replica_id: u64,
        replicas_desc: Vec<ReplicaDesc>,
        applied_hint: Option<u64>,
        init_msg: Option<MultiRaftMessage>,
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
        let rs: raft::RaftState = group_storage
            .initial_state()
            .map_err(|err| Error::Raft(err))?;

        // select a suitable applied index from both storage and initial provided.
        let applied = cmp::max(
            group_storage.get_applied().unwrap_or(0),
            applied_hint.unwrap_or(0),
        );
        let committed_index = rs.hard_state.commit;
        let persisted_index = group_storage.last_index().unwrap();
        if applied > cmp::min(committed_index, persisted_index) {
            panic!(
                "provide hit applied is out of range [applied({}), min (committed({}), persisted({}))]",
                applied, committed_index, persisted_index
            );
        }

        let raft_cfg = raft::Config {
            id: replica_id,
            applied, // TODO: support hint skip
            election_tick: self.cfg.election_tick,
            heartbeat_tick: self.cfg.heartbeat_tick,
            max_size_per_msg: self.cfg.max_size_per_msg,
            max_inflight_msgs: self.cfg.max_inflight_msgs,
            batch_append: self.cfg.batch_append,
            pre_vote: true,
            ..Default::default()
        };
        let raft_store = group_storage.clone();
        let raft_group = raft::RawNode::with_default_logger(&raft_cfg, raft_store)
            .map_err(|err| Error::Raft(err))?;

        info!(
            "node {}: replica({}) of raft group({}) is created",
            self.node_id, group_id, replica_id
        );

        let mut leader: ReplicaDesc = ReplicaDesc::default();

        if let Some(init_msg) = init_msg {
            let mut gs_meta = self
                .storage
                .get_group_metadata(group_id, replica_id)
                .await?
                .expect("why missing group_storage metadata");

            let raft_msg = init_msg.msg.as_ref().unwrap();
            //  Persisted leader info of the current replica to prevent
            //  rejecting the leader heartbeat if it does not have the
            //  leader information after the replica restarts.
            if gs_meta.leader_id != raft_msg.from {
                gs_meta.leader_id = raft_msg.from;
                self.storage.set_group_metadata(gs_meta.clone()).await?;
                info!(
                    "node {}: persisted leader_id({}) to storage for replica({}) of raft group({}) from init msg",
                    self.node_id, raft_msg.from, replica_id, group_id
                );
            }

            // Save the leader and from_node information so that the replica can receive
            // the leader heartbeat after being created
            leader.replica_id = raft_msg.from;
            leader.node_id = init_msg.from_node;
            leader.group_id = init_msg.group_id;
            self.node_manager
                .add_group(init_msg.from_node, init_msg.group_id);
            info!(
                "node {}: initial leader({:?}) for replica({}) of raft group({}) from init msg",
                self.node_id, leader, replica_id, group_id
            );
        }

        //  initialize shared_state of group
        let shared_state = Arc::new(GroupState::from((
            replica_id,
            rs.hard_state.commit, /* commit_index */
            rs.hard_state.term,   /* commit_term */
            NO_LEADER,
            StateRole::Follower,
        )));
        let mut group = RaftGroup {
            node_id: self.cfg.node_id,
            group_id,
            replica_id,
            raft_group,
            node_ids: Vec::new(),
            proposals: ProposalQueue::new(replica_id),
            leader,
            status: Status::None,
            read_index_queue: ReadIndexQueue::new(),
            shared_state: shared_state.clone(),
            // applied_index: 0,
            // applied_term: 0,
            commit_index: rs.hard_state.commit,
            commit_term: rs.hard_state.term,
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

        let prev_shard_state = self.shared_states.insert(group_id, shared_state);

        assert_eq!(
            prev_shard_state.is_none(),
            true,
            "expect group {} shared state is empty, but goted",
            group_id
        );

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
    async fn handle_apply_result(&mut self, result: ApplyResultMessage) {
        let group = match self.groups.get_mut(&result.group_id) {
            Some(group) => group,
            None => {
                warn!("group {} removed, skip apply", result.group_id);
                return;
            }
        };

        group.advance_apply(&result);
        debug!(
            "node {}: group = {} apply state change = {:?}",
            self.node_id, result.group_id, result
        );
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

    fn handle_query_group(&self, msg: QueryGroup) {
        match msg {
            QueryGroup::HasPendingConf(group_id, tx) => match self.get_group(group_id) {
                Err(err) => {
                    // TODO: move response callback queue
                    // TODO: We should consider adding a priority to the response callback queue,
                    // to which the response should have a higher priority
                    if let Err(_) = tx.send(Err(err)) {
                        error!("send query HasPendingConf result error, receiver dropped");
                    }
                }
                Ok(group) => {
                    // TODO: move response callback queue
                    // TODO: We should consider adding a priority to the response callback queue,
                    // to which the response should have a higher priority
                    let res = group.raft_group.raft.has_pending_conf();
                    if let Err(_) = tx.send(Ok(res)) {
                        error!("send query HasPendingConf result error, receiver dropped");
                    }
                }
            },
        }
    }

    #[inline]
    fn get_group(&self, group_id: u64) -> Result<&RaftGroup<RS, RES>, Error> {
        self.groups.get(&group_id).map_or(
            Err(Error::RaftGroup(RaftGroupError::Deleted(
                self.node_id,
                group_id,
            ))),
            |group| Ok(group),
        )
    }

    async fn commit_membership_change(
        &mut self,
        mut view: CommitMembership,
    ) -> Result<ConfState, Error> {
        if view.change_request.is_none() && view.conf_change.leave_joint() {
            tracing::info!("now leave ccv2");
            return self.apply_conf_change(view).await;
        }

        let changes = view.change_request.take().unwrap().changes;
        assert_eq!(changes.len(), view.conf_change.changes.len());

        let group_id = view.group_id;
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
        for (conf_change, change_request) in view.conf_change.changes.iter().zip(changes.iter()) {
            match conf_change.change_type() {
                ConfChangeType::AddNode => {
                    Self::add_replica(
                        self.node_id,
                        group,
                        &mut self.node_manager,
                        &mut self.replica_cache,
                        change_request.node_id,
                        change_request.replica_id,
                    )
                    .await
                }

                ConfChangeType::RemoveNode => {
                    Self::remove_replica(
                        self.node_id,
                        group,
                        &mut self.node_manager,
                        &mut self.replica_cache,
                        change_request.node_id,
                        change_request.replica_id,
                    )
                    .await
                }
                ConfChangeType::AddLearnerNode => unimplemented!(),
            }
        }

        // The leader communicates with the new member after the membership change,
        // sends the snapshot contains the member configuration, and then follower
        // install snapshot.
        // The Append Entries are then received by followers and committed, but the
        // new member configuration is already applied to the followers when the
        // snapshot is installed. In raft-rs, duplicate joint consensus is not allowed,
        // so we catch the error and skip.
        if !view.conf_change.leave_joint() && view.conf_change.enter_joint().is_some() {
            debug!(
                "node {}: for group {} replica {} skip alreay entered joint conf_change {:?}",
                self.node_id, group_id, group.replica_id, view.conf_change
            );
            let conf_state = group.raft_group.raft.prs().conf().to_conf_state();
            if !conf_state.voters_outgoing.is_empty() {
                return Ok(conf_state);
            }
        }

        return self.apply_conf_change(view).await;
        // apply to raft
        // let conf_state = match group.raft_group.apply_conf_change(&view.conf_change) {
        //     Err(err) => {
        //         error!(
        //             "node {}: commit membership change error: group = {}, err = {}",
        //             self.node_id, group_id, err,
        //         );
        //         return Err(Error::Raft(err));
        //     }
        //     Ok(conf_state) => conf_state,
        // };

        // let gs = &self
        //     .storage
        //     .group_storage(group_id, group.replica_id)
        //     .await?;
        // gs.set_confstate(conf_state.clone())?;
        // debug!(
        //     "node {}: applied conf_state {:?} for group {} replica{}",
        //     self.node_id, conf_state, group_id, group.replica_id
        // );
        // return Ok(conf_state);
    }

    async fn apply_conf_change(
        &mut self,
        // group_id: u64,
        // group: &mut RaftGroup<RS, RES>,
        view: CommitMembership,
    ) -> Result<ConfState, Error> {
        let group_id = view.group_id;
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
        gs.set_confstate(conf_state.clone())?;
        debug!(
            "node {}: applied conf_state {:?} for group {} replica{}",
            self.node_id, conf_state, group_id, group.replica_id
        );
        return Ok(conf_state);
    }

    async fn add_replica(
        node_id: u64,
        group: &mut RaftGroup<RS, RES>,
        node_manager: &mut NodeManager,
        replica_cache: &mut ReplicaCache<RS, MRS>,
        change_node_id: u64,
        change_replica_id: u64,
    ) {
        let group_id = group.group_id;
        node_manager.add_group(change_node_id, group_id);

        // TODO: this call need transfer to user call, and if user call return errored,
        // the membership change should failed and user need to retry.
        // we need a channel to provider user notify actor it need call these code.
        // and we recv the notify can executing these code, if executed failed, we
        // response to user and membership change is failed.
        if let Err(err) = replica_cache
            .cache_replica_desc(
                group_id,
                ReplicaDesc {
                    group_id,
                    node_id: change_node_id,
                    replica_id: change_replica_id,
                },
                true,
            )
            .await
        {
            warn!(
                "node {}: the membership change request to add replica desc error: {} ",
                node_id, err
            );
        }

        group.add_track_node(change_node_id);
    }

    async fn remove_replica(
        node_id: u64,
        group: &mut RaftGroup<RS, RES>,
        node_manager: &mut NodeManager,
        replica_cache: &mut ReplicaCache<RS, MRS>,
        changed_node_id: u64,
        changed_replica_id: u64,
    ) {
        let group_id = group.group_id;
        let _ = group.remove_pending_proposals();
        group.remove_track_node(changed_node_id);
        // TODO: think remove if node has empty group_map.
        let _ = node_manager.remove_group(changed_node_id, group_id);

        if let Err(err) = replica_cache
            .remove_replica_desc(
                group_id,
                ReplicaDesc {
                    group_id,
                    node_id: changed_node_id,
                    replica_id: changed_replica_id,
                },
                true,
            )
            .await
        {
            warn!(
                "node {}: the membership change request to add replica desc error: {} ",
                node_id, err
            );
        }
    }

    async fn handle_readys(&mut self) {
        let mut writes = HashMap::new();
        let mut applys = HashMap::new();
        let ready_groups = self.active_groups.drain().collect::<Vec<u64>>();
        for group_id in ready_groups {
            if group_id == NO_GORUP {
                continue;
            }
            let group = match self.groups.get_mut(&group_id) {
                None => {
                    // TODO: remove pending proposals related to this group
                    error!(
                        "node {}: handle group-{} ready, but dropped",
                        self.node_id, group_id
                    );
                    continue;
                }
                Some(group) => group,
            };
            if !group.raft_group.has_ready() {
                continue;
            }

            let res = group
                .handle_ready(
                    self.node_id,
                    &self.transport,
                    &self.storage,
                    &mut self.replica_cache,
                    &mut self.node_manager,
                    &mut self.event_chan,
                )
                .await;

            let err = match res {
                Ok((gwr, apply)) => {
                    writes.insert(group_id, gwr);
                    apply.map(|apply| applys.insert(group_id, apply));
                    continue;
                }
                Err(err) => err,
            };

            match err {
                Error::Storage(storage_err) => match storage_err {
                    super::storage::Error::StorageTemporarilyUnavailable => {
                        warn!(
                            "node {}: group {} storage temporarily unavailable",
                            self.node_id, group_id
                        );
                        self.active_groups.insert(group_id);
                        continue;
                    }
                    _ => {
                        panic!("node {}: storage unavailable", self.node_id)
                    }
                },
                _ => {
                    self.active_groups.insert(group_id);
                    continue;
                }
            }
        }

        if !applys.is_empty() {
            self.send_applys(applys);
        }

        self.handle_writes(writes).await;
    }

    async fn handle_writes(&mut self, mut writes: HashMap<u64, RaftGroupWriteRequest>) {
        let mut applys = HashMap::new();

        // TODO(yuanchang.xu) Disk write flow control
        for (group_id, gwr) in writes.iter_mut() {
            // TODO: cache storage in related raft group.
            let gs = match self.storage.group_storage(*group_id, gwr.replica_id).await {
                Ok(gs) => gs,
                Err(err) => {
                    match err {
                        super::storage::Error::StorageTemporarilyUnavailable => {
                            warn!("node {}: group {} handle_write but storage temporarily unavailable ", self.node_id, group_id);

                            self.active_groups.insert(*group_id);
                            continue;
                        }
                        super::storage::Error::StorageUnavailable => {
                            panic!("node {}: storage unavailable", self.node_id)
                        }
                        _ => {
                            warn!(
                                "node {}: get raft storage for group {} to handle_writes error: {}",
                                self.node_id, *group_id, err
                            );
                            continue;
                        }
                    }
                }
            };

            let group = match self.groups.get_mut(&group_id) {
                Some(group) => group,
                None => {
                    // TODO: remove pending proposals related to this group
                    // If the group does not exist at this point
                    // 1. we may have finished sending messages to the group, role changed notifications,
                    //    committable entires commits
                    // 2. we may not have completed the new proposal append, there may be multiple scenarios
                    //     - The current group is the leader, sent AE, but was deleted before it received a
                    //       response from the follower, so it did not complete the append drop
                    //     - The current group is the follower, which does not affect the completion of the
                    //       AE
                    error!(
                        "node {}: handle group-{} write ready, but dropped",
                        self.node_id, group_id
                    );
                    continue;
                }
            };

            let res = group
                .handle_write(
                    self.node_id,
                    gwr,
                    &gs,
                    &self.transport,
                    &mut self.replica_cache,
                    &mut self.node_manager,
                )
                .await;

            let write_err = match res {
                Ok(apply) => {
                    apply.map(|apply| applys.insert(*group_id, apply));
                    continue;
                }

                Err(err) => err,
            };

            match write_err {
                // if it is, temporary storage unavailability causes write log entries and
                // status failure, this is a recoverable failure, we will consider retrying
                // later.
                super::storage::Error::LogTemporarilyUnavailable
                | super::storage::Error::SnapshotTemporarilyUnavailable
                | super::storage::Error::StorageTemporarilyUnavailable => {
                    self.active_groups.insert(*group_id);
                    continue;
                }

                super::storage::Error::LogUnavailable
                | super::storage::Error::SnapshotUnavailable => {
                    panic!(
                        "node {}: group {} storage unavailable",
                        self.node_id, *group_id
                    );

                    // TODO: consider response and panic here.
                }
                _ => {
                    warn!(
                        "node {}: group {} raft storage to handle_write got error: {}",
                        self.node_id, *group_id, write_err
                    );
                    continue;
                }
            }
        }

        if !applys.is_empty() {
            self.send_applys(applys);
        }
    }

    fn send_applys(&self, applys: HashMap<u64, ApplyData<RES>>) {
        let span = tracing::span::Span::current();
        if let Err(_err) = self
            .apply_tx
            .send((span.clone(), ApplyMessage::Apply { applys }))
        {
            // FIXME: this should unreachable, because the lifetime of apply actor is bound to us.
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
    use std::sync::Arc;

    use super::NodeWorker;
    use crate::proposal::ProposalQueue;
    use crate::proposal::ReadIndexQueue;
    use crate::storage::MemStorage;
    use crate::storage::MultiRaftMemoryStorage;

    use crate::group::RaftGroup;
    use crate::group::Status;

    use crate::prelude::ReplicaDesc;
    use crate::replica_cache::ReplicaCache;
    use crate::transport::LocalTransport;
    use crate::Error;
    use crate::MultiRaftMessageSenderImpl;

    use super::NodeManager;
    use crate::state::GroupState;
    type TestMultiRaftActorRuntime = NodeWorker<
        LocalTransport<MultiRaftMessageSenderImpl>,
        MemStorage,
        MultiRaftMemoryStorage,
        (),
        (),
    >;
    fn new_raft_group(
        node_id: u64,
        group_id: u64,
        replica_id: u64,
        store: &MemStorage,
    ) -> Result<RaftGroup<MemStorage, ()>, Error> {
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
            status: Status::None,
            shared_state: Arc::new(GroupState::default()),
            read_index_queue: ReadIndexQueue::new(),

            commit_term: 0, // TODO: init committed term from storage
            commit_index: 0,
            // applied_index: 0,
            // applied_term: 0,
        })
    }

    #[tokio::test]
    async fn test_membership_add_remove() {
        let raft_store = MemStorage::new();
        let mut node_manager = NodeManager::new();
        let storage = MultiRaftMemoryStorage::new(1);
        let mut replica_cache = ReplicaCache::new(storage);
        let mut raft_group = new_raft_group(1, 1, 1, &raft_store).unwrap();
        let group_id = 1;

        // add five changes
        for i in 2..=5 {
            let node_id = i;
            let replica_id = i;
            TestMultiRaftActorRuntime::add_replica(
                1,
                &mut raft_group,
                &mut node_manager,
                &mut replica_cache,
                node_id,
                replica_id,
            )
            .await;
        }

        for i in 2..=5 {
            let node_id = i;
            let replica_id = i;
            let node = node_manager.get_node(&node_id).unwrap();
            assert_eq!(node.group_map.contains_key(&group_id), true);

            let rep = replica_cache
                .replica_desc(group_id, replica_id)
                .await
                .unwrap()
                .unwrap();

            assert_eq!(
                rep,
                ReplicaDesc {
                    group_id,
                    node_id,
                    replica_id,
                }
            );
        }
        assert_eq!(raft_group.node_ids, vec![1, 2, 3, 4, 5]);

        // remove nodes
        for i in 2..=5 {
            let node_id = i;
            let replica_id = i;
            TestMultiRaftActorRuntime::remove_replica(
                1,
                &mut raft_group,
                &mut node_manager,
                &mut replica_cache,
                node_id,
                replica_id,
            )
            .await;
        }

        for i in 2..=5 {
            let node_id = i;
            let replica_id = i;
            // TODO: if node group is empty, should remove node item from map
            let node = node_manager.get_node(&node_id).unwrap();
            assert_eq!(node.group_map.contains_key(&group_id), false);

            assert_eq!(
                replica_cache
                    .replica_desc(group_id, replica_id)
                    .await
                    .unwrap()
                    .is_none(),
                true
            );
        }

        assert_eq!(raft_group.node_ids, vec![1]);
    }

    #[tokio::test]
    async fn test_replica_add_remove_idempotent() {
        let raft_store = MemStorage::new();
        let mut node_manager = NodeManager::new();
        let storage = MultiRaftMemoryStorage::new(1);
        let mut replica_cache = ReplicaCache::new(storage);
        let mut raft_group = new_raft_group(1, 1, 1, &raft_store).unwrap();
        let group_id = 1;

        // add five replicas for many times
        for _ in 0..5 {
            for i in 2..=5 {
                let node_id = i;
                let replica_id = i;
                TestMultiRaftActorRuntime::add_replica(
                    1,
                    &mut raft_group,
                    &mut node_manager,
                    &mut replica_cache,
                    node_id,
                    replica_id,
                )
                .await;
            }
        }

        // check inner status
        for i in 2..=5 {
            let node_id = i;
            let replica_id = i;
            let node = node_manager.get_node(&node_id).unwrap();
            assert_eq!(node.group_map.contains_key(&group_id), true);

            let rep = replica_cache
                .replica_desc(group_id, replica_id)
                .await
                .unwrap()
                .unwrap();

            assert_eq!(
                rep,
                ReplicaDesc {
                    group_id,
                    node_id,
                    replica_id,
                }
            );
        }
        assert_eq!(raft_group.node_ids, vec![1, 2, 3, 4, 5]);

        // remove nodes for many times
        for _ in 0..5 {
            for i in 2..=5 {
                let node_id = i;
                let replica_id = i;
                TestMultiRaftActorRuntime::remove_replica(
                    1,
                    &mut raft_group,
                    &mut node_manager,
                    &mut replica_cache,
                    node_id,
                    replica_id,
                )
                .await;
            }
        }

        for i in 2..=5 {
            let node_id = i;
            let replica_id = i;
            // TODO: if node group is empty, should remove node item from map
            let node = node_manager.get_node(&node_id).unwrap();
            assert_eq!(node.group_map.contains_key(&group_id), false);

            assert_eq!(
                replica_cache
                    .replica_desc(group_id, replica_id)
                    .await
                    .unwrap()
                    .is_none(),
                true
            );
        }

        assert_eq!(raft_group.node_ids, vec![1]);
    }
}
