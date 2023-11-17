use std::collections::hash_map::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use raft::prelude::ConfState;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;
use tracing::Level;
use tracing::Span;

use super::handle_ready::GroupWriteRequest;
use crate::msg::MessageWithNotify;
use crate::msg::NodeMessage;
use crate::multiraft::ProposeResponse;
use crate::prelude::ConfChangeType;
use crate::prelude::MessageType;
use crate::prelude::MultiRaftMessage;
use crate::prelude::MultiRaftMessageResponse;
use crate::prelude::ReplicaDesc;
use crate::utils;
use crate::utils::mpsc;

use super::group::RaftGroup;
use crate::apply::ApplyActor;
use crate::config::Config;
use crate::error::ChannelError;
use crate::error::Error;
use crate::error::RaftGroupError;
use crate::msg::ApplyConfChange;
use crate::msg::ApplyData;
use crate::msg::ApplyMessage;
use crate::multiraft::NO_GORUP;
use crate::node::NodeManager;
use crate::replica_cache::ReplicaCache;
use crate::rsm::StateMachine;
use crate::state::GroupStates;
use crate::storage::MultiRaftStorage;
use crate::storage::RaftStorage;
use crate::tick::Ticker;
use crate::transport::Transport;
use crate::ProposeRequest;
/// Shrink queue if queue capacity more than and len less than
/// this value.
const SHRINK_CACHE_CAPACITY: usize = 64;

/// ResponseCallback is a callback function for response client.
pub(crate) type ResponseCallback = Box<dyn FnOnce() -> Result<(), Error> + Send + Sync + 'static>;

/// ResponseCallbackQueue is a queue for response client.
pub(crate) struct ResponseCallbackQueue {
    cbs: VecDeque<ResponseCallback>,
}

impl ResponseCallbackQueue {
    /// Create a new ResponseCallbackQueue.
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

pub struct NodeActor<Request, Response>
where
    Request: ProposeRequest,
    Response: ProposeResponse,
{
    node_msg_tx: mpsc::WrapSender<NodeMessage<Request, Response>>,
    peer_msg_tx: mpsc::WrapSender<
        MessageWithNotify<MultiRaftMessage, Result<MultiRaftMessageResponse, Error>>,
    >,
    #[allow(unused)]
    apply: ApplyActor,
}

impl<Request, Response> NodeActor<Request, Response>
where
    Request: ProposeRequest,
    Response: ProposeResponse,
{
    /// Spawn a new node actor.
    pub fn spawn<TR, RS, MRS, RSM>(
        cfg: &Config,
        transport: &TR,
        storage: &MRS,
        rsm: RSM,
        // event_bcast: &EventChannel,
        ticker: Option<Box<dyn Ticker>>,
        states: GroupStates,
        stopped: Arc<AtomicBool>,
    ) -> Self
    where
        TR: Transport + Clone,
        RS: RaftStorage,
        MRS: MultiRaftStorage<RS>,
        RSM: StateMachine<Request, Response>,
    {
        let (tx, rx) = mpsc::channel_wrap::<NodeMessage<Request, Response>>(-1);
        let (peer_msg_tx, peer_msg_rx) = mpsc::channel_wrap(-1);
        let (apply_request_tx, apply_request_rx) = unbounded_channel();
        let rsm = Arc::new(rsm);
        let apply = ApplyActor::spawn(
            cfg,
            &rsm,
            storage.clone(),
            states.clone(),
            tx.clone(),
            apply_request_rx,
            stopped.clone(),
        );

        let mut worker = Inner::<TR, RS, MRS, RSM, Request, Response>::new(
            cfg,
            transport,
            storage,
            rsm,
            rx,
            peer_msg_rx,
            apply_request_tx,
            states,
        );

        tokio::spawn(async move {
            worker.restore().await;
            worker.main_loop(ticker, stopped).await;
        });

        Self {
            node_msg_tx: tx,
            peer_msg_tx,
            apply,
        }
    }

    pub fn try_send_node_msg(&self, msg: NodeMessage<Request, Response>) -> Result<(), Error> {
        match self.node_msg_tx.try_send(msg) {
            Err(mpsc::TrySendError::Full(_)) => Err(Error::Channel(ChannelError::Full(
                "channel no avaiable capacity for write".to_owned(),
            ))),
            Err(mpsc::TrySendError::Closed(_)) => Err(Error::Channel(
                ChannelError::ReceiverClosed("channel receiver closed for write".to_owned()),
            )),
            Ok(_) => Ok(()),
        }
    }

    pub fn clone_peer_msg_sender(
        &self,
    ) -> mpsc::WrapSender<
        MessageWithNotify<MultiRaftMessage, Result<MultiRaftMessageResponse, Error>>,
    > {
        self.peer_msg_tx.clone()
    }
}

pub(super) struct Inner<TR, RS, MRS, M, W, R>
where
    TR: Transport,
    RS: RaftStorage,
    MRS: MultiRaftStorage<RS>,
    M: StateMachine<W, R>,
    W: ProposeRequest,
    R: ProposeResponse,
{
    pub(super) cfg: Config,
    pub(super) node_id: u64,
    pub(super) storage: MRS,
    pub(super) transport: TR,
    pub(super) node_manager: NodeManager,
    pub(super) replica_cache: ReplicaCache<RS, MRS>,
    pub(super) groups: HashMap<u64, RaftGroup<RS, R>>,
    pub(super) active_groups: HashSet<u64>,
    pub(super) pending_responses: ResponseCallbackQueue,
    pub(super) node_msg_rx: mpsc::WrapReceiver<NodeMessage<W, R>>,
    pub(super) peer_msg_rx: mpsc::WrapReceiver<
        MessageWithNotify<MultiRaftMessage, Result<MultiRaftMessageResponse, Error>>,
    >,
    pub(super) apply_msg_tx: UnboundedSender<(Span, ApplyMessage<R>)>,
    pub(super) shared_states: GroupStates,
    pub(super) rsm: Arc<M>,
}

impl<TR, RS, MRS, M, WD, RES> Inner<TR, RS, MRS, M, WD, RES>
where
    TR: Transport + Clone,
    RS: RaftStorage,
    MRS: MultiRaftStorage<RS>,
    M: StateMachine<WD, RES>,
    WD: ProposeRequest,
    RES: ProposeResponse,
{
    fn new(
        cfg: &Config,
        transport: &TR,
        storage: &MRS,
        rsm: Arc<M>,
        rx: utils::mpsc::WrapReceiver<NodeMessage<WD, RES>>,
        peer_msg_rx: mpsc::WrapReceiver<
            MessageWithNotify<MultiRaftMessage, Result<MultiRaftMessageResponse, Error>>,
        >,
        apply_request_tx: UnboundedSender<(Span, ApplyMessage<RES>)>,
        shared_states: GroupStates,
    ) -> Self {
        Inner::<TR, RS, MRS, M, WD, RES> {
            cfg: cfg.clone(),
            node_id: cfg.node_id,
            node_manager: NodeManager::new(),
            groups: HashMap::new(),
            node_msg_rx: rx,
            peer_msg_rx,
            rsm,
            storage: storage.clone(),
            transport: transport.clone(),
            apply_msg_tx: apply_request_tx,
            active_groups: HashSet::new(),
            replica_cache: ReplicaCache::new(storage.clone()),
            // event_chan: event_chan.clone(),
            pending_responses: ResponseCallbackQueue::new(),
            shared_states,
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
        tracing::info!("node {}: start main loop", self.node_id);

        // create default ticker if ticker is None.
        let tick_interval = Duration::from_millis(self.cfg.tick_interval);
        // let mut ticker = ticker.map_or(
        //     Box::new(tokio::time::interval_at(
        //         tokio::time::Instant::now() + tick_interval,
        //         tick_interval,
        //     )) as Box<dyn Ticker>,
        //     |t| t,
        // );

        let mut ticker =
            tokio::time::interval_at(tokio::time::Instant::now() + tick_interval, tick_interval);

        let mut ticks = 0;
        loop {
            if stopped.load(std::sync::atomic::Ordering::SeqCst) {
                self.do_stop();
                break;
            }

            // self.event_chan.flush();
            tokio::select! {
                // Note: see https://github.com/tokio-rs/tokio/discussions/4019 for more
                // information about why mut here.

                Some(msg) = self.node_msg_rx.recv() => self.handle_node_msg(msg).await,

                Some(msg) = self.peer_msg_rx.recv() => {
                    let res = self.handle_peer_msg(msg.msg).await;
                    self.pending_responses
                        .push_back(ResponseCallbackQueue::new_callback(msg.tx, res));
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

                else => {},
            }

            if !self.active_groups.is_empty() {
                self.handle_readys().await;
                /* here is active groups already drained */
            }

            self.pending_responses.flush();
        }
    }

    async fn handle_node_msg(&mut self, msg: NodeMessage<WD, RES>) {
        match msg {
            NodeMessage::Write(msg) => self.handle_write_msg(msg),
            NodeMessage::ReadIndex(msg) => self.handle_readindex_msg(msg),
            NodeMessage::ConfChange(msg) => self.handle_conf_change_msg(msg),
            NodeMessage::CreateGroup(msg) => {
                if let Some(cb) = self.handle_create_group_msg(msg.msg, msg.tx).await {
                    self.pending_responses.push_back(cb);
                }
            }
            NodeMessage::RemoveGroup(msg) => {
                if let Some(cb) = self.handle_remove_group_msg(msg.msg, msg.tx).await {
                    self.pending_responses.push_back(cb);
                }
            }
            NodeMessage::Campaign(msg) => {
                self.campaign_raft(msg.msg, msg.tx);
            }
            // NodeMessage::ApplyResult(msg) => self.handle_apply_result(msg).await,
            NodeMessage::ApplyResult(msg) => self.handle_apply_result_msg(msg).await,
            NodeMessage::Inner(msg) => self.handle_inner_msg(msg).await,
        }
    }

    async fn handle_peer_msg(
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
        level = Level::TRACE,
        name = "NodeActor::campagin_raft", 
        skip(
            self, 
            tx
        )
    )]
    fn campaign_raft(&mut self, group_id: u64, tx: oneshot::Sender<Result<(), Error>>) {
        let res = if let Some(group) = self.groups.get_mut(&group_id) {
            self.active_groups.insert(group_id);
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

    async fn commit_membership_change(
        &mut self,
        mut view: ApplyConfChange,
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

        // The leader communicates with the new member after the membership change,
        // sends the snapshot contains the member configuration, and then follower
        // install snapshot.
        // The Append Entries are then received by followers and committed, but the
        // new member configuration is already applied to the followers when the
        // snapshot is installed. In raft-rs, duplicate joint consensus is not allowed,
        // so we catch the error and skip.
        let conf = group.raft_group.raft.prs().conf().to_conf_state();
        if view.conf_change.enter_joint().is_some() && !conf.voters_outgoing.is_empty() {
            debug!(
                "node {}: for group {} replica {} skip alreay entered joint conf_change {:?}",
                self.node_id, group_id, group.replica_id, view.conf_change
            );
            return Ok(conf);
        }

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

        // if !view.conf_change.leave_joint() && view.conf_change.enter_joint().is_some() {
        //     debug!(
        //         "node {}: for group {} replica {} skip alreay entered joint conf_change {:?}",
        //         self.node_id, group_id, group.replica_id, view.conf_change
        //     );
        //     let conf_state = group.raft_group.raft.prs().conf().to_conf_state();
        //     if !conf_state.voters_outgoing.is_empty() {
        //         return Ok(conf_state);
        //     }
        // }

        return self.apply_conf_change(view).await;
    }

    async fn apply_conf_change(
        &mut self,
        // group_id: u64,
        // group: &mut RaftGroup<RS, RES>,
        view: ApplyConfChange,
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

            let err = match self.handle_group_ready(group_id).await {
                Ok((gwr, apply)) => {
                    gwr.map(|write| writes.insert(group_id, write));
                    apply.map(|apply| applys.insert(group_id, apply));
                    continue;
                }
                Err(err) => err,
            };

            match err {
                Error::Storage(storage_err) => match storage_err {
                    crate::storage::Error::StorageTemporarilyUnavailable => {
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
                Error::RaftGroup(raft_group_err) => match raft_group_err {
                    RaftGroupError::NotExist(node_id, group_id) => {
                        // TODO: remove pending proposals related to this group
                        tracing::error!(
                            "node {}: handle group-{} ready, but not exist",
                            node_id,
                            group_id
                        );
                        continue;
                    }
                    _ => {
                        tracing::warn!(
                            "node {}: group {} raft storage to handle_ready got error: {}",
                            self.node_id,
                            group_id,
                            raft_group_err
                        );
                        continue;
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

    async fn handle_writes(&mut self, mut writes: HashMap<u64, GroupWriteRequest>) {
        let mut applys = HashMap::new();

        // TODO(yuanchang.xu) Disk write flow control
        for (group_id, gwr) in writes.iter_mut() {
            // TODO: cache storage in related raft group.
            // let gs = match self.storage.group_storage(*group_id, gwr.replica_id).await {
            //     Ok(gs) => gs,
            //     Err(err) => {
            //         match err {
            //             super::storage::Error::StorageTemporarilyUnavailable => {
            //                 warn!("node {}: group {} handle_write but storage temporarily unavailable ", self.node_id, group_id);

            //                 self.active_groups.insert(*group_id);
            //                 continue;
            //             }
            //             super::storage::Error::StorageUnavailable => {
            //                 panic!("node {}: storage unavailable", self.node_id)
            //             }
            //             _ => {
            //                 warn!(
            //                     "node {}: get raft storage for group {} to handle_writes error: {}",
            //                     self.node_id, *group_id, err
            //                 );
            //                 continue;
            //             }
            //         }
            //     }
            // };

            // let group = match self.groups.get_mut(&group_id) {
            //     Some(group) => group,
            //     None => {
            //         // TODO: remove pending proposals related to this group
            //         // If the group does not exist at this point
            //         // 1. we may have finished sending messages to the group, role changed notifications,
            //         //    committable entires commits
            //         // 2. we may not have completed the new proposal append, there may be multiple scenarios
            //         //     - The current group is the leader, sent AE, but was deleted before it received a
            //         //       response from the follower, so it did not complete the append drop
            //         //     - The current group is the follower, which does not affect the completion of the
            //         //       AE
            //         error!(
            //             "node {}: handle group-{} write ready, but dropped",
            //             self.node_id, group_id
            //         );
            //         continue;
            //     }
            // };

            // let res = group
            //     .handle_write(
            //         self.node_id,
            //         gwr,
            //         &gs,
            //         &self.transport,
            //         &mut self.replica_cache,
            //         &mut self.node_manager,
            //     )
            //     .await;

            let res = self.handle_write(*group_id, gwr).await;

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
                crate::storage::Error::LogTemporarilyUnavailable
                | crate::storage::Error::SnapshotTemporarilyUnavailable
                | crate::storage::Error::StorageTemporarilyUnavailable => {
                    self.active_groups.insert(*group_id);
                    continue;
                }

                crate::storage::Error::LogUnavailable
                | crate::storage::Error::SnapshotUnavailable => {
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
            .apply_msg_tx
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

    use futures::Future;

    use super::Inner;
    use crate::proposal::ProposalQueue;
    use crate::proposal::ReadIndexQueue;
    use crate::rsm_event;
    use crate::storage::MemStorage;
    use crate::storage::MultiRaftMemoryStorage;
    use crate::StateMachine;

    use crate::node::group::RaftGroup;
    use crate::node::group::Status;

    use crate::prelude::ReplicaDesc;
    use crate::replica_cache::ReplicaCache;
    use crate::transport::LocalTransport;
    use crate::Error;
    use crate::MultiRaftMessageSenderImpl;

    use super::NodeManager;
    use crate::state::GroupState;

    struct NoOpStateMachine {}
    impl StateMachine<(), ()> for NoOpStateMachine {
        type ApplyFuture<'life0> = impl Future<Output = ()> + 'life0
        where
            Self: 'life0;
        fn apply(&self, _: rsm_event::ApplyEvent<(), ()>) -> Self::ApplyFuture<'_> {
            async move {}
        }

        type OnLeaderElectionFuture<'life0> = impl Future<Output = ()> + 'life0
        where
            Self: 'life0;
        fn on_leader_election(
            &self,
            _: rsm_event::LeaderElectionEvent,
        ) -> Self::OnLeaderElectionFuture<'_> {
            async move {}
        }

        type OnGroupCreateFuture<'life0> = impl Future<Output = ()> + 'life0
        where
            Self: 'life0;
        fn on_group_create(&self, _: rsm_event::GroupCreateEvent) -> Self::OnGroupCreateFuture<'_> {
            async move {}
        }
    }

    type TestMultiRaftActorRuntime = Inner<
        LocalTransport<MultiRaftMessageSenderImpl>,
        MemStorage,
        MultiRaftMemoryStorage,
        NoOpStateMachine,
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
            assert_eq!(node.groups.contains_key(&group_id), true);

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
            assert_eq!(node.groups.contains_key(&group_id), false);

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
            assert_eq!(node.groups.contains_key(&group_id), true);

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
            assert_eq!(node.groups.contains_key(&group_id), false);

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