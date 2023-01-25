use std::collections::hash_map::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
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
use raft_proto::prelude::ConfChangeType;
use raft_proto::prelude::Entry;
use raft_proto::prelude::Message;
use raft_proto::prelude::MessageType;
use raft_proto::prelude::RaftMessage;
use raft_proto::prelude::RaftMessageResponse;
use raft_proto::prelude::ReplicaDesc;
use raft_proto::prelude::MembershipChangeRequest;

use tracing::debug;
use tracing::error;
use tracing::trace;
use tracing::warn;
use tracing::Level;

use crate::multiraft::error::RaftGroupError;
use crate::util::Stopper;
use crate::util::TaskGroup;

use super::apply_actor::ApplyActor;
// use super::apply_actor::ApplyActorReceiver;
// use super::apply_actor::ApplyActorSender;
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
use super::raft_group::RaftGroupState;
use super::raft_group::RaftGroupWriteRequest;
use super::replica_cache::ReplicaCache;
use super::storage::MultiRaftStorage;
use super::transport;
use super::transport::Transport;

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
            last_tick: Instant::now(),
            raft_ticks: 0,
            sync_replica_cache: true,
            replica_cache: ReplicaCache::new(storage.clone()),
            pending_events: Vec::new(),
            pending_response_cbs: Vec::new(),
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

    pub fn start(&self, task_group: &TaskGroup) {
        self.apply.start(task_group);

        let runtime = { self.runtime.lock().unwrap().take().unwrap() };

        let stopper = task_group.stopper();
        let jh = task_group.spawn(async move {
            runtime.main_loop(stopper).await;
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
    last_tick: Instant,
    tick_interval: Duration,
    raft_ticks: usize,
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
    apply_request_tx: Sender<ApplyTaskRequest>,
    apply_response_rx: Receiver<ApplyTaskResponse>,
    // write_actor_address: WriteAddress,
    // waiting_ready_groups: VecDeque<HashMap<u64, Ready>>,
    // proposals: ProposalQueueManager,
    // task_group: TaskGroup,
    pending_response_cbs: Vec<ResponseCb>,
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
        name = "MutiRaftActorInner::start"
        level = Level::INFO,
        skip(self),
        fields(node_id=self.node_id)
    )]
    async fn main_loop(mut self, mut stopper: Stopper) {
        // Each time ticker expires, the ticks increments,
        // when ticks >= heartbeat_tick triggers the merged heartbeat.
        // let mut ticks = 0;
        // let mut ticker =
        //     tokio::time::interval_at(Instant::now() + self.tick_interval, self.tick_interval);
        // let mut activity_groups = HashSet::new();

        // let mut ready_ticker = tokio::time::interval_at(
        //     Instant::now() + Duration::from_millis(10),
        //     Duration::from_millis(10),
        // );
        let mut ticker = tokio::time::interval(Duration::from_millis(1));

        self.raft_ticks = 0;
        self.last_tick = Instant::now();

        // let mut stopper = self.task_group.stopper();
        loop {
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

                _ = ticker.tick() => {
                    self.campaign().await;
                }
            //     _ = ticker.tick() => {
            //         // tick all groups
            //         self.groups.iter_mut().for_each(|(_, group)| {
            //             if group.raft_group.tick() {
            //                 self.activity_groups.insert(group.group_id);
            //             }
            //         });

            //         ticks += 1;
            //         if ticks >= self.heartbeat_tick {
            //             ticks = 0;
            //             self.coalesced_heratbeat().await;
            //         }
            //     },

            //     Some((msg, tx)) = self.raft_message_rx.recv() => self.handle_raft_message(msg, tx).await,

                Some((group_id, tx)) = self.campaign_rx.recv() => self.campaign_raft(group_id, tx),

            //     // handle application write request
            //     Some((request, tx)) = self.write_propose_rx.recv() => self.handle_write_request(request, tx),

            //     // handle application read index request.
            //     Some((request, tx)) = self.read_index_propose_rx.recv() => self.handle_read_index_request(request, tx),

            //     Some((msg, tx)) = self.admin_rx.recv() => self.handle_admin_request(msg, tx).await,

            //     // handle apply actor response.
            //     Some(response) = self.apply_actor_rx.rx.recv() => self.handle_apply_task_response(response).await,

            //     // handle application callback event.
            //     Some(msg) = self.callback_event_rx.recv() => {
            //         self.handle_callback(msg).await;
            //     },

            //     _ = ready_ticker.tick() => {
            //         if !self.activity_groups.is_empty() {
            //             self.on_multi_groups_ready().await;
            //             self.activity_groups.clear();
            //             // TODO: shirk_to_fit
            //         }
            //     }
            }
        }
    }

    // #[tracing::instrument(
    //     level = Level::TRACE,
    //     name = "MultiRaftActorInner::campaign",
    //     skip_all
    // )]
    async fn campaign(&mut self) {
        // TODO: We need to process the whole message
        // loop as soon as possible, and the message
        // notifications should be put in place.
        self.recv_raft_msgs().await;

        // tick all groups
        self.handle_ticks().await;

        self.recv_proposes().await;

        self.handle_admin().await;

        if !self.activity_groups.is_empty() {
            self.handle_readys().await;
            self.activity_groups.clear();
            // TODO: shirk_to_fit
        }

        self.handle_apply_results().await;

        if !self.pending_response_cbs.is_empty() {
            self.handle_response_callbacks();
        }
    }

    async fn recv_raft_msgs(&mut self) {
        loop {
            let (msg, tx) = match self.raft_message_rx.try_recv() {
                Ok(msg) => msg,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    error!("raft_message sender of channel dropped");
                    break;
                }
            };
            let response_cb: ResponseCb = match self.step_raft_msg(msg).await {
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
            self.pending_response_cbs.push(response_cb);
        }
    }

    #[tracing::instrument(
        level = Level::TRACE,
        name = "MultiRaftActorInner::step_raft_msg",
        skip(self)
    )]
    async fn step_raft_msg(
        &mut self,
        mut msg: RaftMessage,
        // tx: oneshot::Sender<Result<RaftMessageResponse, Error>>,
    ) -> Result<RaftMessageResponse, Error> {
        let raft_msg = msg.msg.take().expect("invalid message");
        match raft_msg.msg_type() {
            MessageType::MsgHeartbeat => {
                return self.fanout_heartbeat(msg).await;
            }
            MessageType::MsgHeartbeatResponse => {
                return self.fanout_heartbeat_response(msg).await;
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

        let _ = group.raft_group.step(raft_msg).map_err(|err| {
            error!("step raft msg error {}", err);
            Error::Raft(err)
        })?;

        self.activity_groups.insert(group_id);
        Ok(RaftMessageResponse {})

        // if let Err(err) = self
        //     .replica_cache
        //     .cache_replica_desc(group_id, from_replica.clone(), self.sync_replica_cache)
        //     .await
        // {
        //     error!(
        //         "cache group {} {:?} in node {} occurs error: {}",
        //         group_id, from_replica, self.node_id, err
        //     );
        //     let cb = Box::new(move || -> Result<(), Error> {
        //         tx.send(Err(err)).map_err(|_| {
        //             Error::Internal(format!(
        //                 "response RaftMessageReuqest error, receiver dropped"
        //             ))
        //         })
        //     });
        //     self.pending_response_cbs.push(cb);
        //     // tx.send(Err(err)).unwrap();
        //     return;
        // }

        // if let Err(err) = self
        //     .replica_cache
        //     .cache_replica_desc(group_id, to_replica.clone(), self.sync_replica_cache)
        //     .await
        // {
        //     error!(
        //         "cache group {} {:?} in node {} occurs error: {}",
        //         group_id, to_replica, self.node_id, err
        //     );
        //     let cb = Box::new(move || -> Result<(), Error> {
        //         tx.send(Err(err)).map_err(|_| {
        //             Error::Internal(format!(
        //                 "response RaftMessageReuqest error, receiver dropped"
        //             ))
        //         })
        //     });
        //     self.pending_response_cbs.push(cb);
        //     // tx.send(Err(err)).unwrap();
        //     return;
        // }

        // if !self.node_manager.contains_node(&from_replica.node_id) {
        //     self.node_manager.add_node(from_replica.node_id, group_id);
        // }

        // // if a group exists, try to maintain groups on the node
        // if self.groups.contains_key(&group_id)
        //     && !self.node_manager.contains_node(&to_replica.node_id)
        // {
        //     self.node_manager.add_node(to_replica.node_id, group_id);
        // }

        // let group = match self.groups.get_mut(&group_id) {
        //     Some(group) => group,
        //     None => {
        //         // if we receive initialization messages from a raft replica
        //         // on another node, this means that a member change has occurred
        //         // and we should create a new raft replica.
        //         // Note. MsgHeartbeat && commit = 0 is not initial message because we
        //         // hijacked in fanout_heartbeat.
        //         if let Err(err) = self
        //             .create_raft_group(group_id, to_replica.replica_id, msg.replicas)
        //             .await
        //         {
        //             error!("create raft group error {}", err);
        //             let cb = Box::new(move || -> Result<(), Error> {
        //                 tx.send(Err(err)).map_err(|_| {
        //                     Error::Internal(format!(
        //                         "response RaftMessageReuqest error, receiver dropped"
        //                     ))
        //                 })
        //             });
        //             self.pending_response_cbs.push(cb);
        //             // tx.send(Err(err)).unwrap();
        //             return;
        //         }
        //         self.groups.get_mut(&group_id).unwrap()
        //     }
        // };

        // if let Err(err) = group.raft_group.step(raft_msg) {
        //     error!("step raft msg error {}", err);
        //     // tx.send(Err(Error::Raft(err))).unwrap();
        //     let cb = Box::new(move || -> Result<(), Error> {
        //         tx.send(Err(Error::Raft(err))).map_err(|_| {
        //             Error::Internal(format!(
        //                 "response RaftMessageReuqest error, receiver dropped"
        //             ))
        //         })
        //     });
        //     self.pending_response_cbs.push(cb);
        //     return;
        // }

        // self.activity_groups.insert(group_id);
        // let cb = Box::new(move || -> Result<(), Error> {
        //     tx.send(Ok(RaftMessageResponse {})).map_err(|_| {
        //         Error::Internal(format!(
        //             "response RaftMessageReuqest error, receiver dropped"
        //         ))
        //     })
        // });
        // self.pending_response_cbs.push(cb);
        // if let Err(_) = tx.send(Ok(RaftMessageResponse {})) {
        //     // the server of transport dropped
        //     error!("failed to reespond to RaftMessageRequest, the receiver dropped");
        // }
    }

    async fn handle_ticks(&mut self) {
        if self.last_tick.elapsed() >= self.tick_interval {
            // tick all groups
            self.groups.iter_mut().for_each(|(_, group)| {
                if group.raft_group.tick() {
                    self.activity_groups.insert(group.group_id);
                }
            });
            self.last_tick = Instant::now();

            self.raft_ticks += 1;
            if self.raft_ticks >= self.cfg.heartbeat_tick {
                self.raft_ticks = 0;
                self.coalesced_heratbeat().await;
            }
        }
    }

    /// The node sends heartbeats to other nodes instead
    /// of all raft groups on that node.
    async fn coalesced_heratbeat(&self) {
        for (node_id, _) in self.node_manager.iter() {
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
                replicas: vec![],
                msg: Some(raft_msg),
            };

            if let Err(_error) = self.transport.send(msg) {}
        }
    }

    async fn recv_proposes(&mut self) {
        loop {
            let (msg, tx) = match self.write_propose_rx.try_recv() {
                Ok(msg) => msg,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    error!("write_propose sender of channel dropped");
                    break;
                }
            };
            if let Some(cb) = self.propose_write_request(msg, tx) {
                self.pending_response_cbs.push(cb)
            }
        }

        loop {
            let (msg, tx) = match self.read_index_propose_rx.try_recv() {
                Ok(msg) => msg,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    error!("read_index sender of channel dropped");
                    break;
                }
            };
            if let Some(cb) = self.propose_read_index_request(msg, tx) {
                self.pending_response_cbs.push(cb)
            }
        }
    }

    /// if `None` is returned, the write request is successfully committed
    /// to raft, otherwise the callback closure of the error response is
    /// returned.
    ///
    /// Note: Must be called to respond to the client when the loop ends.
    #[tracing::instrument(
        level = Level::TRACE,
        name = "MultiRaftActor::propose_write_request",
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

    /// if `None` is returned, the read_index request is successfully committed
    /// to raft, otherwise the callback closure of the error response is
    /// returned.
    ///
    /// Note: Must be called to respond to the client when the loop ends.
    #[tracing::instrument(
        level = Level::TRACE,
        name = "MultiRaftActor::propose_read_index_request",
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
        name = "MultiRaftActorInner::campagin_raft", 
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

    async fn handle_admin(&mut self) {
        let (msg, tx) = match self.admin_rx.try_recv() {
            Ok(msg) => msg,
            Err(TryRecvError::Empty) => return,
            Err(TryRecvError::Disconnected) => {
                error!("write_propose sender of channel dropped");
                return;
            }
        };
        if let Some(cb) = self.handle_admin_request(msg, tx).await {
            self.pending_response_cbs.push(cb)
        }
    }

    #[tracing::instrument(
        name = "MultiRaftActor::handle_admin_request",
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
        name = "MultiRaftActor::raft_group_management",
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
        name = "MultiRaftActor::create_raft_group", 
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
            max_size_per_msg: 1024 * 1024,
            max_inflight_msgs: 256,
            ..Default::default()
        };

        let raft_store = group_storage.clone();
        let raft_group = raft::RawNode::with_default_logger(&raft_cfg, raft_store)
            .map_err(|err| Error::Raft(err))?;

        let mut group = RaftGroup {
            group_id,
            replica_id,
            raft_group,
            node_ids: Vec::new(),
            proposals: GroupProposalQueue::new(replica_id),
            leader: ReplicaDesc::default(), // TODO: init leader from storage
            committed_term: 0,              // TODO: init committed term from storage
            state: RaftGroupState::default(),
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
            // updating all valid LederElections is required because the lastest commited_term
            // of the follower cannot be updated until after the leader committed.
            let pending_events = std::mem::take(&mut self.pending_events)
                .into_iter()
                .filter_map(|mut pending_event| {
                    match pending_event {
                        Event::LederElection(ref mut leader_elect) => {
                            if let Some(group) = self.groups.get(&leader_elect.group_id) {
                                if leader_elect.committed_term != group.committed_term
                                    && group.committed_term != 0
                                {
                                    leader_elect.committed_term = group.committed_term;
                                }
                                Some(pending_event)
                            } else {
                                // group is removed, but event incoming, so ignore it.
                                None
                            }
                        }
                        _ => Some(pending_event),
                    }
                })
                .collect::<Vec<_>>();

            self.event_tx.send(pending_events).await.unwrap();
        }
    }

    /// Fanout heartbeats from other nodes to all raft groups on this node.
    async fn fanout_heartbeat(&mut self, msg: RaftMessage) -> Result<RaftMessageResponse, Error> {
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
                self.activity_groups.insert(*group_id);

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
        Ok(RaftMessageResponse {})
    }

    async fn handle_apply_results(&mut self) {
        loop {
            let msg = match self.apply_response_rx.try_recv() {
                Ok(msg) => msg,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    error!("raft_message sender of channel dropped");
                    break;
                }
            };
            self.advance_apply(msg).await;
        }
    }

    #[tracing::instrument(
        level = Level::TRACE,
        name = "MultiRaftActor::handle_apply_task_response",
        skip(self))
    ]
    async fn advance_apply(&mut self, response: ApplyTaskResponse) {
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
            trace!(
                " apply state change = {:?}, group = {}",
                group.state.apply_state,
                group_id
            );
            group.raft_group.advance_apply();
            trace!("advance apply, group = {}", group_id);
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
            .send(Err(Error::RaftGroup(RaftGroupError::NotExist(
                self.node_id,
                group_id,
            ))))
            .unwrap()
    }

    #[tracing::instrument(
        level = Level::TRACE,
        name = "MultiRaftActorInner::handle_readys",
        skip(self)
    )]
    pub(crate) async fn handle_readys(&mut self) {
        let mut multi_groups_write = HashMap::new();
        let mut multi_groups_apply = HashMap::new();
        for group_id in self.activity_groups.iter() {
            if *group_id == NO_GORUP {
                continue;
            }

            if let Some(group) = self.groups.get_mut(group_id) {
                group
                    .on_ready(
                        self.node_id,
                        &self.transport,
                        &self.storage,
                        &mut self.replica_cache,
                        &mut self.node_manager,
                        &mut multi_groups_write,
                        &mut multi_groups_apply,
                        &mut self.pending_events,
                    )
                    .await;
            }
        }
        if !multi_groups_apply.is_empty() {
            if let Err(_err) = self
                .apply_request_tx
                .send(ApplyTaskRequest {
                    groups: multi_groups_apply,
                })
                .await
            {
                // FIXME
                warn!("apply actor stopped");
            }
        }

        let gwrs = self.do_multi_groups_write(multi_groups_write).await;
        self.do_multi_groups_write_finish(gwrs).await;
    }

    #[tracing::instrument(
        level = Level::TRACE,
        name = "MultiRaftActorInner::on_multi_groups_write", 
        skip_all
    )]
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
                    .do_write(
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

    #[tracing::instrument(
        level = Level::TRACE,
        name = "MultiRaftActorInner::on_multi_groups_write_finish", 
        skip_all
    )]
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
                .do_write_finish(
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
            if let Err(_err) = self
                .apply_request_tx
                .send(ApplyTaskRequest {
                    groups: multi_groups_apply,
                })
                .await
            {
                // FIXME
                warn!("apply actor stopped");
            }
        }
    }
    #[tracing::instrument(
        name = "MultiRaftActorInner::handle_response_callbacks"
        level = Level::TRACE,
        skip_all
    )]
    fn handle_response_callbacks(&mut self) {
        let drainner = self.pending_response_cbs.drain(..).collect::<Vec<_>>();
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

    #[tracing::instrument(
        name = "MultiRaftActorInner::do_stop"
        level = Level::TRACE,
        skip_all
    )]
    fn do_stop(mut self) {
        self.state.stopped.store(true, Ordering::Release);
    }
}
