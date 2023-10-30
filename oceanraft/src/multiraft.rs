use std::cell::RefCell;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::sync::Mutex;

use futures::Future;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::msg::MessageWithNotify;
use crate::msg::NodeMessage;
use crate::prelude::CreateGroupRequest;
use crate::prelude::MembershipChangeData;
use crate::prelude::MultiRaftMessage;
use crate::prelude::MultiRaftMessageResponse;
use crate::protos::RemoveGroupRequest;
use crate::utils::mpsc;

use super::config::Config;
use super::error::ChannelError;
use super::error::Error;
use super::event::EventChannel;
use super::event::EventReceiver;
// use super::msg::ManageMessage;
use super::msg::MembershipRequest;
// use super::msg::ProposeMessage;
// use super::msg::QueryGroup;
use super::msg::ReadIndexContext;
use super::msg::ReadIndexRequest;
use super::msg::WriteRequest;
use super::node::NodeActor;
use super::state::GroupStates;
use super::storage::MultiRaftStorage;
use super::storage::RaftStorage;
use super::tick::Ticker;
use super::transport::Transport;
use super::RaftGroupError;
use super::StateMachine;

pub const NO_GORUP: u64 = 0;
pub const NO_NODE: u64 = 0;
pub const NO_LEADER: u64 = 0;

/// Propose request can be with custom data types
/// for which `ProposeRequest` provides trait constraints.
pub trait ProposeRequest:
    Debug + Clone + Send + Sync + Serialize + for<'d> Deserialize<'d> + 'static
{
}

impl<T> ProposeRequest for T where
    T: Debug + Clone + Send + Sync + Serialize + for<'d> Deserialize<'d> + 'static
{
}

/// Propose and membership change requests can be responded with custom types
/// for which `ProposePropose` provides trait constraints.
pub trait ProposeResponse: Debug + Clone + Send + Sync + 'static {}

impl<T> ProposeResponse for T where T: Debug + Clone + Send + Sync + 'static {}

pub trait MultiRaftTypeSpecialization {
    type D: ProposeRequest;
    type R: ProposeResponse;
    type M: StateMachine<Self::D, Self::R>;
    type S: RaftStorage;
    type MS: MultiRaftStorage<Self::S>;
}

/// Send `MultiRaftMessage` to `MuiltiRaft`.
///
/// When the server receives a `MultiRaftMessage` from another node,
/// it should send it to `MultiRaft` through the implementors of
/// the trait for further processing.
pub trait MultiRaftMessageSender: Send + Sync + 'static {
    type SendFuture<'life0>: Future<Output = Result<MultiRaftMessageResponse, Error>> + Send
    where
        Self: 'life0;

    /// Send `MultiRaftMessage` to `MultiRaft`. the implementor should return future.
    fn send<'life0>(&'life0 self, msg: MultiRaftMessage) -> Self::SendFuture<'life0>;
}

#[derive(Clone)]
pub struct MultiRaftMessageSenderImpl {
    tx: mpsc::WrapSender<
        MessageWithNotify<MultiRaftMessage, Result<MultiRaftMessageResponse, Error>>,
    >,
}

impl MultiRaftMessageSender for MultiRaftMessageSenderImpl {
    type SendFuture<'life0> = impl Future<Output = Result<MultiRaftMessageResponse, Error>> + Send + 'life0
    where
        Self: 'life0;

    fn send<'life0>(&'life0 self, msg: MultiRaftMessage) -> Self::SendFuture<'life0> {
        async move {
            let (tx, rx) = oneshot::channel();
            let msg = MessageWithNotify { msg, tx };
            match self.tx.try_send(msg) {
                Err(mpsc::TrySendError::Closed(_)) => {
                    Err(Error::Channel(ChannelError::ReceiverClosed(
                        "channel receiver closed for raft message".to_owned(),
                    )))
                }
                Err(mpsc::TrySendError::Full(_)) => Err(Error::Channel(ChannelError::Full(
                    "channel receiver fulled for raft message".to_owned(),
                ))),
                Ok(_) => rx.await.map_err(|_| {
                    Error::Channel(ChannelError::ReceiverClosed(
                        "channel sender closed for raft message".to_owned(),
                    ))
                })?,
            }
        }
    }
}

/// MultiRaft represents a group of raft replicas
pub struct MultiRaft<T, TR>
where
    T: MultiRaftTypeSpecialization,
    TR: Transport + Clone,
{
    cfg: Config,
    node_id: u64,
    stopped: Arc<AtomicBool>,
    actor: Mutex<Option<NodeActor<T::D, T::R>>>,
    shared_states: GroupStates,
    event_bcast: EventChannel,
    storage: T::MS,
    transport: TR,
    state_machine: Mutex<Option<T::M>>,
    ticker: Mutex<Option<Box<dyn Ticker>>>,
    _m1: PhantomData<TR>,
}

impl<T, TR> MultiRaft<T, TR>
where
    T: MultiRaftTypeSpecialization,
    TR: Transport + Clone,
{
    pub fn new(
        cfg: Config,
        transport: TR,
        storage: T::MS,
        state_machine: T::M,
        ticker: Option<Box<dyn Ticker>>,
    ) -> Result<Self, Error> {
        cfg.validate()?;
        let node_id = cfg.node_id;
        let states = GroupStates::new();
        let event_bcast = EventChannel::new(cfg.event_capacity);
        let stopped = Arc::new(AtomicBool::new(false));

        Ok(Self {
            cfg,
            transport,
            storage,
            state_machine: Mutex::new(Some(state_machine)),
            ticker: Mutex::new(ticker),
            node_id,
            event_bcast,
            actor: Mutex::new(None),
            shared_states: states,
            stopped,
            _m1: PhantomData,
        })
    }

    pub fn start(&self) {
        let state_machine = {
            let mut state_machine = self.state_machine.lock().expect("lock state_machine");
            state_machine.take().expect("state_machine is none")
        };

        let ticker = {
            let mut ticker = self.ticker.lock().expect("lock ticker");
            ticker.take()
        };
        let actor = NodeActor::spawn(
            &self.cfg,
            &self.transport,
            &self.storage,
            state_machine,
            &self.event_bcast,
            ticker,
            self.shared_states.clone(),
            self.stopped.clone(),
        );

        self.actor.lock().expect("lock actor").replace(actor);
    }

    pub fn spawn(
        cfg: Config,
        transport: TR,
        storage: T::MS,
        state_machine: T::M,
        ticker: Option<Box<dyn Ticker>>,
    ) -> Result<Self, Error> {
        cfg.validate()?;
        let node_id = cfg.node_id;
        let states = GroupStates::new();
        let event_bcast = EventChannel::new(cfg.event_capacity);
        let stopped = Arc::new(AtomicBool::new(false));
        let actor = NodeActor::spawn(
            &cfg,
            &transport,
            &storage,
            state_machine,
            &event_bcast,
            ticker,
            states.clone(),
            stopped.clone(),
        );

        Ok(Self {
            cfg,
            transport,
            storage,
            state_machine: Mutex::new(None),
            ticker: Mutex::new(None),
            node_id,
            event_bcast,
            actor: Mutex::new(Some(actor)),
            shared_states: states,
            stopped,
            _m1: PhantomData,
        })
    }

    /// `write` the propose data to a specific group in the multiraft system.
    ///
    /// It is a blocking interface in an asynchronous environment. It waits until
    /// the proposal is successfully applied to the state machine  and the `RES and
    /// `context` are returned through the state machine created. If the proposal
    /// fails, an error is returned.
    ///
    /// ## Parameters
    /// - `group_id`: The specific consensus group to write to.
    /// - `term`: The expected term when writing. If the term of the raft log
    /// during application is less than this term, a "Stale" error is returned.
    /// - `context`: The context of the proposal. This information is not
    /// recorded in the raft log, but the context data will go through a
    /// complete write process.
    /// - `propose`: The proposed data, which implements the `ProposeData` type.
    /// This data will be recorded in the raft log.
    ///
    /// ## Errors
    /// Most errors require retries. The following error requires a different
    /// handling approach:
    /// - `ProposeError::NotLeader`: The application can refresh the leader and
    /// retry based on the error information using the route table.
    ///
    /// ## Panics
    pub async fn write(
        &self,
        group_id: u64,
        term: u64,
        context: Option<Vec<u8>>,
        propose: T::D,
    ) -> Result<(T::R, Option<Vec<u8>>), Error> {
        let rx = self.write_non_block(group_id, term, context, propose)?;
        rx.await.map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the write was dropped".to_owned(),
            ))
        })?
    }

    pub fn write_block(
        &self,
        group_id: u64,
        term: u64,
        context: Option<Vec<u8>>,
        data: T::D,
    ) -> Result<(T::R, Option<Vec<u8>>), Error> {
        let rx = self.write_non_block(group_id, term, context, data)?;
        rx.blocking_recv().map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the write was dropped".to_owned(),
            ))
        })?
    }

    fn pre_propose_check(&self, group_id: u64) -> Result<(), Error> {
        let state = self.shared_states.get(group_id).map_or(
            Err(Error::RaftGroup(RaftGroupError::Deleted(0, group_id))),
            |state| Ok(state),
        )?;

        if !state.is_leader() {
            return Err(Error::Propose(super::ProposeError::NotLeader {
                node_id: self.node_id,
                group_id,
                replica_id: state.get_replica_id(),
            }));
        }

        Ok(())
    }

    pub fn write_non_block(
        &self,
        group_id: u64,
        term: u64,
        context: Option<Vec<u8>>,
        data: T::D,
    ) -> Result<oneshot::Receiver<Result<(T::R, Option<Vec<u8>>), Error>>, Error> {
        let _ = self.pre_propose_check(group_id)?;

        let (tx, rx) = oneshot::channel();

        let msg = NodeMessage::Write(WriteRequest {
            group_id,
            term,
            propose: data,
            context,
            tx,
        });

        let mut actor = self.actor.lock().expect("lock actor");
        actor
            .as_ref()
            .expect("actor is none")
            .try_send_node_msg(msg)?;

        Ok(rx)
        // match self
        //     .actor
        //     .borrow()
        //     .as_ref()
        //     .unwrap()
        //     .node_msg_tx
        //     .try_send(NodeMessage::Write(WriteRequest {
        //         group_id,
        //         term,
        //         data,
        //         context,
        //         tx,
        //     })) {
        //     Err(mpsc::TrySendError::Full(_)) => Err(Error::Channel(ChannelError::Full(
        //         "channel no avaiable capacity for write".to_owned(),
        //     ))),
        //     Err(mpsc::TrySendError::Closed(_)) => Err(Error::Channel(
        //         ChannelError::ReceiverClosed("channel receiver closed for write".to_owned()),
        //     )),
        //     Ok(_) => Ok(rx),
        // }
    }

    pub async fn membership(
        &self,
        group_id: u64,
        term: Option<u64>,
        context: Option<Vec<u8>>,
        data: MembershipChangeData,
    ) -> Result<(T::R, Option<Vec<u8>>), Error> {
        let rx = self.membership_non_block(group_id, term, context, data)?;
        rx.await.map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the membership change was dropped".to_owned(),
            ))
        })?
    }

    pub fn membership_block(
        &self,
        group_id: u64,
        term: Option<u64>,
        context: Option<Vec<u8>>,
        data: MembershipChangeData,
    ) -> Result<(T::R, Option<Vec<u8>>), Error> {
        let rx = self.membership_non_block(group_id, term, context, data)?;
        rx.blocking_recv().map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the membership change was dropped".to_owned(),
            ))
        })?
    }

    pub fn membership_non_block(
        &self,
        group_id: u64,
        term: Option<u64>,
        context: Option<Vec<u8>>,
        data: MembershipChangeData,
    ) -> Result<oneshot::Receiver<Result<(T::R, Option<Vec<u8>>), Error>>, Error> {
        let _ = self.pre_propose_check(group_id)?;

        let (tx, rx) = oneshot::channel();

        let request = MembershipRequest {
            group_id,
            term,
            context,
            data,
            tx,
        };

        let msg = NodeMessage::Membership(request);
        let actor = self.actor.lock().expect("lock actor");
        actor
            .as_ref()
            .expect("actor is none")
            .try_send_node_msg(msg)?;
        Ok(rx)
        // match self
        //     .actor
        //     .borrow()
        //     .as_ref()
        //     .unwrap()
        //     .node_msg_tx
        //     .try_send(NodeMessage::Membership(request))
        // {
        //     Err(mpsc::TrySendError::Full(_)) => Err(Error::Channel(ChannelError::Full(
        //         "channel no available capacity for memberhsip".to_owned(),
        //     ))),
        //     Err(mpsc::TrySendError::Closed(_)) => Err(Error::Channel(
        //         ChannelError::ReceiverClosed("channel receiver closed for membership".to_owned()),
        //     )),
        //     Ok(_) => Ok(rx),
        // }
    }

    /// `read_index` is use **read_index algorithm** to read data
    /// from a specific group.
    ///
    /// `read_index` is a blocking interface in an asynchronous environment,
    /// and the user should use `.await` to wait for it to complete. If executed
    /// successfully, it returns the associated `context`, and at this point, the
    /// caller can **safely** read data from the state machine. If it fails, an
    /// error is returned.

    /// ## Parameters
    /// - `group_id`: The specific group to read from.
    /// - `context`: The context associated with the read. The context goes through
    /// the context pipeline during the read.
    ///
    /// ## Notes
    /// read_index implements the approach described in the Raft paper where read
    /// requests do not need to be written to the Raft log, avoiding the cost of
    /// writing to disk.
    ///
    /// ## Errors
    /// Most errors require retries. The following error requires a different
    /// handling approach:
    /// - `ProposeError::NotLeader`: The application can refresh the leader and
    /// retry based on the error information using the route table.
    ///
    /// ## Panics
    pub async fn read_index(
        &self,
        group_id: u64,
        context: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, Error> {
        let rx = self.read_index_non_block(group_id, context)?;
        rx.await.map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the read_index change was dropped".to_owned(),
            ))
        })?
    }

    pub fn read_index_block(
        &self,
        group_id: u64,
        context: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, Error> {
        let rx = self.read_index_non_block(group_id, context)?;
        rx.blocking_recv().map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the read_index was dropped".to_owned(),
            ))
        })?
    }

    pub fn read_index_non_block(
        &self,
        group_id: u64,
        context: Option<Vec<u8>>,
    ) -> Result<oneshot::Receiver<Result<Option<Vec<u8>>, Error>>, Error> {
        let (tx, rx) = oneshot::channel();
        let msg = NodeMessage::ReadIndexData(ReadIndexRequest {
            group_id,
            context: ReadIndexContext {
                uuid: Uuid::new_v4().into_bytes(),
                context,
            },
            tx,
        });

        let actor = self.actor.lock().expect("lock actor");
        actor
            .as_ref()
            .expect("actor is none")
            .try_send_node_msg(msg)?;

        Ok(rx)
        // match self.actor.borrow().as_ref().unwrap().node_msg_tx.try_send(
        //     NodeMessage::ReadIndexData(ReadIndexRequest {
        //         group_id,
        //         context: ReadIndexContext {
        //             uuid: Uuid::new_v4().into_bytes(),
        //             context,
        //         },
        //         tx,
        //     }),
        // ) {
        //     Err(mpsc::TrySendError::Full(_)) => Err(Error::Channel(ChannelError::Full(
        //         "channel no available capacity for read_index".to_owned(),
        //     ))),
        //     Err(mpsc::TrySendError::Closed(_)) => Err(Error::Channel(
        //         ChannelError::ReceiverClosed("channel receiver closed for read_index".to_owned()),
        //     )),
        //     Ok(_) => Ok(rx),
        // }
    }

    /// Campaign and wait raft group by given `group_id`.
    ///
    /// `campaign` is synchronous and waits for the campaign to submitted a
    /// result to raft.
    pub async fn campaign_group(&self, group_id: u64) -> Result<(), Error> {
        let rx = self.campaign_group_non_block(group_id);
        rx.await.map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the campaign group change was dropped".to_owned(),
            ))
        })?
    }

    /// Campaign and without wait raft group by given `group_id`.
    ///
    /// `async_campaign` is asynchronous, meaning that without waiting for
    /// the campaign to actually be submitted to raft group.
    /// `tokio::sync::oneshot::Receiver<Result<(), Error>>` is successfully returned
    /// and the user can receive the response submitted by the campaign to raft. if
    /// campaign receiver stop, `Error` is returned.
    pub fn campaign_group_non_block(&self, group_id: u64) -> oneshot::Receiver<Result<(), Error>> {
        let (tx, rx) = oneshot::channel();
        let actor = self.actor.lock().expect("lock actor");
        if let Err(_) =
            actor
                .as_ref()
                .expect("actor is none")
                .try_send_node_msg(NodeMessage::Campaign(MessageWithNotify {
                    msg: group_id,
                    tx,
                }))
        {
            panic!("MultiRaftActor stopped")
        }

        rx
    }

    pub async fn create_group(&self, request: CreateGroupRequest) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let msg = MessageWithNotify { msg: request, tx };
        self.management_request(NodeMessage::CreateGroup(msg))?;
        rx.await.map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the group_manager change was dropped".to_owned(),
            ))
        })?
    }

    pub async fn remove_group(&self, request: RemoveGroupRequest) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let msg = MessageWithNotify { msg: request, tx };
        self.management_request(NodeMessage::RemoveGroup(msg))?;
        rx.await.map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the group_manager change was dropped".to_owned(),
            ))
        })?
    }

    fn management_request(&self, msg: NodeMessage<T::D, T::R>) -> Result<(), Error> {
        let actor = self.actor.lock().expect("lock actor");
        actor
            .as_ref()
            .expect("actor is none")
            .try_send_node_msg(msg)?;

        Ok(())
        // match self
        //     .actor
        //     .borrow()
        //     .as_ref()
        //     .unwrap()
        //     .node_msg_tx
        //     .try_send(msg)
        // {
        //     Err(mpsc::TrySendError::Full(_)) => Err(Error::Channel(ChannelError::Full(
        //         "channel no available capacity for group management".to_owned(),
        //     ))),
        //     Err(mpsc::TrySendError::Closed(_)) => Err(Error::Channel(ChannelError::SenderClosed(
        //         "channel closed for group management".to_owned(),
        //     ))),
        //     Ok(_) => Ok(()),
        // }
    }

    /// Return true if it is can to submit membership change to givend group_id.
    pub async fn can_submmit_membership_change(&self, group_id: u64) -> Result<bool, Error> {
        todo!()
        // let (tx, rx) = oneshot::channel();
        // self.actor
        //     .query_group_tx
        //     .send(QueryGroup::HasPendingConf(group_id, tx))
        //     .unwrap();
        // let res = rx.await.unwrap()?;
        // Ok(!res)
    }

    #[inline]
    pub fn message_sender(&self) -> MultiRaftMessageSenderImpl {
        let actor = self.actor.lock().expect("lock actor");
        MultiRaftMessageSenderImpl {
            tx: actor
                .as_ref()
                .expect("actor is none")
                .clone_peer_msg_sender(),
        }
    }

    #[inline]
    /// Creates a new Receiver connected to event channel Sender.
    /// Note: The Receiver **does not** turn this channel into a broadcast channel.
    pub fn subscribe(&self) -> EventReceiver {
        self.event_bcast.subscribe()
    }

    pub async fn stop(&self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}
