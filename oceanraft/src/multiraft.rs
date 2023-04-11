use std::fmt::Debug;
use std::marker::PhantomData;

use futures::Future;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::prelude::MembershipChangeData;
use crate::prelude::MultiRaftMessage;
use crate::prelude::MultiRaftMessageResponse;
use crate::prelude::ReplicaDesc;
use crate::task_group::TaskGroup;

use super::config::Config;
use super::error::ChannelError;
use super::error::Error;
use super::event::EventChannel;
use super::event::EventReceiver;
use super::msg::GroupData;
use super::msg::GroupOp;
use super::msg::ManageMessage;
use super::msg::MembershipRequest;
use super::msg::ProposeMessage;
use super::msg::QueryGroup;
use super::msg::ReadIndexContext;
use super::msg::ReadIndexData;
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
pub trait ProposeData:
    Debug + Clone + Send + Sync + Serialize + for<'d> Deserialize<'d> + 'static
{
}

impl<T> ProposeData for T where
    T: Debug + Clone + Send + Sync + Serialize + for<'d> Deserialize<'d> + 'static
{
}

/// Propose and membership change requests can be responded with custom types
/// for which `ProposePropose` provides trait constraints.
pub trait ProposeResponse: Debug + Clone + Send + Sync + 'static {}

impl<R> ProposeResponse for R where R: Debug + Clone + Send + Sync + 'static {}

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
    tx: Sender<(
        MultiRaftMessage,
        oneshot::Sender<Result<MultiRaftMessageResponse, Error>>,
    )>,
}

impl MultiRaftMessageSender for MultiRaftMessageSenderImpl {
    type SendFuture<'life0> = impl Future<Output = Result<MultiRaftMessageResponse, Error>> + Send + 'life0
    where
        Self: 'life0;

    fn send<'life0>(&'life0 self, msg: MultiRaftMessage) -> Self::SendFuture<'life0> {
        async move {
            let (tx, rx) = oneshot::channel();
            match self.tx.try_send((msg, tx)) {
                Err(TrySendError::Closed(_)) => Err(Error::Channel(ChannelError::ReceiverClosed(
                    "channel receiver closed for raft message".to_owned(),
                ))),
                Err(TrySendError::Full(_)) => Err(Error::Channel(ChannelError::Full(
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
pub struct MultiRaft<PD, RES, RSM>
where
    PD: ProposeData,
    RES: ProposeResponse,
{
    node_id: u64,
    task_group: TaskGroup,
    actor: NodeActor<PD, RES>,
    shared_states: GroupStates,
    event_bcast: EventChannel,
    _m1: PhantomData<RSM>,
}

impl<PD, RES, RSM> MultiRaft<PD, RES, RSM>
where
    PD: ProposeData,
    RES: ProposeResponse,
    RSM: StateMachine<PD, RES>,
{
    pub fn new<TR, RS, MRS, TK>(
        cfg: Config,
        transport: TR,
        storage: MRS,
        rsm: RSM,
        task_group: TaskGroup,
        ticker: Option<TK>,
    ) -> Result<Self, Error>
    where
        TR: Transport + Clone,
        RS: RaftStorage,
        MRS: MultiRaftStorage<RS>,
        RSM: StateMachine<PD, RES>,
        TK: Ticker,
    {
        cfg.validate()?;
        let states = GroupStates::new();
        let event_bcast = EventChannel::new(cfg.event_capacity);
        let actor = NodeActor::spawn(
            &cfg,
            &transport,
            &storage,
            rsm,
            &event_bcast,
            &task_group,
            ticker,
            states.clone(),
        );

        Ok(Self {
            node_id: cfg.node_id,
            event_bcast,
            actor,
            task_group,
            shared_states: states,
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
        propose: PD,
    ) -> Result<(RES, Option<Vec<u8>>), Error> {
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
        data: PD,
    ) -> Result<(RES, Option<Vec<u8>>), Error> {
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
        data: PD,
    ) -> Result<oneshot::Receiver<Result<(RES, Option<Vec<u8>>), Error>>, Error> {
        let _ = self.pre_propose_check(group_id)?;

        let (tx, rx) = oneshot::channel();
        match self
            .actor
            .propose_tx
            .try_send(ProposeMessage::Write(WriteRequest {
                group_id,
                term,
                data,
                context,
                tx,
            })) {
            Err(TrySendError::Full(_)) => Err(Error::Channel(ChannelError::Full(
                "channel no avaiable capacity for write".to_owned(),
            ))),
            Err(TrySendError::Closed(_)) => Err(Error::Channel(ChannelError::ReceiverClosed(
                "channel receiver closed for write".to_owned(),
            ))),
            Ok(_) => Ok(rx),
        }
    }

    pub async fn membership(
        &self,
        group_id: u64,
        term: Option<u64>,
        context: Option<Vec<u8>>,
        data: MembershipChangeData,
    ) -> Result<(RES, Option<Vec<u8>>), Error> {
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
    ) -> Result<(RES, Option<Vec<u8>>), Error> {
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
    ) -> Result<oneshot::Receiver<Result<(RES, Option<Vec<u8>>), Error>>, Error> {
        let _ = self.pre_propose_check(group_id)?;

        let (tx, rx) = oneshot::channel();

        let request = MembershipRequest {
            group_id,
            term,
            context,
            data,
            tx,
        };

        match self
            .actor
            .propose_tx
            .try_send(ProposeMessage::Membership(request))
        {
            Err(TrySendError::Full(_)) => Err(Error::Channel(ChannelError::Full(
                "channel no available capacity for memberhsip".to_owned(),
            ))),
            Err(TrySendError::Closed(_)) => Err(Error::Channel(ChannelError::ReceiverClosed(
                "channel receiver closed for membership".to_owned(),
            ))),
            Ok(_) => Ok(rx),
        }
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
        match self
            .actor
            .propose_tx
            .try_send(ProposeMessage::ReadIndexData(ReadIndexData {
                group_id,
                context: ReadIndexContext {
                    uuid: Uuid::new_v4().into_bytes(),
                    context,
                },
                tx,
            })) {
            Err(TrySendError::Full(_)) => Err(Error::Channel(ChannelError::Full(
                "channel no available capacity for read_index".to_owned(),
            ))),
            Err(TrySendError::Closed(_)) => Err(Error::Channel(ChannelError::ReceiverClosed(
                "channel receiver closed for read_index".to_owned(),
            ))),
            Ok(_) => Ok(rx),
        }
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
        if let Err(_) = self.actor.campaign_tx.try_send((group_id, tx)) {
            panic!("MultiRaftActor stopped")
        }

        rx
    }

    pub async fn create_group(
        &self,
        group_id: u64,
        replica_id: u64,
        replicas: Option<Vec<ReplicaDesc>>,
    ) -> Result<(), Error> {
        let rx = self.group_operation(group_id, replica_id, replicas, GroupOp::Create)?;
        rx.await.map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the group_manager change was dropped".to_owned(),
            ))
        })?
    }

    pub fn create_group_block(
        &self,
        group_id: u64,
        replica_id: u64,
        replicas: Option<Vec<ReplicaDesc>>,
    ) -> Result<(), Error> {
        let rx = self.group_operation(group_id, replica_id, replicas, GroupOp::Create)?;
        rx.blocking_recv().map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the group_manager change was dropped".to_owned(),
            ))
        })?
    }

    #[inline]
    pub fn create_group_non_block(
        &self,
        group_id: u64,
        replica_id: u64,
        replicas: Option<Vec<ReplicaDesc>>,
    ) -> Result<oneshot::Receiver<Result<(), Error>>, Error> {
        self.group_operation(group_id, replica_id, replicas, GroupOp::Create)
    }

    pub async fn remove_group(
        &self,
        group_id: u64,
        replica_id: u64,
        replicas: Option<Vec<ReplicaDesc>>,
    ) -> Result<(), Error> {
        let rx = self.group_operation(group_id, replica_id, replicas, GroupOp::Remove)?;
        rx.await.map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the group_manager change was dropped".to_owned(),
            ))
        })?
    }

    pub fn remove_group_block(
        &self,
        group_id: u64,
        replica_id: u64,
        replicas: Option<Vec<ReplicaDesc>>,
    ) -> Result<(), Error> {
        let rx = self.group_operation(group_id, replica_id, replicas, GroupOp::Remove)?;
        rx.blocking_recv().map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the group_manager change was dropped".to_owned(),
            ))
        })?
    }

    #[inline]
    pub fn remove_group_non_block(
        &self,
        group_id: u64,
        replica_id: u64,
        replicas: Option<Vec<ReplicaDesc>>,
    ) -> Result<oneshot::Receiver<Result<(), Error>>, Error> {
        self.group_operation(group_id, replica_id, replicas, GroupOp::Remove)
    }

    fn group_operation(
        &self,
        group_id: u64,
        replica_id: u64,
        replicas: Option<Vec<ReplicaDesc>>,
        op: GroupOp,
    ) -> Result<oneshot::Receiver<Result<(), Error>>, Error> {
        let (tx, rx) = oneshot::channel();
        match self
            .actor
            .manage_tx
            .try_send(ManageMessage::GroupData(GroupData {
                group_id,
                replica_id,
                replicas,
                op,
                tx,
            })) {
            Err(TrySendError::Full(_)) => Err(Error::Channel(ChannelError::Full(
                "channel no available capacity for group management".to_owned(),
            ))),
            Err(TrySendError::Closed(_)) => Err(Error::Channel(ChannelError::SenderClosed(
                "channel closed for group management".to_owned(),
            ))),
            Ok(_) => Ok(rx),
        }
    }

    /// Return true if it is can to submit membership change to givend group_id.
    pub async fn can_submmit_membership_change(&self, group_id: u64) -> Result<bool, Error> {
        let (tx, rx) = oneshot::channel();
        self.actor
            .query_group_tx
            .send(QueryGroup::HasPendingConf(group_id, tx))
            .unwrap();
        let res = rx.await.unwrap()?;
        Ok(!res)
    }

    #[inline]
    pub fn message_sender(&self) -> MultiRaftMessageSenderImpl {
        MultiRaftMessageSenderImpl {
            tx: self.actor.raft_message_tx.clone(),
        }
    }

    #[inline]
    /// Creates a new Receiver connected to event channel Sender.
    /// Note: The Receiver **does not** turn this channel into a broadcast channel.
    pub fn subscribe(&self) -> EventReceiver {
        self.event_bcast.subscribe()
    }

    pub async fn stop(&self) {
        self.task_group.stop();
        self.task_group.joinner().await;
    }
}
