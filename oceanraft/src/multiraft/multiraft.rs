use std::time::Duration;

use futures::Future;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::time::timeout;
use uuid::Uuid;

use crate::prelude::MembershipChangeData;
use crate::prelude::MultiRaftMessage;
use crate::prelude::MultiRaftMessageResponse;
use crate::prelude::ReplicaDesc;
use crate::util::TaskGroup;

use super::config::Config;
use super::error::ChannelError;
use super::error::Error;
use super::event::EventChannel;
use super::event::EventReceiver;
use super::msg::GroupData;
use super::msg::GroupOp;
use super::msg::ManageMessage;
use super::msg::ProposeMessage;
use super::msg::ReadIndexContext;
use super::msg::ReadIndexData;
use super::msg::WriteData;
use super::node::NodeActor;
use super::response::AppWriteResponse;
use super::state::GroupStates;
use super::storage::MultiRaftStorage;
use super::storage::RaftStorage;
use super::transport::Transport;
use super::util::Ticker;
use super::RaftGroupError;
use super::StateMachine;

pub const NO_GORUP: u64 = 0;
pub const NO_NODE: u64 = 0;
pub const NO_LEADER: u64 = 0;

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
pub struct MultiRaft<RES: AppWriteResponse> {
    node_id: u64,
    task_group: TaskGroup,
    actor: NodeActor<RES>,
    shared_states: GroupStates,
    event_bcast: EventChannel,
}

impl<RES> MultiRaft<RES>
where
    RES: AppWriteResponse,
{
    pub fn new<TR, RS, MRS, RSM, TK>(
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
        RSM: StateMachine<RES>,
        RES: AppWriteResponse,
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
        })
    }

    pub async fn write_timeout(
        &self,
        group_id: u64,
        term: u64,
        data: Vec<u8>,
        context: Option<Vec<u8>>,
        duration: Duration,
    ) -> Result<RES, Error> {
        let rx = self.write_non_block(group_id, term, data, context)?;
        match timeout(duration, rx).await {
            Err(_) => Err(Error::Timeout(
                "wait for the write to complete timeout".to_owned(),
            )),
            Ok(res) => res.map_err(|_| {
                Error::Channel(ChannelError::SenderClosed(
                    "the sender that result the write was dropped".to_owned(),
                ))
            })?,
        }
    }

    pub async fn write(
        &self,
        group_id: u64,
        term: u64,
        data: Vec<u8>,
        context: Option<Vec<u8>>,
    ) -> Result<RES, Error> {
        let rx = self.write_non_block(group_id, term, data, context)?;
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
        data: Vec<u8>,
        context: Option<Vec<u8>>,
    ) -> Result<RES, Error> {
        let rx = self.write_non_block(group_id, term, data, context)?;
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
            return Err(Error::Write(super::WriteError::NotLeader {
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
        data: Vec<u8>,
        context: Option<Vec<u8>>,
    ) -> Result<oneshot::Receiver<Result<RES, Error>>, Error> {
        let _ = self.pre_propose_check(group_id)?;

        let (tx, rx) = oneshot::channel();
        match self
            .actor
            .propose_tx
            .try_send(ProposeMessage::WriteData(WriteData {
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

    pub async fn membership_change_timeout(
        &self,
        request: MembershipChangeData,
        duration: Duration,
    ) -> Result<RES, Error> {
        let rx = self.membership_change_non_block(request)?;
        match timeout(duration, rx).await {
            Err(_) => Err(Error::Timeout(
                "wait for the membership change to complete timeout".to_owned(),
            )),
            Ok(res) => res.map_err(|_| {
                Error::Channel(ChannelError::SenderClosed(
                    "the sender that result the membership change was dropped".to_owned(),
                ))
            })?,
        }
    }

    pub async fn membership_change(&self, request: MembershipChangeData) -> Result<RES, Error> {
        let rx = self.membership_change_non_block(request)?;
        rx.await.map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the membership change was dropped".to_owned(),
            ))
        })?
    }

    pub fn membership_change_block(&self, request: MembershipChangeData) -> Result<RES, Error> {
        let rx = self.membership_change_non_block(request)?;
        rx.blocking_recv().map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the membership change was dropped".to_owned(),
            ))
        })?
    }

    pub fn membership_change_non_block(
        &self,
        request: MembershipChangeData,
    ) -> Result<oneshot::Receiver<Result<RES, Error>>, Error> {
        let _ = self.pre_propose_check(request.group_id)?;

        let (tx, rx) = oneshot::channel();
        match self
            .actor
            .propose_tx
            .try_send(ProposeMessage::MembershipData(request, tx))
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

    pub async fn read_index_timeout(
        &self,
        group_id: u64,
        context: Option<Vec<u8>>,
        duration: Duration,
    ) -> Result<(), Error> {
        let rx = self.read_index_non_block(group_id, context)?;
        match timeout(duration, rx).await {
            Err(_) => Err(Error::Timeout(
                "wait for the read index to complete timeout".to_owned(),
            )),
            Ok(res) => res.map_err(|_| {
                Error::Channel(ChannelError::SenderClosed(
                    "the sender that result the read index was dropped".to_owned(),
                ))
            })?,
        }
    }

    pub async fn read_index(&self, group_id: u64, context: Option<Vec<u8>>) -> Result<(), Error> {
        let rx = self.read_index_non_block(group_id, context)?;
        rx.await.map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the read_index change was dropped".to_owned(),
            ))
        })?
    }

    pub fn read_index_block(&self, group_id: u64, context: Option<Vec<u8>>) -> Result<(), Error> {
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
    ) -> Result<oneshot::Receiver<Result<(), Error>>, Error> {
        let (tx, rx) = oneshot::channel();
        match self
            .actor
            .propose_tx
            .try_send(ProposeMessage::ReadIndexData(ReadIndexData {
                group_id,
                context: ReadIndexContext {
                    uuid: Uuid::new_v4(),
                    context,
                },
                tx: tx,
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
