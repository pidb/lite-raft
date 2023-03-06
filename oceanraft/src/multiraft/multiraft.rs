use std::time::Duration;

use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;

use futures::Future;

use raft::Storage;
use tokio::time::timeout;

use crate::util::TaskGroup;

use raft_proto::prelude::AppReadIndexRequest;
use raft_proto::prelude::AppWriteRequest;
use raft_proto::prelude::MembershipChangeRequest;
use raft_proto::prelude::MultiRaftMessage;
use raft_proto::prelude::MultiRaftMessageResponse;
use raft_proto::prelude::RaftGroupManagement;

use super::config::Config;
use super::error::ChannelError;
use super::error::Error;
use super::event::EventChannel;
use super::event::EventReceiver;
use super::msg::AdminMessage;
use super::msg::WriteMessage;
use super::node::NodeActor;
use super::response::AppWriteResponse;
use super::storage::MultiRaftStorage;
use super::transport::Transport;
use super::util::Ticker;
use super::StateMachine;

pub const NO_GORUP: u64 = 0;
pub const NO_NODE: u64 = 0;

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
    // ctx: Context,
    // cfg: Config,
    // transport: T,
    // storage: MRS,
    task_group: TaskGroup,
    actor: NodeActor<RES>,
    // apply_actor: ApplyActor,
    // _m2: PhantomData<T>,
    // _m3: PhantomData<RS>,
    // _m4: PhantomData<MRS>,
    event_bcast: EventChannel,
}

impl<RES> MultiRaft<RES>
where
    RES: AppWriteResponse,
{
    pub fn new<TR, RS, MRS, RSM, TK>(
        config: Config,
        transport: TR,
        storage: MRS,
        rsm: RSM,
        task_group: TaskGroup,
        // event_tx: &Sender<Vec<Event>>,
        ticker: Option<TK>,
    ) -> Result<Self, Error>
    where
        TR: Transport + Clone,
        RS: Storage + Send + Sync + Clone + 'static,
        MRS: MultiRaftStorage<RS>,
        RSM: StateMachine<RES>,
        RES: AppWriteResponse,
        TK: Ticker,
    {
        config.validate()?;
        // let (callback_event_tx, callback_event_rx) = channel(1);

        // let (apply_actor, apply_actor_tx, apply_actor_rx) = apply_actor::spawn(
        //     config.clone(),
        //     event_tx.clone(),
        //     callback_event_tx,
        //     task_group.clone(),
        // );

        // let multiraft_actor = MultiRaftActor::<T, RS, MRS>::new(
        //     &config,
        //     transport,
        //     storage,
        //     event_tx.clone(),
        //     callback_event_rx,
        //     apply_actor_tx.clone(),
        //     apply_actor_rx,
        //     task_group.clone(),
        // );

        // TODO: provide capactiy
        let event_bcast = EventChannel::new(config.event_capacity);
        let actor = NodeActor::spawn(
            &config,
            &transport,
            &storage,
            rsm,
            // event_tx,
            &event_bcast,
            &task_group,
            ticker,
        );

        // let actor = MultiRaftActor::<TR, RS, MRS, RSM, RES>::new(
        //     &config, &transport, &storage, rsm, &event_tx,
        // );

        Ok(Self {
            // cfg: config,
            // transport,
            // storage,
            event_bcast,
            actor,
            task_group,
        })
    }

    // pub fn start<T>(&self, ticker: Option<T>)
    // where
    //     T: Ticker,
    // {
    //     // let (callback_event_tx, callback_event_rx) = channel(1);

    //     // let (apply_actor, apply_actor_tx, apply_actor_rx) = apply_actor::spawn(
    //     //     config.clone(),
    //     //     event_tx.clone(),
    //     //     callback_event_tx,
    //     //     task_group.clone(),
    //     // );

    //     self.actor.start(&self.task_group, ticker);
    //     // TODO: start apply and multiraft actor
    // }

    pub async fn write_timeout(
        &self,
        request: AppWriteRequest,
        duration: Duration,
    ) -> Result<RES, Error> {
        let rx = self.write_non_block(request)?;
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

    pub async fn write(&self, request: AppWriteRequest) -> Result<RES, Error> {
        let rx = self.write_non_block(request)?;
        rx.await.map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the write was dropped".to_owned(),
            ))
        })?
    }

    pub fn write_block(&self, request: AppWriteRequest) -> Result<RES, Error> {
        let rx = self.write_non_block(request)?;
        rx.blocking_recv().map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the write was dropped".to_owned(),
            ))
        })?
    }

    pub fn write_non_block(
        &self,
        request: AppWriteRequest,
    ) -> Result<oneshot::Receiver<Result<RES, Error>>, Error> {
        let (tx, rx) = oneshot::channel();
        match self
            .actor
            .write_propose_tx
            .try_send(WriteMessage::Write(request, tx))
        {
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
        request: MembershipChangeRequest,
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

    pub async fn membership_change(&self, request: MembershipChangeRequest) -> Result<RES, Error> {
        let rx = self.membership_change_non_block(request)?;
        rx.await.map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the membership change was dropped".to_owned(),
            ))
        })?
    }

    pub fn membership_change_block(&self, request: MembershipChangeRequest) -> Result<RES, Error> {
        let rx = self.membership_change_non_block(request)?;
        rx.blocking_recv().map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the membership change was dropped".to_owned(),
            ))
        })?
    }

    pub fn membership_change_non_block(
        &self,
        request: MembershipChangeRequest,
    ) -> Result<oneshot::Receiver<Result<RES, Error>>, Error> {
        let (tx, rx) = oneshot::channel();
        match self
            .actor
            .write_propose_tx
            .try_send(WriteMessage::Membership(request, tx))
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
        request: AppReadIndexRequest,
        duration: Duration,
    ) -> Result<(), Error> {
        let rx = self.read_index_non_block(request)?;
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

    pub async fn read_index(&self, request: AppReadIndexRequest) -> Result<(), Error> {
        let rx = self.read_index_non_block(request)?;
        rx.await.map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the read_index change was dropped".to_owned(),
            ))
        })?
    }

    pub fn read_index_block(&self, request: AppReadIndexRequest) -> Result<(), Error> {
        let rx = self.read_index_non_block(request)?;
        rx.blocking_recv().map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the read_index was dropped".to_owned(),
            ))
        })?
    }

    pub fn read_index_non_block(
        &self,
        request: AppReadIndexRequest,
    ) -> Result<oneshot::Receiver<Result<(), Error>>, Error> {
        let (tx, rx) = oneshot::channel();
        match self.actor.read_index_propose_tx.try_send((request, tx)) {
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

    pub async fn group_manage(&self, request: RaftGroupManagement) -> Result<(), Error> {
        let rx = self.group_manage_non_block(request)?;
        rx.await.map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the group_manager change was dropped".to_owned(),
            ))
        })?
    }

    pub fn group_manage_block(&self, request: RaftGroupManagement) -> Result<(), Error> {
        let rx = self.group_manage_non_block(request)?;
        rx.blocking_recv().map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the group_manager change was dropped".to_owned(),
            ))
        })?
    }

    pub fn group_manage_non_block(
        &self,
        request: RaftGroupManagement,
    ) -> Result<oneshot::Receiver<Result<(), Error>>, Error> {
        let (tx, rx) = oneshot::channel();
        match self
            .actor
            .admin_tx
            .try_send(AdminMessage::Group(request, tx))
        {
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
}
