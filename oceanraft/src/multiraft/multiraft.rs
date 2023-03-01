use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;

use futures::Future;

use raft::Storage;

use crate::util::TaskGroup;

use super::config::Config;
use super::error::ChannelError;
use super::error::Error;
use super::event::Event;
use super::multiraft_actor::MultiRaftActor;
use super::multiraft_actor::ShardState;
use super::multiraft_actor::WriteRequest;
use super::response::AppWriteResponse;
use super::transport::Transport;
use super::util::Ticker;
use super::StateMachine;

use raft_proto::prelude::AppReadIndexRequest;
use raft_proto::prelude::AppWriteRequest;
use raft_proto::prelude::MembershipChangeRequest;
use raft_proto::prelude::MultiRaftMessage;
use raft_proto::prelude::MultiRaftMessageResponse;
use raft_proto::prelude::RaftGroupManagement;

use super::storage::MultiRaftStorage;

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
    shard: ShardState,
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
            if self.shard.stopped() {
                return Err(Error::NodeActor(super::error::NodeActorError::Stopped));
            }
            let (tx, rx) = oneshot::channel();
            let _ = self.tx.send((msg, tx)).await.map_err(|_| {
                Error::Channel(ChannelError::ReceiverClosed(
                    "channel receiver closed for raft message".to_owned(),
                ))
            })?;

            rx.await.map_err(|_| {
                Error::Channel(ChannelError::ReceiverClosed(
                    "channel sender closed for raft message".to_owned(),
                ))
            })?
        }
    }
}

/// MultiRaft represents a group of raft replicas
pub struct MultiRaft<T, RS, MRS, RSM, RES>
where
    T: Transport + Clone,
    RS: Storage + Send + Sync + Clone + 'static,
    MRS: MultiRaftStorage<RS>,
    RSM: StateMachine<RES>,
    RES: AppWriteResponse,
{
    // ctx: Context,
    // cfg: Config,
    // transport: T,
    // storage: MRS,
    task_group: TaskGroup,
    actor: MultiRaftActor<T, RS, MRS, RSM, RES>,
    // apply_actor: ApplyActor,
    // _m2: PhantomData<T>,
    // _m3: PhantomData<RS>,
    // _m4: PhantomData<MRS>,
}

impl<T, RS, MRS, RSM, RES> MultiRaft<T, RS, MRS, RSM, RES>
where
    T: Transport + Clone,
    RS: Storage + Send + Sync + Clone,
    MRS: MultiRaftStorage<RS>,
    RSM: StateMachine<RES>,
    RES: AppWriteResponse,
{
    pub fn new(
        config: Config,
        transport: T,
        storage: MRS,
        rsm: RSM,
        task_group: TaskGroup,
        event_tx: &Sender<Vec<Event>>,
    ) -> Result<Self, Error> {
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

        let actor = MultiRaftActor::<T, RS, MRS, RSM, RES>::new(
            &config, &transport, &storage, rsm, &event_tx,
        );

        Ok(Self {
            // cfg: config,
            // transport,
            // storage,
            task_group,
            actor,
        })
    }

    pub fn start(&self, ticker: Option<Box<dyn Ticker>>) {
        // let (callback_event_tx, callback_event_rx) = channel(1);

        // let (apply_actor, apply_actor_tx, apply_actor_rx) = apply_actor::spawn(
        //     config.clone(),
        //     event_tx.clone(),
        //     callback_event_tx,
        //     task_group.clone(),
        // );

        self.actor.start(&self.task_group, ticker);
        // TODO: start apply and multiraft actor
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
            .try_send(WriteRequest::Write(request, tx))
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
            .try_send(WriteRequest::Membership(request, tx))
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

    pub async fn read_index(&self, request: AppReadIndexRequest) -> Result<(), Error> {
        let rx = self.read_index_non_block(request);
        rx.await.map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the read_index change was dropped".to_owned(),
            ))
        })?
    }

    pub fn read_index_block(&self, request: AppReadIndexRequest) -> Result<(), Error> {
        let rx = self.read_index_non_block(request);
        rx.blocking_recv().map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the read_index change was dropped".to_owned(),
            ))
        })?
    }

    pub fn read_index_non_block(
        &self,
        request: AppReadIndexRequest,
    ) -> oneshot::Receiver<Result<(), Error>> {
        let (tx, rx) = oneshot::channel();
        if let Err(_) = self.actor.read_index_propose_tx.try_send((request, tx)) {
            panic!("MultiRaftActor stopped")
        }

        rx
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
        match self.actor.admin_tx.try_send((request, tx)) {
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
            shard: self.actor.shard.clone(),
            tx: self.actor.raft_message_tx.clone(),
        }
    }
}
