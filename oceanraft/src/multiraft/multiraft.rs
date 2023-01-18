use std::marker::PhantomData;

use raft::prelude::AdminMessage;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use futures::Future;

use raft::Storage;

use crate::util::TaskGroup;

use super::apply_actor;
use super::apply_actor::ApplyActor;
use super::apply_actor::ApplyActorContext;
use super::apply_actor::ApplyActorInner;
use super::apply_actor::ApplyActorSender;
use super::config::Config;
use super::error::Error;
use super::event::Event;
use super::multiraft_actor;
use super::multiraft_actor::MultiRaftActor;
use super::multiraft_actor::MultiRaftActorContext;
use super::multiraft_actor::MultiRaftActorSender;
use super::transport::RaftMessageDispatch;
use super::transport::Transport;

use tracing::error;

use raft_proto::prelude::AppReadIndexRequest;
use raft_proto::prelude::AppWriteRequest;
use raft_proto::prelude::RaftMessage;
use raft_proto::prelude::RaftMessageResponse;

use super::storage::MultiRaftStorage;

pub const NO_GORUP: u64 = 0;
pub const NO_NODE: u64 = 0;

/// Shard multiraft inner state.
#[derive(Clone)]
pub struct Context {
    pub config: Config,
    pub multiraft_actor_ctx: MultiRaftActorContext,
    pub multiraft_actor_tx: MultiRaftActorSender,
    pub apply_actor_ctx: ApplyActorContext,
    pub apply_actor_tx: ApplyActorSender,
}

#[derive(Clone)]
pub struct RaftMessageDispatchImpl(Context);

impl RaftMessageDispatch for RaftMessageDispatchImpl {
    type DispatchFuture<'life0> = impl Future<Output = Result<RaftMessageResponse, Error>> + Send + 'life0
    where
        Self: 'life0;

    fn dispatch<'life0>(&'life0 self, msg: RaftMessage) -> Self::DispatchFuture<'life0> {
        async move {
            if self.0.multiraft_actor_ctx.stopped() {
                return Err(Error::Stop);
            }
            let (tx, rx) = oneshot::channel();
            self.0
                .multiraft_actor_tx
                .raft_message_tx
                .send((msg, tx))
                .await
                .unwrap();
            rx.await.unwrap()
        }
    }
}

/// MultiRaft represents a group of raft replicas
pub struct MultiRaft<T, RS, MRS>
where
    T: Transport,
    RS: Storage + Send + Sync + 'static,
    MRS: MultiRaftStorage<RS>,
{
    ctx: Context,
    multiraft_actor: MultiRaftActor,
    apply_actor: ApplyActor,
    _m2: PhantomData<T>,
    _m3: PhantomData<RS>,
    _m4: PhantomData<MRS>,
}

impl<T, RS, MRS> MultiRaft<T, RS, MRS>
where
    T: Transport,
    RS: Storage + Send + Sync + Clone,
    MRS: MultiRaftStorage<RS>,
{
    /// Create a new multiraft. spawn multiraft actor and apply actor.
    pub fn new(
        config: Config,
        transport: T,
        storage: MRS,
        task_group: TaskGroup,
        event_tx: Sender<Vec<Event>>,
    ) -> Self {
        let (callback_event_tx, callback_event_rx) = channel(1);

        let (apply_actor, apply_actor_tx, apply_actor_rx) = apply_actor::spawn(
            config.clone(),
            event_tx.clone(),
            callback_event_tx,
            task_group.clone(),
        );

        let (multiraft_actor, multiraft_actor_tx) = multiraft_actor::spawn::<T, RS, MRS>(
            &config,
            transport,
            storage,
            event_tx.clone(),
            callback_event_rx,
            apply_actor_tx.clone(),
            apply_actor_rx,
            task_group.clone(),
        );

        Self {
            ctx: Context {
                config,
                multiraft_actor_ctx: multiraft_actor.context(),
                multiraft_actor_tx,
                apply_actor_ctx: apply_actor.context(),
                apply_actor_tx,
            },
            multiraft_actor,
            apply_actor,
            _m2: PhantomData,
            _m3: PhantomData,
            _m4: PhantomData,
        }
    }

    /// Panic. if multi raft actor stopped.
    pub async fn write(&self, request: AppWriteRequest) -> Result<(), Error> {
        let rx = self.async_write(request);
        rx.await.map_err(|_| {
            Error::Internal("the sender that result the write was dropped".to_string())
        })?
    }

    /// Panic. if multi raft actor stopped.
    pub fn async_write(&self, request: AppWriteRequest) -> oneshot::Receiver<Result<(), Error>>  {
        let (tx, rx) = oneshot::channel();
        if let Err(_) = self
            .ctx
            .multiraft_actor_tx
            .write_propose_tx
            .try_send((request, tx))
        {
            panic!("MultiRaftActor stopped")
        }

        rx
    }

    pub async fn read_index(&self, request: AppReadIndexRequest) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        if let Err(_) = self
            .ctx
            .multiraft_actor_tx
            .read_index_propose_tx
            .send((request, tx))
            .await
        {}

        rx.await.unwrap()
    }

    /// Campaign and wait raft group by given `group_id`.
    /// 
    /// `campaign` is synchronous and waits for the campaign to submitted a
    /// result to raft.
    pub async fn campaign(&self, group_id: u64) -> Result<(), Error> {
        let rx = self.async_campaign(group_id)?;
        rx.await.map_err(|_| {
            Error::Internal("the sender that result the campaign was dropped".to_string())
        })?
    }

    /// Campaign and without wait raft group by given `group_id`.
    ///
    /// `async_campaign` is asynchronous, meaning that without waiting for
    /// the campaign to actually be submitted to raft group.
    /// `tokio::sync::oneshot::Receiver<Result<(), Error>>` is successfully returned
    /// and the user can receive the response submitted by the campaign to raft. if
    /// campaign receiver stop, `Error` is returned.
    pub fn async_campaign(
        &self,
        group_id: u64,
    ) -> Result<oneshot::Receiver<Result<(), Error>>, Error> {
        let (tx, rx) = oneshot::channel();
        if let Err(_) = self
            .ctx
            .multiraft_actor_tx
            .campaign_tx
            .try_send((group_id, tx))
        {
            return Err(Error::Internal("campaign receiver dropped".to_string()));
        }

        Ok(rx)
    }

    pub async fn admin(&self, msg: AdminMessage) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        if let Err(_error) = self.ctx.multiraft_actor_tx.admin_tx.send((msg, tx)).await {
            panic!("manager group receiver dropped")
        }

        match rx.await {
            Err(_error) => panic!("sender dopped"),
            Ok(res) => res,
        }
    }

    #[inline]
    pub fn dispatch_impl(&self) -> RaftMessageDispatchImpl {
        RaftMessageDispatchImpl(self.ctx.clone())
    }
}
