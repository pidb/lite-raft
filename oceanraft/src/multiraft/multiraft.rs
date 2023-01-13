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

    pub async fn write(&self, request: AppWriteRequest) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        if let Err(_) = self
            .ctx
            .multiraft_actor_tx
            .write_propose_tx
            .send((request, tx))
            .await
        {}

        rx.await.unwrap()
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

    pub async fn campagin(&self, group_id: u64) {
        self.ctx
            .multiraft_actor_tx
            .campagin_tx
            .send(group_id)
            .await
            .unwrap()
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
