use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;

use futures::Future;

use raft::Storage;

use crate::util::TaskGroup;

use super::config::Config;
use super::error::Error;
use super::event::Event;
use super::multiraft_actor::MultiRaftActor;
use super::multiraft_actor::ShardState;
use super::transport::RaftMessageDispatch;
use super::transport::Transport;

use raft_proto::prelude::AdminMessage;
use raft_proto::prelude::AppReadIndexRequest;
use raft_proto::prelude::AppWriteRequest;
use raft_proto::prelude::RaftMessage;
use raft_proto::prelude::RaftMessageResponse;
use raft_proto::prelude::MembershipChangeRequest;



use super::storage::MultiRaftStorage;

pub const NO_GORUP: u64 = 0;
pub const NO_NODE: u64 = 0;

#[derive(Clone)]
pub struct RaftMessageDispatchImpl {
    shard: ShardState,
    tx: Sender<(
        RaftMessage,
        oneshot::Sender<Result<RaftMessageResponse, Error>>,
    )>,
}

impl RaftMessageDispatch for RaftMessageDispatchImpl {
    type DispatchFuture<'life0> = impl Future<Output = Result<RaftMessageResponse, Error>> + Send + 'life0
    where
        Self: 'life0;

    fn dispatch<'life0>(&'life0 self, msg: RaftMessage) -> Self::DispatchFuture<'life0> {
        async move {
            if self.shard.stopped() {
                return Err(Error::Stop);
            }
            let (tx, rx) = oneshot::channel();
            self.tx.send((msg, tx)).await.unwrap();
            rx.await.unwrap()
        }
    }
}

/// MultiRaft represents a group of raft replicas
pub struct MultiRaft<T, RS, MRS>
where
    T: Transport + Clone,
    RS: Storage + Send + Sync + Clone + 'static,
    MRS: MultiRaftStorage<RS>,
{
    // ctx: Context,
    cfg: Config,
    transport: T,
    storage: MRS,
    task_group: TaskGroup,
    actor: MultiRaftActor<T, RS, MRS>,
    // apply_actor: ApplyActor,
    // _m2: PhantomData<T>,
    // _m3: PhantomData<RS>,
    // _m4: PhantomData<MRS>,
}

impl<T, RS, MRS> MultiRaft<T, RS, MRS>
where
    T: Transport + Clone,
    RS: Storage + Send + Sync + Clone,
    MRS: MultiRaftStorage<RS>,
{
    /// Create a new multiraft. spawn multiraft actor and apply actor.
    pub fn new(
        config: Config,
        transport: T,
        storage: MRS,
        task_group: TaskGroup,
        event_tx: &Sender<Vec<Event>>,
    ) -> Self {
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

        let actor = MultiRaftActor::<T, RS, MRS>::new(&config, &transport, &storage, &event_tx);

        Self {
            cfg: config,
            transport,
            storage,
            task_group,
            actor,
        }
    }

    pub fn start(&self) {
        // let (callback_event_tx, callback_event_rx) = channel(1);

        // let (apply_actor, apply_actor_tx, apply_actor_rx) = apply_actor::spawn(
        //     config.clone(),
        //     event_tx.clone(),
        //     callback_event_tx,
        //     task_group.clone(),
        // );

        self.actor.start(&self.task_group);
        // TODO: start apply and multiraft actor
    }

    /// Panic. if multi raft actor stopped.
    pub async fn write(&self, request: AppWriteRequest) -> Result<(), Error> {
        let rx = self.async_write(request);
        rx.await.map_err(|_| {
            Error::Internal("the sender that result the write was dropped".to_string())
        })?
    }

    pub fn async_write(&self, request: AppWriteRequest) -> oneshot::Receiver<Result<(), Error>> {
        let (tx, rx) = oneshot::channel();
        if let Err(_) = self.actor.write_propose_tx.try_send((request, tx)) {
            panic!("MultiRaftActor stopped")
        }

        rx
    }

    pub async fn read_index(&self, request: AppReadIndexRequest) -> Result<(), Error> {
        let rx = self.async_read_index(request);
        rx.await.map_err(|_| {
            Error::Internal("the sender that result the read_index was dropped".to_string())
        })?
    }

    pub fn async_read_index(
        &self,
        request: AppReadIndexRequest,
    ) -> oneshot::Receiver<Result<(), Error>> {
        let (tx, rx) = oneshot::channel();
        if let Err(_) = self.actor.read_index_propose_tx.try_send((request, tx)) {
            panic!("MultiRaftActor stopped")
        }

        rx
    }

    pub async fn membership_change(&self, request: MembershipChangeRequest) -> Result<(), Error> {
        let rx = self.async_membership_change(request);
        rx.await.map_err(|_| {
            Error::Internal("the sender that result the read_index was dropped".to_string())
        })?
    }

    pub fn async_membership_change(
        &self,
        request: MembershipChangeRequest,
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
        let rx = self.async_campaign_group(group_id);
        rx.await.map_err(|_| {
            Error::Internal("the sender that result the campaign was dropped".to_string())
        })?
    }

    pub async fn admin(&self, msg: AdminMessage) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        if let Err(_error) = self.actor.admin_tx.send((msg, tx)).await {
            panic!("manager group receiver dropped")
        }

        match rx.await {
            Err(_error) => panic!("sender dopped"),
            Ok(res) => res,
        }
    }

    /// Campaign and without wait raft group by given `group_id`.
    ///
    /// `async_campaign` is asynchronous, meaning that without waiting for
    /// the campaign to actually be submitted to raft group.
    /// `tokio::sync::oneshot::Receiver<Result<(), Error>>` is successfully returned
    /// and the user can receive the response submitted by the campaign to raft. if
    /// campaign receiver stop, `Error` is returned.
    pub fn async_campaign_group(&self, group_id: u64) -> oneshot::Receiver<Result<(), Error>> {
        let (tx, rx) = oneshot::channel();
        if let Err(_) = self.actor.campaign_tx.try_send((group_id, tx)) {
            panic!("MultiRaftActor stopped")
        }

        rx
    }

    #[inline]
    pub fn dispatch_impl(&self) -> RaftMessageDispatchImpl {
        RaftMessageDispatchImpl {
            shard: self.actor.shard.clone(),
            tx: self.actor.raft_message_tx.clone(),
        }
    }
}
