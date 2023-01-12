use std::marker::PhantomData;

use raft::prelude::AdminMessage;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use futures::Future;

use raft::Storage;

use crate::util::TaskGroup;

use super::apply::ApplyActor;
use super::config::Config;
use super::error::Error;
use super::event::Event;
use super::multiraft_actor::MultiRaftActor;
use super::multiraft_actor::MultiRaftActorAddress;
use super::transport::RaftMessageDispatch;
use super::transport::Transport;

use raft_proto::prelude::AppReadIndexRequest;
use raft_proto::prelude::AppWriteRequest;
use raft_proto::prelude::RaftMessage;
use raft_proto::prelude::RaftMessageResponse;

use super::storage::MultiRaftStorage;

pub const NO_GORUP: u64 = 0;
pub const NO_NODE: u64 = 0;

#[derive(Clone)]
pub struct RaftMessageDispatchImpl {
    actor_address: MultiRaftActorAddress,
}

impl RaftMessageDispatch for RaftMessageDispatchImpl {
    type DispatchFuture<'life0> = impl Future<Output = Result<RaftMessageResponse, Error>> + Send + 'life0
    where
        Self: 'life0;

    fn dispatch<'life0>(&'life0 self, msg: RaftMessage) -> Self::DispatchFuture<'life0> {
        async move {
            let (tx, rx) = oneshot::channel();
            self.actor_address
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
    config: Config,
    node_id: u64,
    actor_address: MultiRaftActorAddress,
    apply_join_handle: JoinHandle<()>,
    actor_join_handle: JoinHandle<()>,
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

        let (apply_join_handle, apply_actor_address) = ApplyActor::spawn(
            config.clone(),
            event_tx.clone(),
            callback_event_tx,
            task_group.clone(),
        );

        let (actor_join_handle, actor_address) = MultiRaftActor::<T, RS, MRS>::spawn(
            &config,
            transport,
            storage,
            event_tx.clone(),
            callback_event_rx,
            apply_actor_address,
            task_group.clone(),
        );

        Self {
            node_id: config.node_id,
            config,
            apply_join_handle,
            actor_address,
            actor_join_handle,
            _m2: PhantomData,
            _m3: PhantomData,
            _m4: PhantomData,
        }
    }

    pub async fn write(&self, request: AppWriteRequest) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        if let Err(_) = self
            .actor_address
            .write_propose_tx
            .send((request, tx))
            .await
        {}

        rx.await.unwrap()
    }

    pub async fn read_index(&self, request: AppReadIndexRequest) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        if let Err(_) = self
            .actor_address
            .read_index_propose_tx
            .send((request, tx))
            .await
        {}

        rx.await.unwrap()
    }

    pub async fn campagin(&self, group_id: u64) {
        self.actor_address.campagin_tx.send(group_id).await.unwrap()
    }

    pub async fn admin(&self, msg: AdminMessage) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        if let Err(_error) = self.actor_address.admin_tx.send((msg, tx)).await {
            panic!("manager group receiver dropped")
        }

        match rx.await {
            Err(_error) => panic!("sender dopped"),
            Ok(res) => res,
        }
    }

    #[inline]
    pub fn dispatch_impl(&self) -> RaftMessageDispatchImpl {
        RaftMessageDispatchImpl { actor_address: self.actor_address.clone() }
    }
}
