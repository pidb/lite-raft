use std::marker::PhantomData;

use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;

use super::config::MultiRaftConfig;
use super::error::Error;
use super::event::Event;
use super::multiraft_actor::MultiRaftActor;
use super::multiraft_actor::MultiRaftActorAddress;
use super::transport::MessageInterface;
use super::transport::Transport;

use crate::proto::AppReadIndexRequest;
use crate::proto::AppWriteRequest;
use crate::proto::RaftGroupManagementMessage;
use crate::proto::RaftGroupManagementMessageType;

use crate::storage::MultiRaftStorage;
use crate::storage::RaftStorage;

pub const NO_GORUP: u64 = 0;
pub const NO_NODE: u64 = 0;

/// MultiRaft represents a group of raft replicas
pub struct MultiRaft<MI, T, RS, MRS>
where
    MI: MessageInterface,
    T: Transport<MI>,
    RS: RaftStorage,
    MRS: MultiRaftStorage<RS>,
{
    store_id: u64,
    config: MultiRaftConfig,
    actor_address: MultiRaftActorAddress,
    actor_join_handle: JoinHandle<()>,
    _m1: PhantomData<MI>,
    _m2: PhantomData<T>,
    _m3: PhantomData<RS>,
    _m4: PhantomData<MRS>,
}

impl<MI, T, RS, MRS> MultiRaft<MI, T, RS, MRS>
where
    MI: MessageInterface,
    T: Transport<MI>,
    RS: RaftStorage,
    MRS: MultiRaftStorage<RS>,
{
    pub fn new(
        config: MultiRaftConfig,
        node_id: u64,
        store_id: u64,
        transport: T,
        storage: MRS,
        stop: watch::Receiver<bool>,
        event_tx: Sender<Vec<Event>>,
    ) -> Self {
        let (actor_join_handle, actor_address) =
            MultiRaftActor::spawn(&config, node_id, store_id, transport, event_tx.clone(), storage, stop);
        Self {
            store_id,
            config,
            actor_address,
            actor_join_handle,
            _m1: PhantomData,
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

    pub async fn initial_raft_group(&self, msg: RaftGroupManagementMessage) -> Result<(), Error> {
        assert_eq!(
            msg.msg_type(),
            RaftGroupManagementMessageType::MsgInitialGroup
        );
        let (tx, rx) = oneshot::channel();
        if let Err(_error) = self.actor_address.manager_group_tx.send((msg, tx)).await {
            panic!("manager group receiver dropped")
        }

        match rx.await {
            Err(_error) => panic!("sender dopped"),
            Ok(res) => res,
        }
    }

    /// Bootstrap a new raft consensus group.
    pub async fn bootstrap_raft_group(&self, group_id: u64, replica_id: u64) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let mut msg = RaftGroupManagementMessage::default();
        msg.group_id = group_id;
        msg.replica_id = replica_id;
        msg.set_msg_type(RaftGroupManagementMessageType::MsgCreateGroup);

        if let Err(_error) = self.actor_address.manager_group_tx.send((msg, tx)).await {
            panic!("manager group receiver dropped")
        }

        match rx.await {
            Err(_error) => panic!("sender dopped"),
            Ok(res) => res,
        }
    }
}
