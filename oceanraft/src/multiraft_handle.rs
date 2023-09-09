use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::msg::NodeMessage;
use crate::prelude::CreateGroupRequest;
use crate::prelude::MembershipChangeData;
use crate::protos::RemoveGroupRequest;
use crate::utils::mpsc;
use crate::MultiRaftMessageSenderImpl;
use crate::MultiRaftTypeSpecialization;

use super::error::*;
use super::event::EventChannel;
use super::event::EventReceiver;
use super::msg::ManageMessage;
use super::msg::MembershipRequest;
use super::msg::ProposeMessage;
use super::msg::QueryGroup;
use super::msg::ReadIndexContext;
use super::msg::ReadIndexData;
use super::msg::WriteRequest;
use super::node_handle::NodeHandle;
use super::state::GroupStates;
use super::RaftGroupError;

pub struct MultiRaftHandle<T>
where
    T: MultiRaftTypeSpecialization,
{
    node_handle: NodeHandle<T::D, T::R>,
    shared_states: GroupStates,
    event_bcast: EventChannel,
    node_id: u64,
    stopped: Arc<AtomicBool>,
}

impl<T> MultiRaftHandle<T>
where
    T: MultiRaftTypeSpecialization,
{
    fn pre_write_check(&self, group_id: u64) -> Result<(), Error> {
        let state = self.shared_states.get(group_id).map_or(
            Err(Error::RaftGroup(RaftGroupError::Deleted(0, group_id))),
            |state| Ok(state),
        )?;

        // TODO: make configurable: enter following case if don't allow forward to leader propose
        if !state.is_leader() {
            return Err(Error::Propose(super::ProposeError::NotLeader {
                node_id: self.node_id,
                group_id,
                replica_id: state.get_replica_id(),
            }));
        }

        Ok(())
    }

    /// `async_write` the propose data to a specific group in the multiraft system.
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
    pub async fn async_write(
        &self,
        group_id: u64,
        term: u64,
        context: Option<Vec<u8>>,
        propose: T::D,
    ) -> Result<(T::R, Option<Vec<u8>>), Error> {
        let rx = self.write(group_id, term, context, propose)?;
        rx.await.map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the write was dropped".to_owned(),
            ))
        })?
    }

    pub fn blocking_write(
        &self,
        group_id: u64,
        term: u64,
        context: Option<Vec<u8>>,
        data: T::D,
    ) -> Result<(T::R, Option<Vec<u8>>), Error> {
        let rx = self.write(group_id, term, context, data)?;
        rx.blocking_recv().map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the write was dropped".to_owned(),
            ))
        })?
    }

    pub fn write(
        &self,
        group_id: u64,
        term: u64,
        context: Option<Vec<u8>>,
        data: T::D,
    ) -> Result<oneshot::Receiver<Result<(T::R, Option<Vec<u8>>), Error>>, Error> {
        let _ = self.pre_write_check(group_id)?;

        let (tx, rx) = oneshot::channel();
        match self
            .node_handle
            .tx
            .as_ref()
            .unwrap()
            .try_send(NodeMessage::Write(WriteRequest {
                group_id,
                term,
                data,
                context,
                tx,
            })) {
            Err(mpsc::TrySendError::Full(_)) => Err(Error::Channel(ChannelError::Full(
                "channel no avaiable capacity for write".to_owned(),
            ))),
            Err(mpsc::TrySendError::Closed(_)) => Err(Error::Channel(
                ChannelError::ReceiverClosed("channel receiver closed for write".to_owned()),
            )),
            Ok(_) => Ok(rx),
        }
    }

    pub async fn async_membership(
        &self,
        group_id: u64,
        term: Option<u64>,
        context: Option<Vec<u8>>,
        data: MembershipChangeData,
    ) -> Result<(T::R, Option<Vec<u8>>), Error> {
        let rx = self.membership(group_id, term, context, data)?;
        rx.await.map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the membership change was dropped".to_owned(),
            ))
        })?
    }

    pub fn blocking_membership(
        &self,
        group_id: u64,
        term: Option<u64>,
        context: Option<Vec<u8>>,
        data: MembershipChangeData,
    ) -> Result<(T::R, Option<Vec<u8>>), Error> {
        let rx = self.membership(group_id, term, context, data)?;
        rx.blocking_recv().map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the membership change was dropped".to_owned(),
            ))
        })?
    }

    pub fn membership(
        &self,
        group_id: u64,
        term: Option<u64>,
        context: Option<Vec<u8>>,
        data: MembershipChangeData,
    ) -> Result<oneshot::Receiver<Result<(T::R, Option<Vec<u8>>), Error>>, Error> {
        let _ = self.pre_write_check(group_id)?;

        let (tx, rx) = oneshot::channel();

        let request = MembershipRequest {
            group_id,
            term,
            context,
            data,
            tx,
        };

        match self
            .node_handle
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
    pub async fn async_read_index(
        &self,
        group_id: u64,
        context: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, Error> {
        let rx = self.read_index(group_id, context)?;
        rx.await.map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the read_index change was dropped".to_owned(),
            ))
        })?
    }

    pub fn blocking_read_index(
        &self,
        group_id: u64,
        context: Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, Error> {
        let rx = self.read_index(group_id, context)?;
        rx.blocking_recv().map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the read_index was dropped".to_owned(),
            ))
        })?
    }

    pub fn read_index(
        &self,
        group_id: u64,
        context: Option<Vec<u8>>,
    ) -> Result<oneshot::Receiver<Result<Option<Vec<u8>>, Error>>, Error> {
        let (tx, rx) = oneshot::channel();
        match self
            .node_handle
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
    pub async fn async_campaign_group(&self, group_id: u64) -> Result<(), Error> {
        let rx = self.campaign_group(group_id);
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
    pub fn campaign_group(&self, group_id: u64) -> oneshot::Receiver<Result<(), Error>> {
        let (tx, rx) = oneshot::channel();
        if let Err(_) = self.node_handle.campaign_tx.try_send((group_id, tx)) {
            panic!("MultiRaftActor stopped")
        }

        rx
    }

    pub async fn async_create_group(&self, request: CreateGroupRequest) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.management_request(ManageMessage::CreateGroup(request, tx))?;
        rx.await.map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the group_manager change was dropped".to_owned(),
            ))
        })?
    }

    pub async fn async_remove_group(&self, request: RemoveGroupRequest) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        self.management_request(ManageMessage::RemoveGroup(request, tx))?;
        rx.await.map_err(|_| {
            Error::Channel(ChannelError::SenderClosed(
                "the sender that result the group_manager change was dropped".to_owned(),
            ))
        })?
    }

    fn management_request(&self, msg: ManageMessage) -> Result<(), Error> {
        match self.node_handle.manage_tx.try_send(msg) {
            Err(TrySendError::Full(_)) => Err(Error::Channel(ChannelError::Full(
                "channel no available capacity for group management".to_owned(),
            ))),
            Err(TrySendError::Closed(_)) => Err(Error::Channel(ChannelError::SenderClosed(
                "channel closed for group management".to_owned(),
            ))),
            Ok(_) => Ok(()),
        }
    }

    /// Return true if it is can to submit membership change to givend group_id.
    pub async fn can_submmit_membership_change(&self, group_id: u64) -> Result<bool, Error> {
        let (tx, rx) = oneshot::channel();
        self.node_handle
            .query_group_tx
            .send(QueryGroup::HasPendingConf(group_id, tx))
            .unwrap();
        let res = rx.await.unwrap()?;
        Ok(!res)
    }

    #[inline]
    pub fn message_sender(&self) -> MultiRaftMessageSenderImpl {
        MultiRaftMessageSenderImpl {
            tx: self.node_handle.raft_message_tx.clone(),
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
