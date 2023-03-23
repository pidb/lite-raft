extern crate raft_proto;

use futures::Future;
use prost::Message;
use raft_proto::ConfChangeI;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

use crate::multiraft::error::ChannelError;
use crate::multiraft::WriteResponse;
use crate::prelude::ConfChange;
use crate::prelude::ConfChangeV2;
use crate::prelude::Entry;
use crate::prelude::EntryType;
use crate::prelude::MembershipChangeData;

use super::error::Error;
use super::msg::ApplyCommitMessage;
use super::msg::CommitMembership;
use super::GroupState;
use super::WriteData;

#[derive(Debug)]
pub struct ApplyNoOp {
    pub group_id: u64,
    pub entry_index: u64,
    pub entry_term: u64,
}

#[derive(Debug)]
pub struct ApplyNormal<W, R>
where
    W: WriteData,
    R: WriteResponse,
{
    pub group_id: u64,
    // pub entry: Entry,
    pub index: u64,
    pub term: u64,
    pub data: W,
    pub context: Option<Vec<u8>>,
    pub is_conf_change: bool,
    pub tx: Option<oneshot::Sender<Result<R, Error>>>,
}

#[derive(Debug)]
pub struct ApplyMembership<RES: WriteResponse> {
    pub group_id: u64,
    pub index: u64,
    pub term: u64,
    pub conf_change: ConfChangeV2,
    pub change_data: MembershipChangeData,
    // pub entry: Entry,
    pub tx: Option<oneshot::Sender<Result<RES, Error>>>,
    pub commit_tx: UnboundedSender<ApplyCommitMessage>,
}

impl<RES: WriteResponse> ApplyMembership<RES> {
    /// Consumption entry is parse to `ApplyMembership`.
    pub fn parse(
        group_id: u64,
        entry: Entry,
        tx: Option<oneshot::Sender<Result<RES, Error>>>,
        commit_tx: UnboundedSender<ApplyCommitMessage>,
    ) -> Self {
        let (index, term) = (entry.index, entry.term);
        let (conf_change, change_data) = match entry.entry_type() {
            EntryType::EntryNormal => unreachable!(),
            EntryType::EntryConfChange => {
                let mut conf_change = ConfChange::default();
                conf_change.merge(entry.data.as_ref()).unwrap();

                let mut change_data = MembershipChangeData::default();
                change_data.merge(entry.context.as_ref()).unwrap();

                (conf_change.into_v2(), change_data)
            }
            EntryType::EntryConfChangeV2 => {
                let mut conf_change = ConfChangeV2::default();
                conf_change.merge(entry.data.as_ref()).unwrap();

                let mut change_data = MembershipChangeData::default();
                change_data.merge(entry.context.as_ref()).unwrap();

                (conf_change, change_data)
            }
        };

        Self {
            group_id,
            index,
            term,
            conf_change,
            change_data,
            tx,
            commit_tx,
        }
    }

    /// Commit membership change to multiraft.
    ///
    /// Note: it's must be called because multiraft need apply membersip change to raft.
    pub async fn done(&self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let commit = CommitMembership {
            entry_index: self.index,
            conf_change: self.conf_change.clone(),
            change_request: self.change_data.clone(),
        };

        match self
            .commit_tx
            .send(ApplyCommitMessage::Membership((commit, tx)))
        {
            Ok(_) => {}
            Err(_) => {
                return Err(Error::Channel(ChannelError::ReceiverClosed(
                    "node actor dropped".to_owned(),
                )));
            }
        }

        if let Err(_) = rx.await {
            return Err(Error::Channel(ChannelError::SenderClosed(
                "node actor dropped".to_owned(),
            )));
        }

        Ok(())
    }
}

#[derive(Debug)]
pub enum Apply<W, R>
where
    W: WriteData,
    R: WriteResponse,
{
    NoOp(ApplyNoOp),
    Normal(ApplyNormal<W, R>),
    Membership(ApplyMembership<R>),
}

impl<W, R> Apply<W, R>
where
    W: WriteData,
    R: WriteResponse,
{
    pub(crate) fn get_index(&self) -> u64 {
        match self {
            Self::NoOp(noop) => noop.entry_index,
            Self::Normal(normal) => normal.index,
            Self::Membership(membership) => membership.index,
        }
    }

    #[allow(unused)]
    pub(crate) fn get_term(&self) -> u64 {
        match self {
            Self::NoOp(noop) => noop.entry_term,
            Self::Normal(normal) => normal.term,
            Self::Membership(membership) => membership.term,
        }
    }

    #[allow(unused)]
    pub(crate) fn get_data(&self) -> W {
        match self {
            Self::NoOp(noop) => W::default(),
            Self::Normal(normal) => normal.data.clone(),
            Self::Membership(membership) => unimplemented!(),
        }
    }
}

pub trait StateMachine<W, R>: Send + Sync + 'static
where
    W: WriteData,
    R: WriteResponse,
{
    type ApplyFuture<'life0>: Send + Future<Output = ()>
    where
        Self: 'life0;

    fn apply(
        &self,
        group_id: u64,
        state: &GroupState,
        applys: Vec<Apply<W, R>>,
    ) -> Self::ApplyFuture<'_>;
}