extern crate raft_proto;

use std::vec::IntoIter;

use futures::Future;
use prost::Message;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

use raft_proto::ConfChangeI;

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
    pub entry: Entry,
    pub tx: Option<oneshot::Sender<Result<RES, Error>>>,
    pub(crate) commit_tx: UnboundedSender<ApplyCommitMessage>,
}

impl<RES: WriteResponse> ApplyMembership<RES> {
    fn parse(&self) -> CommitMembership {
        match self.entry.entry_type() {
            EntryType::EntryNormal => unreachable!(),
            EntryType::EntryConfChange => {
                let mut conf_change = ConfChange::default();
                conf_change.merge(self.entry.data.as_ref()).unwrap();

                let mut change_request = MembershipChangeData::default();
                change_request.merge(self.entry.context.as_ref()).unwrap();

                CommitMembership {
                    entry_index: self.entry.index,
                    conf_change: conf_change.into_v2(),
                    change_request,
                }
            }
            EntryType::EntryConfChangeV2 => {
                let mut conf_change = ConfChangeV2::default();
                conf_change.merge(self.entry.data.as_ref()).unwrap();

                let mut change_request = MembershipChangeData::default();
                change_request.merge(self.entry.context.as_ref()).unwrap();

                CommitMembership {
                    entry_index: self.entry.index,
                    conf_change,
                    change_request,
                }
            }
        }
    }

    /// Commit membership change to multiraft.
    ///
    /// Note: it's must be called because multiraft need apply membersip change to raft.
    pub async fn done(&self) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let commit = self.parse();
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
            Self::Membership(membership) => membership.entry.index,
        }
    }

    #[allow(unused)]
    pub(crate) fn get_term(&self) -> u64 {
        match self {
            Self::NoOp(noop) => noop.entry_term,
            Self::Normal(normal) => normal.term,
            Self::Membership(membership) => membership.entry.term,
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
    type ApplyFuture<'life0>: Send + Future<Output = Option<IntoIter<Apply<W, R>>>>
    where
        Self: 'life0;

    fn apply(
        &self,
        group_id: u64,
        state: &GroupState,
        iter: IntoIter<Apply<W, R>>,
    ) -> Self::ApplyFuture<'_>;
}
