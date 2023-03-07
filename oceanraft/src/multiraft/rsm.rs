use std::vec::IntoIter;

use futures::Future;
use prost::Message;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

use crate::multiraft::error::ChannelError;
use crate::prelude::ConfChange;
use crate::prelude::ConfChangeI;
use crate::prelude::ConfChangeV2;
use crate::prelude::Entry;
use crate::prelude::EntryType;
use crate::prelude::MembershipChangeRequest;

use super::error::Error;
use super::msg::ApplyCommitMessage;
use super::msg::CommitMembership;
use super::response::AppWriteResponse;

#[derive(Debug)]
pub struct ApplyNoOp {
    pub group_id: u64,
    pub entry_index: u64,
    pub entry_term: u64,
}

#[derive(Debug)]
pub struct ApplyNormal<RES: AppWriteResponse> {
    pub group_id: u64,
    pub entry: Entry,
    pub is_conf_change: bool,
    pub tx: Option<oneshot::Sender<Result<RES, Error>>>,
}

#[derive(Debug)]
pub struct ApplyMembership<RES: AppWriteResponse> {
    pub group_id: u64,
    pub entry: Entry,
    pub tx: Option<oneshot::Sender<Result<RES, Error>>>,
    pub(crate) commit_tx: UnboundedSender<ApplyCommitMessage>,
}

impl<RES: AppWriteResponse> ApplyMembership<RES> {
    fn parse(&self) -> CommitMembership {
        match self.entry.entry_type() {
            EntryType::EntryNormal => unreachable!(),
            EntryType::EntryConfChange => {
                let mut conf_change = ConfChange::default();
                conf_change.merge(self.entry.data.as_ref()).unwrap();

                let mut change_request = MembershipChangeRequest::default();
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

                let mut change_request = MembershipChangeRequest::default();
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
pub enum Apply<RES: AppWriteResponse> {
    NoOp(ApplyNoOp),
    Normal(ApplyNormal<RES>),
    Membership(ApplyMembership<RES>),
}

impl<RES: AppWriteResponse> Apply<RES> {
    pub(crate) fn entry_index(&self) -> u64 {
        match self {
            Self::NoOp(noop) => noop.entry_index,
            Self::Normal(normal) => normal.entry.index,
            Self::Membership(membership) => membership.entry.index,
        }
    }

    #[allow(unused)]
    pub(crate) fn entry_term(&self) -> u64 {
        match self {
            Self::NoOp(noop) => noop.entry_term,
            Self::Normal(normal) => normal.entry.term,
            Self::Membership(membership) => membership.entry.term,
        }
    }
}

pub trait StateMachine<R>: Send + 'static
where
    R: AppWriteResponse,
{
    type ApplyFuture<'life0>: Send + Future<Output = Option<IntoIter<Apply<R>>>>
    where
        Self: 'life0;

    fn apply(&mut self, iter: IntoIter<Apply<R>>) -> Self::ApplyFuture<'_>;
}
