extern crate raft_proto;

use std::collections::HashMap;

use serde::Deserialize;
use serde::Serialize;
use tokio::sync::oneshot;

use crate::multiraft::ProposeResponse;
use crate::prelude::ConfChangeV2;
use crate::prelude::ConfState;
use crate::prelude::CreateGroupRequest;
use crate::prelude::Entry;
use crate::prelude::MembershipChangeData;
use crate::prelude::RemoveGroupRequest;

use super::error::Error;
use super::proposal::Proposal;
use super::ProposeRequest;

/// Message with notify.
pub struct MessageWithNotify<T, R> {
    pub msg: T,
    pub tx: oneshot::Sender<R>,
}

/// WriteMessage propose a write request message to raft.
pub struct WriteMessage<REQ>
where
    REQ: ProposeRequest,
{
    /// Specific the group to write to.
    pub group_id: u64,
    /// The term attached to the write (usually the current group leader term),
    /// and an error is returned if the group's term is greater than this value,
    /// commonly used to detect if a leader switch has occurred.
    /// The default is 0 which means the check is disabled.
    pub term: u64,
    pub propose: REQ,
    pub context: Option<Vec<u8>>,
}

/// Inner WriteMessage with sender to  propose a write request message to raft.
pub(crate) struct WriteMessageWithSender<REQ, RES>
where
    REQ: ProposeRequest,
    RES: ProposeResponse,
{
    pub(crate) msg: WriteMessage<REQ>,
    pub(crate) tx: oneshot::Sender<Result<(RES, Option<Vec<u8>>), Error>>,
}

/// ReadIndexContext is the context for read index request.
#[derive(Clone, Serialize, Deserialize)]
pub struct ReadIndexContext {
    /// Uuid for read index request.If the uuid is None, it will
    /// be automatically generated during the execution process.
    pub uuid: Option<[u8; 16]>,

    /// data for user context
    pub data: Option<Vec<u8>>,
}

/// ReadIndexMessage propose a read index request message to raft.
pub struct ReadIndexMessage {
    pub group_id: u64,
    pub context: ReadIndexContext,
}

pub(crate) struct ReadIndexMessageWithSender {
    pub(crate) msg: ReadIndexMessage,
    pub(crate) tx: oneshot::Sender<Result<Option<Vec<u8>>, Error>>,
}

#[derive(Serialize, Deserialize)]
pub struct ConfChangeContext {
    pub data: MembershipChangeData,
    pub user_ctx: Option<Vec<u8>>,
}

pub struct ConfChangeMessage {
    pub group_id: u64,
    pub term: u64,
    pub context: Option<Vec<u8>>,
    pub data: MembershipChangeData,
}

pub(crate) struct ConfChangeMessageWithSender<RES>
where
    RES: ProposeResponse,
{
    pub(crate) msg: ConfChangeMessage,
    pub(crate) tx: oneshot::Sender<Result<(RES, Option<Vec<u8>>), Error>>,
}

#[derive(Debug)]
pub enum ApplyResultMessage {
    None,
    ApplyResult(ApplyResult),
    ApplyConfChange((ApplyConfChange, oneshot::Sender<Result<ConfState, Error>>)),
}

impl Default for ApplyResultMessage {
    fn default() -> Self {
        ApplyResultMessage::None
    }
}

/// Apply conf change results.
///
/// If proposed change is ConfChange, the ConfChangeV2 is converted
/// from ConfChange. If ConfChangeV2 is used, changes contains multiple
/// requests, otherwise changes contains only one request.
#[derive(Debug, Clone)]
pub struct ApplyConfChange {
    /// Specific group.
    pub group_id: u64,

    /// Entry index.
    pub index: u64,

    /// Entry term.
    pub term: u64,

    /// Conf change.
    pub conf_change: ConfChangeV2,

    /// Specific change request data from the client.
    pub change_request: Option<MembershipChangeData>,
}

#[derive(Debug)]
pub struct ApplyResult {
    pub group_id: u64,
    pub applied_index: u64,
    pub applied_term: u64,
}

pub(crate) enum NodeMessage<REQ, RES>
where
    REQ: ProposeRequest,
    RES: ProposeResponse,
{
    Write(WriteMessageWithSender<REQ, RES>),
    ReadIndex(ReadIndexMessageWithSender),
    ConfChange(ConfChangeMessageWithSender<RES>),
    Campaign(MessageWithNotify<u64, Result<(), Error>>),
    CreateGroup(MessageWithNotify<CreateGroupRequest, Result<(), Error>>),
    RemoveGroup(MessageWithNotify<RemoveGroupRequest, Result<(), Error>>),
    Inner(InnerMessage),
    ApplyResult(ApplyResultMessage),
}

#[allow(unused)]
pub const SUGGEST_MAX_APPLY_BATCH_SIZE: usize = 64 * 1024 * 1024;

#[derive(Debug)]
pub struct ApplyDataMeta {
    pub replica_id: u64,
    pub group_id: u64,
    pub leader_id: u64,
    pub term: u64,
    pub commit_index: u64,
    pub commit_term: u64,
    pub entries_size: usize,
}

#[derive(Debug)]
pub struct ApplyData<R>
where
    R: ProposeResponse,
{
    pub meta: ApplyDataMeta,
    pub entries: Vec<Entry>,
    pub proposals: Vec<Proposal<R>>,
}

impl<R> ApplyData<R>
where
    R: ProposeResponse,
{
    pub fn try_batch(&mut self, that: &mut ApplyData<R>, max_batch_size: usize) -> bool {
        assert_eq!(self.meta.replica_id, that.meta.replica_id);
        assert_eq!(self.meta.group_id, that.meta.group_id);
        assert!(that.meta.term >= self.meta.term);
        assert!(that.meta.commit_index >= self.meta.commit_index);
        assert!(that.meta.commit_term >= self.meta.commit_term);
        if max_batch_size == 0 || self.meta.entries_size + that.meta.entries_size > max_batch_size {
            return false;
        }
        self.meta.term = that.meta.term;
        self.meta.commit_index = that.meta.commit_index;
        self.meta.commit_term = that.meta.commit_term;
        self.entries.append(&mut that.entries);
        self.meta.entries_size += that.meta.entries_size;
        self.proposals.append(&mut that.proposals);
        return true;
    }
}

pub enum ApplyMessage<RES>
where
    RES: ProposeResponse,
{
    Apply {
        applys: HashMap<u64, ApplyData<RES>>,
    },
}

/// An internal structure to query raft internal status in
/// a memory communicative way.
#[derive(Debug)]
pub enum InnerMessage {
    /// Queries if there has a pending configuration,
    /// returns true or false
    HasPendingConfChange(u64, oneshot::Sender<Result<bool, Error>>),
}

impl std::fmt::Display for InnerMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InnerMessage::HasPendingConfChange(group_id, _) => {
                write!(f, "InnerMessage::HasPendingConfChange({})", group_id)
            }
        }
    }
}
