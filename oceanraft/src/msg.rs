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
// use crate::prelude::MultiRaftMessageResponse;
use crate::prelude::RemoveGroupRequest;
// use crate::protos::MultiRaftMessage;

use super::error::Error;
use super::proposal::Proposal;
use super::ProposeRequest;

/// Message with notify.
pub struct MessageWithNotify<T, R> {
    pub msg: T,
    pub tx: oneshot::Sender<R>,
}

/// WriteRequest propose a write request to raft.
pub struct WriteRequest<REQ, RES>
where
    REQ: ProposeRequest,
    RES: ProposeResponse,
{
    pub group_id: u64,
    pub term: u64,
    pub propose: REQ,
    pub context: Option<Vec<u8>>,
    pub tx: oneshot::Sender<Result<(RES, Option<Vec<u8>>), Error>>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ReadIndexContext {
    pub uuid: [u8; 16],

    /// context for user
    pub context: Option<Vec<u8>>,
}

pub struct ReadIndexRequest {
    pub group_id: u64,
    pub context: ReadIndexContext,
    pub tx: oneshot::Sender<Result<Option<Vec<u8>>, Error>>,
}

#[derive(Serialize, Deserialize)]
pub struct MembershipRequestContext {
    pub data: MembershipChangeData,
    pub user_ctx: Option<Vec<u8>>,
}

pub struct MembershipRequest<RES>
where
    RES: ProposeResponse,
{
    pub group_id: u64,
    pub term: Option<u64>,
    pub context: Option<Vec<u8>>,
    pub data: MembershipChangeData,
    pub tx: oneshot::Sender<Result<(RES, Option<Vec<u8>>), Error>>,
}

#[derive(Debug)]
pub struct ApplyResultRequest {
    pub group_id: u64,
    pub applied_index: u64,
    pub applied_term: u64,
}

#[derive(Debug)]
pub enum ApplyResultMessage {
    None,
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

pub enum NodeMessage<REQ, RES>
where
    REQ: ProposeRequest,
    RES: ProposeResponse,
{
    // Peer(MessageWithNotify<MultiRaftMessage, Result<MultiRaftMessageResponse, Error>>),
    Write(WriteRequest<REQ, RES>),
    Membership(MembershipRequest<RES>),
    ReadIndexData(ReadIndexRequest),
    Campaign(MessageWithNotify<u64, Result<(), Error>>),
    CreateGroup(MessageWithNotify<CreateGroupRequest, Result<(), Error>>),
    RemoveGroup(MessageWithNotify<RemoveGroupRequest, Result<(), Error>>),
    Inner(InnerMessage),
    ApplyResult(ApplyResultRequest),
    ApplyCommit(ApplyResultMessage),
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
