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
use crate::protos::MultiRaftMessage;

use super::error::Error;
use super::proposal::Proposal;
use super::ProposeRequest;

pub struct WriteRequest<REQ, RES>
    where
        REQ: ProposeRequest,
        RES: ProposeResponse,
{
    pub group_id: u64,
    pub term: u64,
    pub data: REQ,
    pub context: Option<Vec<u8>>,
    pub tx: oneshot::Sender<Result<(RES, Option<Vec<u8>>), Error>>,
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

#[derive(Clone, Serialize, Deserialize)]
pub struct ReadIndexContext {
    pub uuid: [u8; 16],

    /// context for user
    pub context: Option<Vec<u8>>,
}

pub struct ReadIndexData {
    pub group_id: u64,
    pub context: ReadIndexContext,
    pub tx: oneshot::Sender<Result<Option<Vec<u8>>, Error>>,
}

pub enum ProposeMessage<REQ, RES>
    where
        REQ: ProposeRequest,
        RES: ProposeResponse,
{
    Write(WriteRequest<REQ, RES>),
    Membership(MembershipRequest<RES>),
    ReadIndexData(ReadIndexData),
}

pub enum ManageMessage {
    CreateGroup(CreateGroupRequest, oneshot::Sender<Result<(), Error>>),
    RemoveGroup(RemoveGroupRequest, oneshot::Sender<Result<(), Error>>),
}

pub enum NodeMessage<REQ, RES>
    where
        REQ: ProposeRequest,
        RES: ProposeResponse
{
    Peer(MultiRaftMessage),
    Write(WriteRequest<REQ, RES>),
    Membership(MembershipRequest<RES>),
    ReadIndexData(ReadIndexData),
    CreateGroup(CreateGroupRequest, oneshot::Sender<Result<(), Error>>),
    RemoveGroup(RemoveGroupRequest, oneshot::Sender<Result<(), Error>>),
}

#[allow(unused)]
pub const SUGGEST_MAX_APPLY_BATCH_SIZE: usize = 64 * 1024 * 1024;

#[derive(Debug)]
pub struct ApplyData<R>
    where
        R: ProposeResponse,
{
    pub replica_id: u64,
    pub group_id: u64,
    pub term: u64,
    pub commit_index: u64,
    pub commit_term: u64,
    pub entries: Vec<Entry>,
    pub entries_size: usize,
    pub proposals: Vec<Proposal<R>>,
}

impl<R> ApplyData<R>
    where
        R: ProposeResponse,
{
    pub fn try_batch(&mut self, that: &mut ApplyData<R>, max_batch_size: usize) -> bool {
        assert_eq!(self.replica_id, that.replica_id);
        assert_eq!(self.group_id, that.group_id);
        assert!(that.term >= self.term);
        assert!(that.commit_index >= self.commit_index);
        assert!(that.commit_term >= self.commit_term);
        if max_batch_size == 0 || self.entries_size + that.entries_size > max_batch_size {
            return false;
        }
        self.term = that.term;
        self.commit_index = that.commit_index;
        self.commit_term = that.commit_term;
        self.entries.append(&mut that.entries);
        self.entries_size += that.entries_size;
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

#[derive(Debug)]
pub struct ApplyResultMessage {
    pub group_id: u64,
    pub applied_index: u64,
    pub applied_term: u64,
}

/// Commit membership change results.
///
/// If proposed change is ConfChange, the ConfChangeV2 is converted
/// from ConfChange. If ConfChangeV2 is used, changes contains multiple
/// requests, otherwise changes contains only one request.
#[derive(Debug, Clone)]
pub struct CommitMembership {
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
pub enum ApplyCommitMessage {
    None,
    Membership((CommitMembership, oneshot::Sender<Result<ConfState, Error>>)),
}

impl Default for ApplyCommitMessage {
    fn default() -> Self {
        ApplyCommitMessage::None
    }
}

/// An internal structure to query raft internal status in
/// a memory communicative way.
#[derive(Debug)]
pub enum QueryGroup {
    /// Queries if there has a pending configuration,
    /// returns true or false
    HasPendingConf(u64, oneshot::Sender<Result<bool, Error>>),
}
