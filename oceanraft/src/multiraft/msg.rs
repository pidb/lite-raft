use std::collections::HashMap;

use tokio::sync::oneshot;

use crate::prelude::AppWriteRequest;
use crate::prelude::ConfChangeV2;
use crate::prelude::Entry;
use crate::prelude::MembershipChangeRequest;
use crate::prelude::RaftGroupManagement;

use super::error::Error;
use super::group::RaftGroupApplyState;
use super::proposal::Proposal;
use super::response::AppWriteResponse;

pub enum WriteMessage<RES: AppWriteResponse> {
    Write(AppWriteRequest, oneshot::Sender<Result<RES, Error>>),
    Membership(MembershipChangeRequest, oneshot::Sender<Result<RES, Error>>),
}

pub enum AdminMessage {
    Group(RaftGroupManagement, oneshot::Sender<Result<(), Error>>),
}

#[derive(Debug)]
pub struct ApplyData<RES>
where
    RES: AppWriteResponse,
{
    pub replica_id: u64,
    pub group_id: u64,
    pub term: u64,
    pub commit_index: u64,
    pub commit_term: u64,
    pub entries: Vec<Entry>,
    pub entries_size: usize,
    pub proposals: Vec<Proposal<RES>>,
}

impl<RES> ApplyData<RES>
where
    RES: AppWriteResponse,
{
    pub fn try_batch(&mut self, that: &mut ApplyData<RES>, max_batch_size: usize) -> bool {
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

pub enum ApplyMessage<RES: AppWriteResponse> {
    Apply {
        applys: HashMap<u64, ApplyData<RES>>,
    },
}

#[derive(Debug)]
pub struct ApplyResultMessage {
    pub group_id: u64,
    pub apply_state: RaftGroupApplyState,
}

/// Commit membership change results.
///
/// If proposed change is ConfChange, the ConfChangeV2 is converted
/// from ConfChange. If ConfChangeV2 is used, changes contains multiple
/// requests, otherwise changes contains only one request.
#[derive(Debug)]
pub(crate) struct CommitMembership {
    #[allow(unused)]
    pub(crate) entry_index: u64,
    pub(crate) conf_change: ConfChangeV2,
    pub(crate) change_request: MembershipChangeRequest,
}

#[derive(Debug)]
pub(crate) enum ApplyCommitMessage {
    None,
    Membership((CommitMembership, oneshot::Sender<Result<(), Error>>)),
}

impl Default for ApplyCommitMessage {
    fn default() -> Self {
        ApplyCommitMessage::None
    }
}

