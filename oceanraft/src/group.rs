use std::sync::Arc;

use raft::prelude::ConfChangeTransition;
use raft::RaftLog;
use raft::RawNode;
use raft::Ready;
use raft::StateRole;
use tracing::error;
use uuid::Uuid;

use crate::msg::MembershipRequestContext;
use crate::multiraft::ProposeResponse;
use crate::prelude::ConfChange;
use crate::prelude::ConfChangeSingle;
use crate::prelude::ConfChangeV2;
use crate::prelude::MembershipChangeData;
use crate::prelude::ReplicaDesc;

use super::error::Error;
use super::error::ProposeError;
use super::error::RaftGroupError;
use super::msg::ApplyResultRequest;
use super::msg::MembershipRequest;
use super::msg::ReadIndexRequest;
use super::msg::WriteRequest;
use super::node::ResponseCallback;
use super::node::ResponseCallbackQueue;
use super::proposal::Proposal;
use super::proposal::ProposalQueue;
use super::proposal::ReadIndexProposal;
use super::proposal::ReadIndexQueue;
use super::state::GroupState;
use super::storage::RaftStorage;
use super::utils::flexbuffer_serialize;
use super::ProposeRequest;

pub enum Status {
    None,
    Delete,
}

#[derive(Default, Debug)]
pub struct RaftGroupWriteRequest {
    pub replica_id: u64,
    pub ready: Option<Ready>,
}

/// Represents a replica of a raft group.
pub struct RaftGroup<RS, RES>
where
    RS: RaftStorage,
    RES: ProposeResponse,
{
    /// Indicates the id of the node where the group resides.
    pub node_id: u64,

    pub group_id: u64,
    pub replica_id: u64,
    pub raft_group: RawNode<RS>,
    // track the nodes which members ofq the raft consensus group
    pub node_ids: Vec<u64>,
    pub proposals: ProposalQueue<RES>,

    pub leader: ReplicaDesc,

    /// the current latest commit index, which is different from the
    /// internal `commit_index` of `raft_group`, may be the `commit_index`
    /// but not yet advance state machine, meaning that `commit_index`
    /// should be greater than or equal to `raft_group`
    pub commit_index: u64,

    /// the current latest `commit_term`, which is different from the
    /// internal `commit_term` of `raft_group`, may be the `commit_term`
    /// but not yet advance state machine, meaning that `commit_term`
    /// should be greater than or equal to `raft_group`
    pub commit_term: u64,

    pub status: Status,
    pub read_index_queue: ReadIndexQueue,
    pub shared_state: Arc<GroupState>,
}

impl<RS, RES> RaftGroup<RS, RES>
where
    RS: RaftStorage,
    RES: ProposeResponse,
{
    #[inline]
    pub(crate) fn has_ready(&self) -> bool {
        self.raft_group.has_ready()
    }

    #[inline]
    pub(crate) fn is_leader(&self) -> bool {
        self.raft_group.raft.state == StateRole::Leader
    }

    #[inline]
    pub(crate) fn is_candidate(&self) -> bool {
        self.raft_group.raft.state == StateRole::Candidate
    }

    #[inline]
    pub(crate) fn is_pre_candidate(&self) -> bool {
        self.raft_group.raft.state == StateRole::PreCandidate
    }

    #[inline]
    pub(crate) fn term(&self) -> u64 {
        self.raft_group.raft.term
    }

    #[inline]
    pub(crate) fn last_index(&self) -> u64 {
        self.raft_group.raft.raft_log.last_index()
    }

    #[inline]
    pub(crate) fn group_id(&self) -> u64 {
        self.group_id
    }

    #[inline]
    pub(crate) fn replica_id(&self) -> u64 {
        self.raft_group.raft.id
    }

    #[inline]
    pub(crate) fn ready(&mut self) -> Ready {
        self.raft_group.ready()
    }

    #[inline]
    pub(crate) fn leader(&self) -> ReplicaDesc {
        self.leader.clone()
    }

    #[inline]
    pub(crate) fn set_leader(&mut self, leader: ReplicaDesc) {
        self.leader = leader;
    }

    #[inline]
    pub(crate) fn raft_log(&self) -> &RaftLog<RS> {
        &self.raft_group.raft.raft_log
    }

    #[inline]
    pub(crate) fn commit_term(&self) -> u64 {
        self.commit_term
    }

    #[inline]
    pub(crate) fn set_commit_term(&mut self, term: u64) {
        self.commit_term = term;
    }

    #[inline]
    pub(crate) fn commit_index(&self) -> u64 {
        self.commit_index
    }

    #[inline]
    pub(crate) fn set_commit_index(&mut self, index: u64) {
        self.commit_index = index;
    }

    #[inline]
    pub(crate) fn proposal_queue_mut(&mut self) -> &mut ProposalQueue<RES> {
        &mut self.proposals
    }

    fn pre_propose_write<WD: ProposeRequest>(
        &mut self,
        request: &WriteRequest<WD, RES>,
    ) -> Result<(), Error> {
        // if write_data.data.is_empty() {
        //     return Err(Error::BadParameter(
        //         "write request data must not be empty".to_owned(),
        //     ));
        // }

        // TODO: let forward_to_leader as configurable
        if !self.is_leader() {
            return Err(Error::Propose(ProposeError::NotLeader {
                node_id: self.node_id,
                group_id: self.group_id,
                replica_id: self.replica_id,
            }));
        }

        if request.term != 0 && self.term() > request.term {
            return Err(Error::Propose(ProposeError::Stale(
                request.term,
                self.term(),
            )));
        }

        Ok(())
    }

    pub fn propose_write<REQ: ProposeRequest>(
        &mut self,
        request: WriteRequest<REQ, RES>,
    ) -> Option<ResponseCallback> {
        if let Err(err) = self.pre_propose_write(&request) {
            return Some(ResponseCallbackQueue::new_error_callback(request.tx, err));
        }

        let term = self.term();

        // serialize propose data
        let data = match flexbuffer_serialize(&request.propose) {
            Err(err) => {
                return Some(ResponseCallbackQueue::new_error_callback(request.tx, err));
            }
            Ok(mut ser) => ser.take_buffer(),
        };

        // propose to raft group
        let next_index = self.last_index() + 1;
        let context = request.context.map_or(vec![], |ctx_data| ctx_data);
        if let Err(err) = self.raft_group.propose(context, data) {
            return Some(ResponseCallbackQueue::new_error_callback(
                request.tx,
                Error::Raft(err),
            ));
        }

        let index = self.last_index() + 1;
        if next_index == index {
            return Some(ResponseCallbackQueue::new_error_callback(
                request.tx,
                Error::Propose(ProposeError::UnexpectedIndex {
                    node_id: self.node_id,
                    group_id: self.group_id,
                    replica_id: self.replica_id,
                    expected: next_index,
                    unexpected: index - 1,
                }),
            ));
        }

        // save proposal to queue
        let proposal = Proposal {
            index: next_index,
            term,
            is_conf_change: false,
            tx: Some(request.tx),
        };
        self.proposals.push(proposal);

        None
    }

    pub fn read_index_propose(&mut self, data: ReadIndexRequest) -> Option<ResponseCallback> {
        let mut flexs = flexbuffer_serialize(&data.context).expect("invalid ReadIndexContext type");
        self.raft_group.read_index(flexs.take_buffer());

        let proposal = ReadIndexProposal {
            uuid: Uuid::from_bytes(data.context.uuid),
            read_index: None,
            context: None,
            tx: Some(data.tx),
        };
        self.read_index_queue.push_back(proposal);
        None
    }

    fn pre_propose_membership(&mut self, request: &MembershipRequest<RES>) -> Result<(), Error> {
        if self.raft_group.raft.has_pending_conf() {
            return Err(Error::Propose(
                super::error::ProposeError::MembershipPending(self.node_id, self.group_id),
            ));
        }

        if request.group_id == 0 {
            return Err(Error::BadParameter(
                "group id must be more than 0".to_owned(),
            ));
        }

        if !self.is_leader() {
            return Err(Error::Propose(ProposeError::NotLeader {
                node_id: self.node_id,
                group_id: self.group_id,
                replica_id: self.replica_id,
            }));
        }

        if !request.term.is_none() && self.term() > request.term.unwrap() {
            return Err(Error::Propose(ProposeError::Stale(
                request.term.unwrap(),
                self.term(),
            )));
        }

        Ok(())
    }

    pub fn propose_membership_change(
        &mut self,
        request: MembershipRequest<RES>,
    ) -> Option<ResponseCallback> {
        // TODO: add pre propose check
        if let Err(err) = self.pre_propose_membership(&request) {
            return Some(ResponseCallbackQueue::new_error_callback(request.tx, err));
        }

        let term = self.term();

        let next_index = self.last_index() + 1;

        let res = if request.data.changes.len() == 1 {
            let (ctx, cc) = to_cc(request.data, request.context); // TODO: move to proto
            assert_ne!(ctx.len(), 0);
            self.raft_group.propose_conf_change(ctx, cc)
        } else {
            let (ctx, cc) = to_ccv2(request.data, request.context); // TODO: move to proto and use into
            self.raft_group.propose_conf_change(ctx, cc)
        };

        if let Err(err) = res {
            error!(
                "node {}: propose membership change error: error = {}",
                0, /* TODO: add it*/ err
            );
            return Some(ResponseCallbackQueue::new_error_callback(
                request.tx,
                Error::Raft(err),
            ));
        }

        let index = self.last_index() + 1;
        if next_index == index {
            error!(
                "node {}: propose membership failed, expect log index = {}, got = {}",
                0, /* TODO: add it*/
                next_index,
                index - 1,
            );

            return Some(ResponseCallbackQueue::new_error_callback(
                request.tx,
                Error::Propose(ProposeError::UnexpectedIndex {
                    node_id: self.node_id,
                    group_id: self.group_id,
                    replica_id: self.replica_id,
                    expected: next_index,
                    unexpected: index - 1,
                }),
            ));
        }

        let proposal = Proposal {
            index: next_index,
            term,
            is_conf_change: true,
            tx: Some(request.tx),
        };

        self.proposals.push(proposal);
        None
    }

    /// Remove pending proposals.
    pub(crate) fn remove_pending_proposals(&mut self) {
        let proposals = self.proposals.drain(..);
        for proposal in proposals.into_iter() {
            let err = Err(Error::RaftGroup(RaftGroupError::Deleted(
                self.group_id,
                self.replica_id,
            )));
            // TODO: move to event queue
            proposal.tx.map(|tx| tx.send(err));
        }
    }

    pub(crate) fn add_track_node(&mut self, node_id: u64) {
        if self.node_ids.iter().position(|id| *id == node_id).is_none() {
            self.node_ids.push(node_id)
        }
    }

    /// Remove the node in the group where the replica is located in the tracing nodes.
    pub(crate) fn remove_track_node(&mut self, node_id: u64) {
        let len = self.node_ids.len();
        self.node_ids
            .iter()
            .position(|id| node_id == *id)
            .map(|idx| {
                self.node_ids.swap(idx, len - 1);
                self.node_ids.truncate(len - 1);
            });
    }

    pub(crate) fn advance_apply(&mut self, result: &ApplyResultRequest) {
        // keep  invariant
        assert!(result.applied_index <= self.commit_index);

        self.raft_group.advance_apply_to(result.applied_index);

        // update local apply state
        // self.applied_index = result.applied_index;
        // self.applied_term = result.applied_term;

        // update shared state for apply
        // self.shared_state.set_applied_index(result.applied_index);
        // self.shared_state.set_applied_term(result.applied_term);
    }
}

fn to_cc(data: MembershipChangeData, user_ctx: Option<Vec<u8>>) -> (Vec<u8>, ConfChange) {
    assert_eq!(data.changes.len(), 1);
    let mut cc = ConfChange::default();
    cc.set_change_type(data.changes[0].change_type());
    cc.node_id = data.changes[0].replica_id;

    let ctx = MembershipRequestContext { data, user_ctx };

    let mut ser = flexbuffer_serialize(&ctx).unwrap();
    (ser.take_buffer(), cc)
}

fn to_ccv2(data: MembershipChangeData, user_ctx: Option<Vec<u8>>) -> (Vec<u8>, ConfChangeV2) {
    // Handle auto leave case
    if data.transition() == ConfChangeTransition::Auto && data.changes.is_empty() {
        let cc = ConfChangeV2::default();
        return (vec![], cc);
    }

    let mut cc = ConfChangeV2::default();
    cc.set_transition(data.transition());
    let mut sc = vec![];
    for change in data.changes.iter() {
        sc.push(ConfChangeSingle {
            change_type: change.change_type,
            node_id: change.replica_id,
        });
    }

    cc.set_changes(sc);

    let ctx = MembershipRequestContext { data, user_ctx };

    let mut ser = flexbuffer_serialize(&ctx).unwrap();
    (ser.take_buffer(), cc)
}
