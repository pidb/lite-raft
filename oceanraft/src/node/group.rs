use std::sync::Arc;

use raft::prelude::ConfChangeTransition;
use raft::RaftLog;
use raft::RawNode;
use raft::Ready;
use raft::StateRole;
use tracing::debug;
use tracing::error;
use uuid::Uuid;

use crate::msg::ApplyResult;
use crate::msg::ConfChangeContext;
use crate::msg::ConfChangeMessageWithSender;
use crate::msg::ReadIndexMessageWithSender;
use crate::msg::WriteMessageWithSender;
use crate::multiraft::ProposeResponse;
use crate::prelude::ConfChange;
use crate::prelude::ConfChangeSingle;
use crate::prelude::ConfChangeV2;
use crate::prelude::MembershipChangeData;
use crate::prelude::ReplicaDesc;

use super::actor::ResponseCallback;
use super::actor::ResponseCallbackQueue;
use crate::error::Error;
use crate::error::ProposeError;
use crate::error::RaftGroupError;
use crate::msg::ConfChangeMessage;
use crate::msg::WriteMessage;
use crate::proposal::Proposal;
use crate::proposal::ProposalQueue;
use crate::proposal::ReadIndexProposal;
use crate::proposal::ReadIndexQueue;
use crate::state::GroupState;
use crate::storage::RaftStorage;
use crate::utils::flexbuffer_serialize;
use crate::ProposeRequest;

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
    pub(crate) fn has_pending_conf(&self) -> bool {
        self.raft_group.raft.has_pending_conf()
    }

    fn pre_propose_write<WD: ProposeRequest>(
        &mut self,
        request: &WriteMessage<WD>,
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
        msg: WriteMessageWithSender<REQ, RES>,
    ) -> Option<ResponseCallback> {
        if let Err(err) = self.pre_propose_write(&msg.msg) {
            return Some(ResponseCallbackQueue::new_error_callback(msg.tx, err));
        }

        let term = self.term();

        // serialize propose data
        let data = match flexbuffer_serialize(&msg.msg.propose) {
            Err(err) => {
                return Some(ResponseCallbackQueue::new_error_callback(msg.tx, err));
            }
            Ok(mut ser) => ser.take_buffer(),
        };

        // propose to raft group
        let next_index = self.last_index() + 1;
        let context = msg.msg.context.map_or(vec![], |ctx_data| ctx_data);
        if let Err(err) = self.raft_group.propose(context, data) {
            return Some(ResponseCallbackQueue::new_error_callback(
                msg.tx,
                Error::Raft(err),
            ));
        }

        let index = self.last_index() + 1;
        if next_index == index {
            return Some(ResponseCallbackQueue::new_error_callback(
                msg.tx,
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
            tx: Some(msg.tx),
        };
        self.proposals.push(proposal);
        tracing::info!(
            "node {}: group-{} replica-{} propose {}",
            self.node_id,
            self.group_id,
            self.replica_id,
            next_index
        );
        None
    }

    pub fn read_index_propose(
        &mut self,
        mut msg: ReadIndexMessageWithSender,
    ) -> Option<ResponseCallback> {
        let uuid = match msg.msg.context.uuid {
            Some(uuid) => uuid.clone(),
            None => {
                let new_uuid = *Uuid::new_v4().as_bytes();
                msg.msg.context.uuid = Some(new_uuid.clone());
                new_uuid
            }
        };

        let mut flexs =
            flexbuffer_serialize(&msg.msg.context).expect("invalid ReadIndexContext type");
        self.raft_group.read_index(flexs.take_buffer());

        let proposal = ReadIndexProposal {
            uuid: Uuid::from_bytes(uuid),
            read_index: None,
            context: None,
            tx: Some(msg.tx),
        };
        self.read_index_queue.push_back(proposal);
        None
    }

    fn pre_propose_conf_change(&mut self, msg: &ConfChangeMessage) -> Result<(), Error> {
        if self.raft_group.raft.has_pending_conf() {
            return Err(Error::Propose(ProposeError::MembershipPending(
                self.node_id,
                self.group_id,
            )));
        }

        if msg.group_id == 0 {
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

        if !msg.term != 0 && self.term() > msg.term {
            return Err(Error::Propose(ProposeError::Stale(msg.term, self.term())));
        }

        Ok(())
    }

    #[tracing::instrument(
        name = "RaftGroup::propose_conf_change",
        level = tracing::Level::TRACE,
        skip_all,
        fields(node_id=self.node_id, group_id=msg.msg.group_id),
   )]
    pub(crate) fn propose_conf_change(
        &mut self,
        msg: ConfChangeMessageWithSender<RES>,
    ) -> Option<ResponseCallback> {
        if let Err(err) = self.pre_propose_conf_change(&msg.msg) {
            return Some(ResponseCallbackQueue::new_error_callback(msg.tx, err));
        }

        let term = self.term();
        let next_index = self.last_index() + 1;

        let res = if msg.msg.data.changes.len() == 1 {
            let (ctx, cc) = to_cc(msg.msg.data, msg.msg.context); // TODO: move to proto
            assert_ne!(ctx.len(), 0);
            self.raft_group.propose_conf_change(ctx, cc)
        } else {
            let (ctx, cc) = to_ccv2(msg.msg.data, msg.msg.context); // TODO: move to proto and use into
            self.raft_group.propose_conf_change(ctx, cc)
        };

        if let Err(err) = res {
            error!(
                "node {}: propose membership change error: error = {}",
                self.node_id, err
            );
            return Some(ResponseCallbackQueue::new_error_callback(
                msg.tx,
                Error::Raft(err),
            ));
        }

        let index = self.last_index() + 1;
        if next_index == index {
            error!(
                "node {}: propose membership failed, expect log index = {}, got = {}",
                self.node_id,
                next_index,
                index - 1,
            );

            return Some(ResponseCallbackQueue::new_error_callback(
                msg.tx,
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
            tx: Some(msg.tx),
        };

        self.proposals.push(proposal);

        debug!(
            "node {}: group-{} replica-{} propose conf change {}",
            self.node_id, self.group_id, self.replica_id, next_index
        );
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

    pub(crate) fn advance_apply(&mut self, result: &ApplyResult) {
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

    let ctx = ConfChangeContext { data, user_ctx };

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

    let ctx = ConfChangeContext { data, user_ctx };

    let mut ser = flexbuffer_serialize(&ctx).unwrap();
    (ser.take_buffer(), cc)
}
