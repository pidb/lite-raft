use std::sync::Arc;

use prost::Message;
use raft::prelude::Entry;
use raft::RawNode;
use raft::ReadState;
use raft::Ready;
use raft::SoftState;
use raft::StateRole;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;
use tracing::Level;
use uuid::Uuid;

use crate::msg::MembershipRequestContext;
use crate::multiraft::ProposeResponse;
use crate::prelude::ConfChange;
use crate::prelude::ConfChangeSingle;
use crate::prelude::ConfChangeV2;
use crate::prelude::MembershipChangeData;
use crate::prelude::ReplicaDesc;
use crate::prelude::Snapshot;

use super::error::Error;
use super::error::ProposeError;
use super::error::RaftGroupError;
use super::event::EventChannel;
use super::event::LeaderElectionEvent;
use super::msg::ApplyData;
use super::msg::ApplyResultMessage;
use super::msg::MembershipRequest;
use super::msg::ReadIndexData;
use super::msg::WriteRequest;
use super::multiraft::NO_NODE;
use super::node::NodeManager;
use super::node::ResponseCallback;
use super::node::ResponseCallbackQueue;
use super::proposal::Proposal;
use super::proposal::ProposalQueue;
use super::proposal::ReadIndexProposal;
use super::proposal::ReadIndexQueue;
use super::replica_cache::ReplicaCache;
use super::state::GroupState;
use super::storage::MultiRaftStorage;
use super::storage::RaftStorage;
use super::transport;
use super::utils;
use super::utils::flexbuffer_serialize;
use super::Event;
use super::ProposeData;

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

    /// Represents the index applied to the current group apply,
    /// updated when apply returns results    
    pub applied_index: u64,

    /// Represents the term applied to the current group apply,
    /// updated when apply returns results    
    pub applied_term: u64,

    // pub state: RaftGroupState,
    pub status: Status,
    pub read_index_queue: ReadIndexQueue,
    pub shared_state: Arc<GroupState>,
}

//===----------------------------------------------------------------------===//
// The raft group internal state
//===----------------------------------------------------------------------===//
impl<RS, RES> RaftGroup<RS, RES>
where
    RS: RaftStorage,
    RES: ProposeResponse,
{
    #[inline]
    pub(crate) fn is_leader(&self) -> bool {
        self.raft_group.raft.state == StateRole::Leader
    }

    #[inline]
    pub(crate) fn term(&self) -> u64 {
        self.raft_group.raft.term
    }

    #[inline]
    pub(crate) fn last_index(&self) -> u64 {
        self.raft_group.raft.raft_log.last_index()
    }
}

//===----------------------------------------------------------------------===//
// Handle raft group ready
//===----------------------------------------------------------------------===//
impl<RS, RES> RaftGroup<RS, RES>
where
    RS: RaftStorage,
    RES: ProposeResponse,
{
    #[tracing::instrument(
        level = Level::TRACE,
        name = "RaftGroup::handle_ready",
        skip_all,
        fields(node_id=node_id, group_id=self.group_id)
    )]
    pub(crate) async fn handle_ready<TR: transport::Transport, MRS: MultiRaftStorage<RS>>(
        &mut self,
        node_id: u64,
        transport: &TR,
        storage: &MRS,
        replica_cache: &mut ReplicaCache<RS, MRS>,
        node_manager: &mut NodeManager,
        event_bcast: &mut EventChannel,
    ) -> Result<(RaftGroupWriteRequest, Option<ApplyData<RES>>), Error> {
        let group_id = self.group_id;
        // we need to know which replica in raft group is ready.
        let replica_desc = replica_cache.replica_for_node(group_id, node_id).await?;
        let replica_desc = match replica_desc {
            Some(replica_desc) => {
                assert_eq!(replica_desc.replica_id, self.raft_group.raft.id);
                replica_desc
            }
            None => {
                // if we can't look up the replica in storage, but the group is ready,
                // we know that one of the replicas must be ready, so we can repair the
                // storage to store this replica.
                let repaired_replica_desc = ReplicaDesc {
                    group_id,
                    node_id,
                    replica_id: self.raft_group.raft.id,
                };

                replica_cache
                    .cache_replica_desc(group_id, repaired_replica_desc.clone(), true)
                    .await?;
                repaired_replica_desc
            }
        };

        // TODO: cache storage in related raft group.
        let gs = storage
            .group_storage(group_id, replica_desc.replica_id)
            .await?;

        // TODO: move brefore codes to node.rs, because theses codes maybe trigger storage error and the ready  is impacted.

        let mut rd = self.raft_group.ready();

        // send out messages
        if !rd.messages().is_empty() {
            transport::send_messages(
                node_id,
                transport,
                replica_cache,
                node_manager,
                group_id,
                rd.take_messages(),
            )
            .await;
        }

        if let Some(ss) = rd.ss() {
            self.handle_soft_state_change(node_id, ss, replica_cache, event_bcast)
                .await;
        }

        if !rd.read_states().is_empty() {
            self.on_reads_ready(rd.take_read_states())
        }

        // make apply task if need to apply commit entries
        let apply = if !rd.committed_entries().is_empty() {
            // insert_commit_entries will update latest commit term by commit entries.
            let apply = self.handle_can_apply_entries(
                node_id,
                &gs,
                replica_desc.replica_id,
                rd.take_committed_entries(),
            )?;

            Some(apply)
        } else {
            None
        };

        let gwr = RaftGroupWriteRequest {
            replica_id: replica_desc.replica_id,
            ready: Some(rd),
        };
        // make write task if need to write disk.
        Ok((gwr, apply))
    }

    fn handle_can_apply_entries(
        &mut self,
        node_id: u64,
        gs: &RS,
        replica_id: u64,
        entries: Vec<Entry>,
    ) -> Result<ApplyData<RES>, super::storage::Error> {
        debug!(
            "node {}: create apply entries [{}, {}], group = {}, replica = {}",
            node_id,
            entries[0].index,
            entries[entries.len() - 1].index,
            self.group_id,
            replica_id
        );
        // let group_id = self.group_id;
        let last_commit_ent = &entries[entries.len() - 1];

        // update shared_state for latest commit
        self.shared_state.set_commit_index(last_commit_ent.index);
        self.shared_state.set_commit_term(last_commit_ent.term);

        // update group local state without shared
        if self.commit_term != last_commit_ent.term && self.leader.replica_id != 0 {
            self.commit_term = last_commit_ent.term;
        }
        if self.commit_index != last_commit_ent.index && self.leader.replica_id != 0 {
            self.commit_index = last_commit_ent.index;
        }

        self.create_apply(gs, replica_id, entries)
    }

    fn create_apply(
        &mut self,
        gs: &RS,
        replica_id: u64,
        entries: Vec<Entry>,
    ) -> Result<ApplyData<RES>, super::storage::Error> {
        // this is different from `commit_index` and `commit_term` for self local,
        // we need a commit state that has been advanced to the state machine.
        let commit_index = std::cmp::min(
            self.raft_group.raft.raft_log.committed,
            self.raft_group.raft.raft_log.persisted,
        );
        // TODO: handle error: if storage error we need retry instead of panic
        let commit_term = gs.term(commit_index)?;

        let current_term = self.raft_group.raft.term;
        let mut proposals = Vec::new();
        if !self.proposals.is_empty() {
            for entry in entries.iter() {
                trace!(
                    "try find propsal with entry ({}, {}, {:?}) on replica {} in proposals {:?}",
                    entry.index,
                    entry.term,
                    entry.data,
                    replica_id,
                    self.proposals
                );
                match self
                    .proposals
                    .find_proposal(entry.term, entry.index, current_term)
                {
                    None => {
                        trace!(
                            "can't find entry ({}, {}) related proposal on replica {}",
                            entry.index,
                            entry.term,
                            replica_id
                        );
                        continue;
                    }

                    Some(p) => proposals.push(p),
                };
            }
        }

        // trace!("find proposals {:?} on replica {}", proposals, replica_id);

        let entries_size = entries
            .iter()
            .map(|ent| utils::compute_entry_size(ent))
            .sum::<usize>();
        let apply = ApplyData {
            replica_id,
            group_id: self.group_id,
            term: current_term,
            commit_index,
            commit_term,
            entries,
            entries_size,
            proposals,
        };

        // trace!("make apply {:?}", apply);

        Ok(apply)
    }

    fn on_reads_ready(&mut self, rss: Vec<ReadState>) {
        self.read_index_queue.advance_reads(rss);
        while let Some(p) = self.read_index_queue.pop_front() {
            p.tx.map(|tx| tx.send(Ok(p.context.map_or(None, |mut ctx| ctx.context.take()))));
        }
    }

    // Dispatch soft state changed related events.
    async fn handle_soft_state_change<MRS: MultiRaftStorage<RS>>(
        &mut self,
        node_id: u64,
        ss: &SoftState,
        replica_cache: &mut ReplicaCache<RS, MRS>,
        event_bcast: &mut EventChannel,
    ) {
        if ss.leader_id != 0 && ss.leader_id != self.leader.replica_id {
            return self
                .handle_leader_change(node_id, ss, replica_cache, event_bcast)
                .await;
        }
    }

    // Process soft state changed on leader changed
    #[tracing::instrument(
        level = Level::TRACE,
        name = "RaftGroup::handle_leader_change", 
        skip_all
    )]
    async fn handle_leader_change<MRS: MultiRaftStorage<RS>>(
        &mut self,
        node_id: u64,
        ss: &SoftState,
        replica_cache: &mut ReplicaCache<RS, MRS>,
        event_bcast: &mut EventChannel,
    ) {
        let group_id = self.group_id;
        let replica_desc = match replica_cache
            .replica_desc(self.group_id, ss.leader_id)
            .await
        {
            Err(err) => {
                // TODO: handle storage error kind, such as try again
                error!("node {}: group {} replica{} become leader, but got it replica description for node id error {}",
                    node_id, group_id, ss.leader_id, err);
                return;
            }
            Ok(op) => match op {
                Some(desc) => desc,
                None => {
                    // this means that we do not know which node the leader is on,
                    // but this does not affect us to send LeaderElectionEvent, as
                    // this will be fixed by subsequent message communication.
                    // TODO: and asynchronous broadcasting
                    warn!(
                        "replica {} of raft group {} becomes leader, but  node id is not known",
                        ss.leader_id, group_id
                    );

                    ReplicaDesc {
                        group_id,
                        node_id: NO_NODE,
                        replica_id: ss.leader_id,
                    }
                }
            },
        };

        // update shared states
        self.shared_state.set_leader_id(ss.leader_id);
        self.shared_state.set_role(&ss.raft_state);

        info!(
            "node {}: group = {}, replica = {} became leader",
            node_id, self.group_id, ss.leader_id
        );
        let replica_id = replica_desc.replica_id;
        self.leader = replica_desc; // always set because node_id maybe NO_NODE.

        event_bcast.push(Event::LederElection(LeaderElectionEvent {
            group_id: self.group_id,
            leader_id: ss.leader_id,
            replica_id,
        }));
    }

    #[tracing::instrument(
        level = Level::TRACE,
        name = "RaftGroup::handle_write",
        skip_all,
        fields(node_id=node_id, group_id=self.group_id)
    )]
    pub(crate) async fn handle_write<TR: transport::Transport, MRS: MultiRaftStorage<RS>>(
        &mut self,
        node_id: u64,
        write: &mut RaftGroupWriteRequest,
        gs: &RS, // TODO: cache storage in RaftGroup
        transport: &TR,
        replica_cache: &mut ReplicaCache<RS, MRS>,
        node_manager: &mut NodeManager,
    ) -> Result<Option<ApplyData<RES>>, super::storage::Error> {
        let group_id = self.group_id;
        let mut ready = write.ready.take().unwrap();
        if *ready.snapshot() != Snapshot::default() {
            let snapshot = ready.snapshot().clone();
            debug!("node {}: install snapshot {:?}", node_id, snapshot);
            // FIXME: call add voters to track node, node mgr etc.
            // TODO: consider move install_snapshot to async queues.
            gs.install_snapshot(snapshot)?;
        }

        if !ready.entries().is_empty() {
            let entries = ready.take_entries();
            debug!(
                "node {}: append entries [{}, {}]",
                node_id,
                entries[0].index,
                entries[entries.len() - 1].index
            );

            // If append fails due to temporary storage unavailability,
            // we will try again later.
            gs.append(&entries)?;
        }
        if let Some(hs) = ready.hs() {
            gs.set_hardstate(hs.clone())?
        }

        if !ready.persisted_messages().is_empty() {
            transport::send_messages(
                node_id,
                transport,
                replica_cache,
                node_manager,
                group_id,
                ready.take_persisted_messages(),
            )
            .await;
        }

        let mut light_ready = self.raft_group.advance_append(ready);

        if let Some(commit) = light_ready.commit_index() {
            debug!("node {}: set commit = {}", node_id, commit);
            self.commit_index = commit;
        }

        if !light_ready.messages().is_empty() {
            let messages = light_ready.take_messages();
            transport::send_messages(
                node_id,
                transport,
                replica_cache,
                node_manager,
                group_id,
                messages,
            )
            .await;
        }

        if !light_ready.committed_entries().is_empty() {
            let apply = self.handle_can_apply_entries(
                node_id,
                &gs,
                write.replica_id,
                light_ready.take_committed_entries(),
            )?;
            return Ok(Some(apply));
        }
        Ok(None)
    }

    fn pre_propose_write<WD: ProposeData>(
        &mut self,
        write_data: &WriteRequest<WD, RES>,
    ) -> Result<(), Error> {
        // if write_data.data.is_empty() {
        //     return Err(Error::BadParameter(
        //         "write request data must not be empty".to_owned(),
        //     ));
        // }

        if !self.is_leader() {
            return Err(Error::Propose(ProposeError::NotLeader {
                node_id: self.node_id,
                group_id: self.group_id,
                replica_id: self.replica_id,
            }));
        }

        if write_data.term != 0 && self.term() > write_data.term {
            return Err(Error::Propose(ProposeError::Stale(
                write_data.term,
                self.term(),
            )));
        }

        Ok(())
    }

    pub fn propose_write<WD: ProposeData>(
        &mut self,
        write_request: WriteRequest<WD, RES>,
    ) -> Option<ResponseCallback> {
        if let Err(err) = self.pre_propose_write(&write_request) {
            return Some(ResponseCallbackQueue::new_error_callback(
                write_request.tx,
                err,
            ));
        }

        let term = self.term();
        let data = match flexbuffer_serialize(&write_request.data) {
            Err(err) => {
                return Some(ResponseCallbackQueue::new_error_callback(
                    write_request.tx,
                    err,
                ));
            }
            Ok(mut ser) => ser.take_buffer(),
        };

        // propose to raft group
        let next_index = self.last_index() + 1;
        if let Err(err) = self.raft_group.propose(
            write_request.context.map_or(vec![], |ctx_data| ctx_data),
            data,
        ) {
            return Some(ResponseCallbackQueue::new_error_callback(
                write_request.tx,
                Error::Raft(err),
            ));
        }

        let index = self.last_index() + 1;
        if next_index == index {
            return Some(ResponseCallbackQueue::new_error_callback(
                write_request.tx,
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
            is_conf_change: false,
            tx: Some(write_request.tx),
        };

        self.proposals.push(proposal);
        None
    }

    pub fn read_index_propose(&mut self, data: ReadIndexData) -> Option<ResponseCallback> {
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
            let (ctx, cc) = to_cc(request.data, request.context);
            assert_ne!(ctx.len(), 0);
            self.raft_group.propose_conf_change(ctx, cc)
        } else {
            let (ctx, cc) = to_ccv2(request.data, request.context);
            assert_ne!(ctx.len(), 0);
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

    pub(crate) fn advance_apply(&mut self, result: &ApplyResultMessage) {
        // keep  invariant
        assert!(result.applied_index <= self.commit_index);

        self.raft_group.advance_apply_to(result.applied_index);

        // update local apply state
        self.applied_index = result.applied_index;
        self.applied_term = result.applied_term;

        // update shared state for apply
        self.shared_state.set_applied_index(result.applied_index);
        self.shared_state.set_applied_term(result.applied_term);
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
