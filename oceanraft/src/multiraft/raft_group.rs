use std::collections::HashMap;
use std::collections::VecDeque;

use futures::future::OptionFuture;
use prost::Message;
use raft::prelude::Entry;
use raft::prelude::MembershipChangeRequest;
use raft::LightReady;
use raft::RawNode;
use raft::Ready;
use raft::SoftState;
use raft::StateRole;
use raft::Storage;
use raft_proto::ConfChangeI;
use tokio::sync::oneshot;

use raft_proto::prelude::AppReadIndexRequest;
use raft_proto::prelude::AppWriteRequest;
use raft_proto::prelude::ConfChange;
use raft_proto::prelude::ConfChangeSingle;
use raft_proto::prelude::ConfChangeV2;
use raft_proto::prelude::ConfState;
use raft_proto::prelude::HardState;
use raft_proto::prelude::ReplicaDesc;
use raft_proto::prelude::Snapshot;

use tracing::debug;
use tracing::error;
use tracing::trace;
use tracing::warn;
use tracing::Level;

use super::apply_actor::Apply;
use super::apply_actor::ApplyTask;
use super::error::Error;
use super::error::ProposalError;
use super::event::LeaderElectionEvent;
use super::multiraft::NO_NODE;
use super::multiraft_actor::new_response_error_callback;
use super::multiraft_actor::ResponseCb;
use super::node::NodeManager;
use super::proposal::GroupProposalQueue;
use super::proposal::Proposal;
use super::proposal::ReadIndexProposal;
use super::replica_cache::ReplicaCache;
use super::storage::MultiRaftStorage;
use super::transport;
use super::util;
use super::Event;

#[derive(Debug, Default, Clone, PartialEq)]
pub struct RaftGroupApplyState {
    pub commit_index: u64,
    pub commit_term: u64,
    pub applied_term: u64,
    pub applied_index: u64,
}

#[derive(Debug, Default, PartialEq)]
pub struct RaftGroupState {
    pub group_id: u64,
    pub replica_id: u64,
    // pub hard_state: HardState,
    pub soft_state: SoftState,
    pub membership_state: ConfState,
    pub apply_state: RaftGroupApplyState,
}

#[derive(Default, Debug)]
pub struct RaftGroupWriteRequest {
    pub replica_id: u64,
    pub ready: Option<Ready>,
    pub light_ready: Option<LightReady>,
}

/// Represents a replica of a raft group.
pub struct RaftGroup<RS: Storage> {
    pub group_id: u64,
    pub replica_id: u64,
    pub raft_group: RawNode<RS>,
    // track the nodes which members ofq the raft consensus group
    pub node_ids: Vec<u64>,
    pub proposals: GroupProposalQueue,
    pub leader: ReplicaDesc,
    pub committed_term: u64,
    pub state: RaftGroupState,

    pub batch_apply: bool,
    pub batch_size: usize,
    pub pending_apply: Option<Apply>,
}

impl<RS> RaftGroup<RS>
where
    RS: Storage,
{
    #[inline]
    pub fn is_leader(&self) -> bool {
        self.raft_group.raft.state == StateRole::Leader
    }

    #[inline]
    pub fn term(&self) -> u64 {
        self.raft_group.raft.term
    }

    #[inline]
    pub fn committed_term(&self) -> u64 {
        self.committed_term
    }

    #[inline]
    pub fn last_index(&self) -> u64 {
        self.raft_group.raft.raft_log.last_index()
    }

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
        multi_groups_write: &mut HashMap<u64, RaftGroupWriteRequest>,
        multi_groups_apply: &mut HashMap<u64, ApplyTask>,
        pending_events: &mut Vec<Event>,
    ) {
        let group_id = self.group_id;
        let mut rd = self.raft_group.ready();

        // we need to know which replica in raft group is ready.
        let replica_desc = match replica_cache.replica_for_node(group_id, node_id).await {
            Err(err) => {
                error!(
                    "node {}: write is error, got {} group replica  of storage error {}",
                    node_id, group_id, err
                );
                return;
            }
            Ok(replica_desc) => match replica_desc {
                Some(replica_desc) => {
                    assert_eq!(replica_desc.replica_id, self.raft_group.raft.id);
                    replica_desc
                }
                None => {
                    // if we can't look up the replica in storage, but the group is ready,
                    // we know that one of the replicas must be ready, so we can repair the
                    // storage to store this replica.
                    let repaired_replica_desc = ReplicaDesc {
                        node_id,
                        replica_id: self.raft_group.raft.id,
                    };
                    if let Err(err) = replica_cache
                        .cache_replica_desc(group_id, repaired_replica_desc.clone(), true)
                        .await
                    {
                        error!(
                            "write is error, got {} group replica  of storage error {}",
                            group_id, err
                        );
                        return;
                    }
                    repaired_replica_desc
                }
            },
        };

        // TODO: cache storage in related raft group.
        let gs = match storage
            .group_storage(group_id, replica_desc.replica_id)
            .await
        {
            Ok(gs) => gs,
            Err(err) => {
                error!(
                    "node({}) group({}) ready but got group storage error {}",
                    node_id, group_id, err
                );
                return;
            }
        };

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

        // make apply task if need to apply commit entries
        if !rd.committed_entries().is_empty() {
            // insert_commit_entries will update latest commit term by commit entries.
            self.handle_committed_entries(
                &gs,
                replica_desc.replica_id,
                rd.take_committed_entries(),
                multi_groups_apply,
            ).await;
        }

        if let Some(ss) = rd.ss() {
            self.handle_soft_state_change(ss, replica_cache, pending_events)
                .await;
        }

        // make write task if need to write disk.
        multi_groups_write.insert(
            group_id,
            RaftGroupWriteRequest {
                replica_id: replica_desc.replica_id,
                ready: Some(rd),
                light_ready: None,
            },
        );
    }

   
    #[tracing::instrument(
        level = Level::TRACE,
        name = "RaftGroup:handle_committed_entries", 
        skip_all
    )]
    async fn handle_committed_entries(
        &mut self,
        gs: &RS,
        replica_id: u64,
        entries: Vec<Entry>,
        multi_groups_apply: &mut HashMap<u64, ApplyTask>,
    ) {
        trace!(
            "node , committed entries [{}, {}], group = {}, replica = {}",
            entries[0].index,
            entries[entries.len() - 1].index,
            self.group_id,
            replica_id
        );
        let group_id = self.group_id;
        let last_term = entries[entries.len() - 1].term;
        self.maybe_update_committed_term(last_term);

        let apply = self.create_apply(gs, replica_id, entries);
        multi_groups_apply.insert(group_id, ApplyTask::Apply(apply));
    }

    /// Update the term of the latest entries committed during
    /// the term of the leader.
    #[inline]
    fn maybe_update_committed_term(&mut self, term: u64) {
        if self.committed_term != term && self.leader.replica_id != 0 {
            self.committed_term = term
        }
    }

    fn create_apply(&mut self, gs: &RS, replica_id: u64, entries: Vec<Entry>) -> Apply {
        let current_term = self.raft_group.raft.term;
        // TODO: min(persistent, committed)
        let commit_index = self.raft_group.raft.raft_log.committed;
        let commit_term = gs.term(commit_index).unwrap();
        let mut proposals = VecDeque::new();
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
                    Err(err) => {
                        // FIXME: don't panic
                        panic!("find proposal error {}", err);
                    }
                    Ok(proposal) => match proposal {
                        None => {
                            trace!(
                                "can't find entry ({}, {}) related proposal on replica {}",
                                entry.index,
                                entry.term,
                                replica_id
                            );
                            continue;
                        }

                        Some(p) => proposals.push_back(p),
                    },
                };
            }
        }

        trace!("find proposals {:?} on replica {}", proposals, replica_id);

        let entries_size = entries
            .iter()
            .map(|ent| util::compute_entry_size(ent))
            .sum::<usize>();
        let apply = Apply {
            replica_id,
            group_id: self.group_id,
            term: current_term,
            commit_index,
            commit_term,
            entries,
            entries_size,
            proposals,
        };

        trace!("make apply {:?}", apply);

        apply
    }

    // Dispatch soft state changed related events.
    async fn handle_soft_state_change<MRS: MultiRaftStorage<RS>>(
        &mut self,
        ss: &SoftState,
        replica_cache: &mut ReplicaCache<RS, MRS>,
        pending_events: &mut Vec<Event>,
    ) {
        // if leader change
        if ss.leader_id != 0 && ss.leader_id != self.leader.replica_id {
            return self
                .handle_leader_change(ss, replica_cache, pending_events)
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
        ss: &SoftState,
        replica_cache: &mut ReplicaCache<RS, MRS>,
        pending_events: &mut Vec<Event>,
    ) {
        let group_id = self.group_id;
        let replica_desc = match replica_cache
            .replica_desc(self.group_id, ss.leader_id)
            .await
        {
            Err(err) => {
                error!("group({}) replica({}) become leader, but got it replica description for node id error {}",
            group_id, ss.leader_id, err);
                // FIXME: it maybe temporary error, retry or use NO_NODE.
                return;
            }
            Ok(op) => op,
        };

        let replica_desc = match replica_desc {
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
                    node_id: NO_NODE,
                    replica_id: ss.leader_id,
                }
            }
        };

        trace!(
            "group = {}, replica = {} becomes leader",
            self.group_id,
            ss.leader_id
        );
        let replica_id = replica_desc.replica_id;
        self.leader = replica_desc; // always set because node_id maybe NO_NODE.
        pending_events.push(Event::LederElection(LeaderElectionEvent {
            group_id: self.group_id,
            leader_id: ss.leader_id,
            replica_id,
            committed_term: self.committed_term,
        }));
    }

    #[tracing::instrument(
        level = Level::TRACE,
        name = "RaftGroup::handle_write",
        skip_all,
        fields(node_id=node_id, group_id=self.group_id)
    )]
    pub(crate) async fn handle_ready_write<TR: transport::Transport, MRS: MultiRaftStorage<RS>>(
        &mut self,
        node_id: u64,
        gwr: &mut RaftGroupWriteRequest,
        gs: &RS,
        transport: &TR,
        replica_cache: &mut ReplicaCache<RS, MRS>,
        node_manager: &mut NodeManager,
    ) {
        let group_id = self.group_id;
        // TODO: cache storage in RaftGroup

        let mut ready = gwr.ready.take().unwrap();
        if *ready.snapshot() != Snapshot::default() {
            let snapshot = ready.snapshot().clone();
            warn!("TODO: should apply snapshit = {:?}", snapshot);
            // TODO: add apply snapshot
            // 当添加新的 replica 时, 需要同步成员变更信息
            gs.apply_snapshot(snapshot).unwrap();
        }

        if !ready.entries().is_empty() {
            let entries = ready.take_entries();
            if let Err(_error) = gs.append_entries(&entries) {
                panic!("node {}: append entries error = {}", node_id, _error);
            }
        }

        if let Some(hs) = ready.hs() {
            let hs = hs.clone();
            if let Err(_error) = gs.set_hardstate(hs) {
                panic!("node {}: set hardstate error = {}", node_id, _error);
            }
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

        let light_ready = self.raft_group.advance(ready);
        gwr.light_ready = Some(light_ready);
    }

    #[tracing::instrument(
        level = Level::TRACE,
        name = "RaftGroup::handle_light_ready",
        skip_all,
        fields(node_id=node_id, group_id=self.group_id)
    )]
    pub async fn handle_light_ready<TR: transport::Transport, MRS: MultiRaftStorage<RS>>(
        &mut self,
        node_id: u64,
        transport: &TR,
        storage: &MRS,
        replica_cache: &mut ReplicaCache<RS, MRS>,
        node_manager: &mut NodeManager,
        gwr: &mut RaftGroupWriteRequest,
        multi_groups_apply: &mut HashMap<u64, ApplyTask>,
    ) {
        let group_id = self.group_id;
        let replica_id = gwr.replica_id;
        let mut light_ready = gwr.light_ready.take().unwrap();
        // let group_storage = self
        //     .storage
        //     .group_storage(group_id, gwr.replica_id)
        //     .await
        //     .unwrap();

        if let Some(commit) = light_ready.commit_index() {
            debug!("node {}: set commit = {}", node_id, commit);
            // group_storage.set_commit(commit);
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
            debug!("node {}: light ready has committed entries", node_id);
            // TODO: cache storage in related raft group.
            let gs = match storage.group_storage(group_id, gwr.replica_id).await {
                Ok(gs) => gs,
                Err(err) => {
                    error!(
                        "node({}) group({}) ready but got group storage error {}",
                        node_id, group_id, err
                    );
                    return;
                }
            };
            self.handle_committed_entries(
                &gs,
                replica_id,
                light_ready.take_committed_entries(),
                multi_groups_apply,
            ).await;
        }
        // FIXME: always advance apply
        // TODO: move to upper layer
        tracing::info!(
            "node {}: committed = {}",
            node_id,
            self.raft_group.raft.raft_log.committed
        );
        // self.raft_group.advance_apply();
    }

    fn pre_propose_write(&mut self, request: &AppWriteRequest) -> Result<(), Error>
    where
        RS: Storage,
    {
        if request.data.is_empty() {
            return Err(Error::BadParameter(format!("write request data is empty")));
        }

        if !self.is_leader() {
            return Err(Error::Proposal(ProposalError::NotLeader(
                self.group_id,
                self.replica_id,
                self.raft_group.raft.leader_id,
            )));
        }

        if request.term != 0 && self.term() > request.term {
            return Err(Error::Proposal(ProposalError::Stale(request.term)));
        }

        Ok(())
    }

    pub fn propose_write(
        &mut self,
        request: AppWriteRequest,
        tx: oneshot::Sender<Result<(), Error>>,
    ) -> Option<ResponseCb> {
        if let Err(err) = self.pre_propose_write(&request) {
            return Some(new_response_error_callback(tx, err));
            // tx.send(Err(err)).unwrap();
            // return;
        }
        let term = self.term();

        // propose to raft group
        let next_index = self.last_index() + 1;
        if let Err(err) = self.raft_group.propose(request.context, request.data) {
            return Some(new_response_error_callback(tx, Error::Raft(err)));
            //tx.send(Err(Error::Proposal(ProposalError::Other(Box::new(err)))))
            //    .unwrap();
            //return;
        }

        let index = self.last_index() + 1;
        if next_index == index {
            return Some(new_response_error_callback(
                tx,
                Error::Proposal(ProposalError::Unexpected(index)),
            ));
            // tx.send(Err(Error::Proposal(ProposalError::Unexpected(index))))
            //     .unwrap();
            // return;
        }

        let proposal = Proposal {
            index: next_index,
            term,
            is_conf_change: false,
            tx: Some(tx),
        };

        // FIXME: should return error ResponseCb
        self.proposals.push(proposal).unwrap();
        None
    }

    pub fn read_index_propose(
        &mut self,
        request: AppReadIndexRequest,
        tx: oneshot::Sender<Result<(), Error>>,
    ) -> Option<ResponseCb> {
        let uuid = uuid::Uuid::new_v4();
        let term = self.term();
        let read_context = match request.context {
            None => vec![],
            Some(ctx) => ctx.encode_length_delimited_to_vec(),
        };

        self.raft_group.read_index(read_context);

        let proposal = ReadIndexProposal {
            uuid,
            read_index: None,
            context: None,
            tx: Some(tx),
        };

        None
    }

    pub fn propose_membership_change(
        &mut self,
        req: MembershipChangeRequest,
        tx: oneshot::Sender<Result<(), Error>>,
    ) -> Option<ResponseCb> {
        // TODO: add pre propose check
        let term = self.term();

        // propose to raft group
        let next_index = self.last_index() + 1;

        let res = if req.changes.len() == 1 {
            let (ctx, cc) = to_cc(&req);
            self.raft_group.propose_conf_change(ctx, cc)
        } else {
            let (ctx, cc) = to_ccv2(&req);
            self.raft_group.propose_conf_change(ctx, cc)
        };

        if let Err(err) = res {
            error!("propose membership change error = {}", err);
            return Some(new_response_error_callback(tx, Error::Raft(err)));
        }

        let index = self.last_index() + 1;
        if next_index == index {
            error!("propose_conf_change index invalid");
            return Some(new_response_error_callback(
                tx,
                Error::Proposal(ProposalError::Unexpected(index)),
            ));
        }

        let proposal = Proposal {
            index: next_index,
            term,
            is_conf_change: true,
            tx: Some(tx),
        };

        // FIXME: should return error ResponseCb
        self.proposals.push(proposal).unwrap();
        None
    }
}

fn to_cc(req: &MembershipChangeRequest) -> (Vec<u8>, ConfChange) {
    assert_eq!(req.changes.len(), 1);
    let mut cc = ConfChange::default();
    cc.set_change_type(req.changes[0].change_type());
    // TODO: set membership change id
    cc.node_id = req.changes[0].replica_id;
    (req.encode_to_vec(), cc)
}

fn to_ccv2(req: &MembershipChangeRequest) -> (Vec<u8>, ConfChangeV2) {
    assert!(req.changes.len() > 1);
    let mut cc = ConfChangeV2::default();
    let mut sc = vec![];
    for change in req.changes.iter() {
        sc.push(ConfChangeSingle {
            change_type: change.change_type,
            node_id: change.replica_id,
        });
    }

    // TODO: consider setting transaction type
    cc.set_changes(sc);
    (req.encode_to_vec(), cc)
}
