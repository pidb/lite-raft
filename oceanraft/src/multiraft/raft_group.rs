use std::collections::HashMap;
use std::collections::VecDeque;

use prost::Message;
use raft::prelude::Entry;
use raft::LightReady;
use raft::RawNode;
use raft::Ready;
use raft::SoftState;
use raft::StateRole;
use raft::Storage;
use tokio::sync::oneshot;

use raft_proto::prelude::AppReadIndexRequest;
use raft_proto::prelude::AppWriteRequest;
use raft_proto::prelude::ConfState;
use raft_proto::prelude::HardState;
use raft_proto::prelude::ReplicaDesc;

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
        name = "RaftGroup::on_ready",
        skip_all,
        fields(node_id=node_id, group_id=self.group_id)
    )]
    pub(crate) async fn on_ready<TR: transport::Transport, MRS: MultiRaftStorage<RS>>(
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
        if !self.raft_group.has_ready() {
            return;
        }

        let group_id = self.group_id;
        let mut rd = self.raft_group.ready();

        // we need to know which replica in raft group is ready.
        let replica_desc = match replica_cache.replica_for_node(group_id, node_id).await {
            Err(err) => {
                error!(
                    "write is error, got {} group replica  of storage error {}",
                    group_id, err
                );
                return;
            }
            Ok(replica_desc) => match replica_desc {
                Some(replica_desc) => replica_desc,
                None => {
                    // if we can't look up the replica in storage, but the group is ready,
                    // we know that one of the replicas must be ready, so we can repair the
                    // storage to store this replica.
                    let replica_id = self.raft_group.raft.id;
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
            self.insert_apply_task(
                &gs,
                replica_desc.replica_id,
                rd.take_committed_entries(),
                multi_groups_apply,
            );
        }

        // there are dispatch soft state if changed.
        if let Some(ss) = rd.ss() {
            self.on_soft_state_change(ss, replica_cache, pending_events)
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

    fn insert_apply_task(
        &mut self,
        gs: &RS,
        replica_id: u64,
        entries: Vec<Entry>,
        multi_groups_apply: &mut HashMap<u64, ApplyTask>,
    ) {
        let group_id = self.group_id;
        debug!(
            "replica ({}) of raft group ({}) commit entries {:?}",
            replica_id, group_id, entries
        );
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

    pub fn create_apply(&mut self, gs: &RS, replica_id: u64, entries: Vec<Entry>) -> Apply {
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
    #[tracing::instrument(
        level = Level::TRACE,
        name = "RaftGroup::on_soft_state_change", 
        skip(self, replica_cache, pending_events)
    )]
    async fn on_soft_state_change<MRS: MultiRaftStorage<RS>>(
        &mut self,
        ss: &SoftState,
        replica_cache: &mut ReplicaCache<RS, MRS>,
        pending_events: &mut Vec<Event>,
    ) {
        // if leader change
        if ss.leader_id != 0 && ss.leader_id != self.leader.replica_id {
            return self
                .on_leader_change(ss, replica_cache, pending_events)
                .await;
        }
    }

    // Process soft state changed on leader changed
    #[tracing::instrument(
        level = Level::TRACE,
        name = "RaftGroup::on_leader_change", 
        skip_all
    )]
    async fn on_leader_change<MRS: MultiRaftStorage<RS>>(
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
            "replica ({}) of raft group ({}) becomes leader",
            ss.leader_id,
            self.group_id
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

    pub async fn do_write<TR: transport::Transport, MRS: MultiRaftStorage<RS>>(
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
        if *ready.snapshot() != raft::prelude::Snapshot::default() {
            let snapshot = ready.snapshot().clone();
            // TODO: add apply snapshot
            // if let Err(_error) = gs.apply_snapshot(snapshot) {}
        }

        if !ready.entries().is_empty() {
            let entries = ready.take_entries();
            if let Err(_error) = gs.append_entries(&entries) {}
        }

        if let Some(hs) = ready.hs() {
            let hs = hs.clone();
            if let Err(_error) = gs.set_hardstate(hs) {}
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

    pub async fn do_write_finish<TR: transport::Transport, MRS: MultiRaftStorage<RS>>(
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
            // TODO: cache storage in related raft group.
            let gs = match storage
                .group_storage(group_id, gwr.replica_id)
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
            self.insert_apply_task(
                &gs,
                replica_id,
                light_ready.take_committed_entries(),
                multi_groups_apply,
            );
        }
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
    ) {
        if let Err(err) = self.pre_propose_write(&request) {
            tx.send(Err(err)).unwrap();
            return;
        }
        let term = self.term();

        // propose to raft group
        let next_index = self.last_index() + 1;
        if let Err(err) = self.raft_group.propose(request.context, request.data) {
            tx.send(Err(Error::Proposal(ProposalError::Other(Box::new(err)))))
                .unwrap();
            return;
        }

        let index = self.last_index() + 1;
        if next_index == index {
            tx.send(Err(Error::Proposal(ProposalError::Unexpected(index))))
                .unwrap();
            return;
        }

        let proposal = Proposal {
            index: next_index,
            term,
            is_conf_change: false,
            tx: Some(tx),
        };

        self.proposals.push(proposal).unwrap();
    }

    pub fn read_index_propose(
        &mut self,
        request: AppReadIndexRequest,
        tx: oneshot::Sender<Result<(), Error>>,
    ) {
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
    }
}
