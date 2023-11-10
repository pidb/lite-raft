use raft::prelude::Entry;
use raft::ReadState;
use raft::Ready;
use raft::SoftState;

use crate::msg::ApplyData;
use crate::multiraft::ProposeResponse;
use crate::prelude::ReplicaDesc;
use crate::prelude::Snapshot;
use crate::transport;
use crate::utils;
use crate::LeaderElectionEvent;

use super::actor::Inner;

use crate::error::Error;
use crate::error::RaftGroupError;
use crate::event::Event;
use crate::multiraft::NO_NODE;
use crate::storage::MultiRaftStorage;
use crate::storage::RaftStorage;
use crate::transport::Transport;
use crate::ProposeRequest;

#[derive(Default, Debug)]
pub(super) struct GroupWriteRequest {
    pub replica_id: u64,
    pub ready: Option<Ready>,
}

impl<TR, RS, MRS, REQ, RES> Inner<TR, RS, MRS, REQ, RES>
where
    TR: Transport + Clone,
    RS: RaftStorage,
    MRS: MultiRaftStorage<RS>,
    REQ: ProposeRequest,
    RES: ProposeResponse,
{
    pub(super) async fn handle_group_ready(
        &mut self,
        group_id: u64,
    ) -> Result<(Option<GroupWriteRequest>, Option<ApplyData<RES>>), Error> {
        let group = match self.groups.get_mut(&group_id) {
            None => {
                return Err(Error::RaftGroup(RaftGroupError::NotExist(
                    self.node_id,
                    group_id,
                )));
            }
            Some(group) => group,
        };

        if !group.has_ready() {
            return Ok((None, None));
        }

        let node_id = self.node_id;
        let group_id = group.group_id();
        let replica_id = group.replica_id();

        // we need to know which replica in raft group is ready.
        let replica_desc = self
            .replica_cache
            .replica_for_node(group_id, self.node_id)
            .await?;
        let replica_desc = match replica_desc {
            Some(replica_desc) => {
                assert_eq!(replica_desc.replica_id, replica_id);
                replica_desc
            }
            None => {
                // if we can't look up the replica in storage, but the group is ready,
                // we know that one of the replicas must be ready, so we can repair the
                // storage to store this replica.
                let repaired_replica_desc = ReplicaDesc {
                    group_id,
                    node_id,
                    replica_id,
                };

                self.replica_cache
                    .cache_replica_desc(group_id, repaired_replica_desc.clone(), true)
                    .await?;
                repaired_replica_desc
            }
        };

        let mut rd = group.ready();

        // send out messages
        if !rd.messages().is_empty() {
            transport::send_messages(
                node_id,
                &mut self.transport,
                &mut self.replica_cache,
                &mut self.node_manager,
                group_id,
                rd.take_messages(),
            )
            .await;
        }

        if let Some(ss) = rd.ss() {
            // TODO: using trait call
            self.handle_soft_state_change(group_id, ss).await;
        }

        if !rd.read_states().is_empty() {
            self.handle_read_index(group_id, rd.take_read_states())
        }

        // make apply task if need to apply commit entries
        let apply = if !rd.committed_entries().is_empty() {
            // insert_commit_entries will update latest commit term by commit entries.
            let apply = self
                .handle_apply_entries(
                    group_id,
                    replica_desc.replica_id,
                    rd.take_committed_entries(),
                )
                .await?;

            Some(apply)
        } else {
            None
        };

        let gwr = Some(GroupWriteRequest {
            replica_id: replica_desc.replica_id,
            ready: Some(rd),
        });
        // make write task if need to write disk.
        Ok((gwr, apply))
    }

    // Dispatch soft state changed related events.
    async fn handle_soft_state_change(&mut self, group_id: u64, ss: &SoftState) {
        let group = self.groups.get_mut(&group_id).unwrap();
        let current_leader = group.leader();
        if ss.leader_id != 0 && ss.leader_id != current_leader.replica_id {
            return self.handle_leader_change(group_id, ss).await;
        }
    }

    // Process soft state changed on leader changed
    async fn handle_leader_change(&mut self, group_id: u64, ss: &SoftState) {
        let group = self.groups.get_mut(&group_id).unwrap();
        let node_id = self.node_id;
        let group_id = group.group_id();
        let replica_id = group.replica_id();

        // cache leader replica desc
        let leader_replica_desc = match self
            .replica_cache
            .replica_desc(group_id, ss.leader_id)
            .await
        {
            Err(err) => {
                // TODO: handle storage error kind, such as try again
                tracing::error!("node {}: group {} replica{} become leader, but got it replica description for node id error {}",
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
                    tracing::warn!(
                        "replica {} of raft group {} becomes leader, but  node id is not known",
                        ss.leader_id,
                        group_id
                    );

                    ReplicaDesc {
                        group_id,
                        node_id: NO_NODE,
                        replica_id: ss.leader_id,
                    }
                }
            },
        };

        // update group storage metadata to save current leader id
        let mut gs_meta = self
            .storage
            .get_group_metadata(group_id, replica_id)
            .await
            .unwrap() // TODO: handle error
            .expect("why missing group_storage metadata");
        if gs_meta.leader_id != ss.leader_id {
            gs_meta.leader_id = ss.leader_id;
            self.storage.set_group_metadata(gs_meta).await.unwrap(); // TODO handle error
        }

        // update shared states
        // TODO: move to inner of RaftGroup
        let replica_id = leader_replica_desc.replica_id;
        group.shared_state.set_leader_id(ss.leader_id);
        group.shared_state.set_role(&ss.raft_state);
        group.set_leader(leader_replica_desc); // always set because node_id maybe NO_NODE.

        tracing::info!(
            "node {}: group = {}, replica = {} became leader",
            node_id,
            group_id,
            ss.leader_id
        );

        self.event_chan
            .push(Event::LederElection(LeaderElectionEvent {
                group_id,
                leader_id: ss.leader_id,
                replica_id,
            }));
    }

    async fn handle_apply_entries(
        &mut self,
        group_id: u64,
        replica_id: u64,
        entries: Vec<Entry>,
    ) -> Result<ApplyData<RES>, crate::storage::Error> {
        let group = self.groups.get_mut(&group_id).unwrap();
        let group_id = group.group_id();
        tracing::debug!(
            "node {}: create apply entries [{}, {}], group = {}, replica = {}",
            self.node_id,
            entries[0].index,
            entries[entries.len() - 1].index,
            group_id,
            replica_id
        );

        // update shared_state for latest commit
        let last_commit_ent = &entries[entries.len() - 1];
        group.shared_state.set_commit_index(last_commit_ent.index);
        group.shared_state.set_commit_term(last_commit_ent.term);

        // update group local state without shared
        let group_leader = group.leader();
        let groupo_commit_term = group.commit_term();
        let group_commit_index = group.commit_index();
        if groupo_commit_term != last_commit_ent.term && group_leader.replica_id != 0 {
            group.set_commit_term(last_commit_ent.term);
        }
        if group_commit_index != last_commit_ent.index && group_leader.replica_id != 0 {
            group.set_commit_index(last_commit_ent.index);
        }

        self.make_apply(group_id, replica_id, entries).await
    }

    async fn make_apply(
        &mut self,
        group_id: u64,
        replica_id: u64,
        entries: Vec<Entry>,
    ) -> Result<ApplyData<RES>, crate::storage::Error> {
        let group = self.groups.get_mut(&group_id).unwrap();
        // TODO: cache storage in related raft group.
        let gs = self
            .storage
            .group_storage(group_id, replica_id)
            .await
            .unwrap();

        // this is different from `commit_index` and `commit_term` for self local,
        // we need a commit state that has been advanced to the state machine.
        let raft_log = group.raft_log();
        let commit_index = std::cmp::min(raft_log.committed, raft_log.persisted);
        let commit_term = gs.term(commit_index)?;
        let current_term = group.term();

        let proposal_queue = &mut group.proposals;
        let apply_proposals = entries
            .iter()
            .map(|ent| {
                // tracing::trace!(
                //     "try find propsal with entry ({}, {}, {:?}) on replica {} in proposals {:?}",
                //     entry.index,
                //     entry.term,
                //     entry.data,
                //     replica_id,
                //     self.proposals
                // );

                // FIXME: O(N)
                match proposal_queue.find_proposal(ent.term, ent.index, current_term) {
                    None => {
                        tracing::warn!(
                            "can't find entry ({}, {}) related proposal on replica {}",
                            ent.index,
                            ent.term,
                            replica_id
                        );
                        None
                    }

                    Some(p) => Some(p),
                }
            })
            .filter(|p| p.is_some())
            .map(|p| p.unwrap())
            .collect::<Vec<_>>();

        // trace!("find proposals {:?} on replica {}", proposals, replica_id);

        let entries_size = entries
            .iter()
            .map(|ent| utils::compute_entry_size(ent))
            .sum::<usize>();

        Ok(ApplyData {
            replica_id,
            group_id: group.group_id(),
            term: current_term,
            commit_index,
            commit_term,
            entries,
            entries_size,
            proposals: apply_proposals,
        })
    }

    fn handle_read_index(&mut self, group_id: u64, rss: Vec<ReadState>) {
        let group = self.groups.get_mut(&group_id).unwrap();
        group.read_index_queue.advance_reads(rss);
        while let Some(p) = group.read_index_queue.pop_front() {
            p.tx.map(|tx| tx.send(Ok(p.context.map_or(None, |mut ctx| ctx.context.take()))));
        }
    }

    pub(super) async fn handle_write(
        &mut self,
        group_id: u64,
        // group: &mut RaftGroup<RS, RES>,
        write: &mut GroupWriteRequest,
        // gs: &RS, // TODO: cache storage in RaftGroup
    ) -> Result<Option<ApplyData<RES>>, crate::storage::Error> {
        let gs = match self.storage.group_storage(group_id, write.replica_id).await {
            Ok(gs) => gs,
            Err(err) => match err {
                crate::storage::Error::StorageTemporarilyUnavailable => {
                    tracing::warn!(
                        "node {}: group {} handle_write but storage temporarily unavailable ",
                        self.node_id,
                        group_id
                    );

                    self.active_groups.insert(group_id);
                    return Err(err);
                }
                crate::storage::Error::StorageUnavailable => {
                    panic!("node {}: storage unavailable", self.node_id)
                }
                _ => {
                    tracing::warn!(
                        "node {}: get raft storage for group {} to handle_writes error: {}",
                        self.node_id,
                        group_id,
                        err
                    );
                    return Ok(None);
                }
            },
        };

        let group = match self.groups.get_mut(&group_id) {
            Some(group) => group,
            None => {
                // TODO: remove pending proposals related to this group
                // If the group does not exist at this point
                // 1. we may have finished sending messages to the group, role changed notifications,
                //    committable entires commits
                // 2. we may not have completed the new proposal append, there may be multiple scenarios
                //     - The current group is the leader, sent AE, but was deleted before it received a
                //       response from the follower, so it did not complete the append drop
                //     - The current group is the follower, which does not affect the completion of the
                //       AE
                tracing::error!(
                    "node {}: handle group-{} write ready, but dropped",
                    self.node_id,
                    group_id
                );
                return Ok(None);
            }
        };

        let group_id = group.group_id;
        let node_id = self.node_id;
        let mut ready = write.ready.take().unwrap();
        if *ready.snapshot() != Snapshot::default() {
            let snapshot = ready.snapshot().clone();
            tracing::debug!(
                "node {}: group {} start install snapshot",
                node_id,
                group_id
            );
            // FIXME: call add voters to track node, node mgr etc.
            // TODO: consider move install_snapshot to async queues.
            gs.install_snapshot(snapshot)?;
            tracing::debug!("node {}: group {} install snapshot done", node_id, group_id);
        }

        if !ready.entries().is_empty() {
            let entries = ready.take_entries();
            let (start, end) = (entries[0].index, entries[entries.len() - 1].index);
            tracing::debug!(
                "node {}: group {} start append entries [{}, {}]",
                node_id,
                group_id,
                start,
                end
            );

            // If append fails due to temporary storage unavailability,
            // we will try again later.
            gs.append(&entries)?;
            tracing::debug!(
                "node {}: group {} append entries [{}, {}] done",
                node_id,
                group_id,
                start,
                end
            );
        }

        if let Some(hs) = ready.hs() {
            gs.set_hardstate(hs.clone())?
        }

        if !ready.persisted_messages().is_empty() {
            transport::send_messages(
                node_id,
                &mut self.transport,
                &mut self.replica_cache,
                &mut self.node_manager,
                group_id,
                ready.take_persisted_messages(),
            )
            .await;
        }

        let mut light_ready = group.raft_group.advance_append(ready);

        if let Some(commit) = light_ready.commit_index() {
            tracing::debug!("node {}: set commit = {}", node_id, commit);
            group.set_commit_index(commit);
            gs.set_hardstate_commit(commit)?;
            group.shared_state.set_commit_index(commit);
        }

        if !light_ready.messages().is_empty() {
            let messages = light_ready.take_messages();
            transport::send_messages(
                node_id,
                &mut self.transport,
                &mut self.replica_cache,
                &mut self.node_manager,
                group_id,
                messages,
            )
            .await;
        }

        if !light_ready.committed_entries().is_empty() {
            let apply = self
                .handle_apply_entries(
                    group_id,
                    // group,
                    // &gs,
                    write.replica_id,
                    light_ready.take_committed_entries(),
                )
                .await?;
            return Ok(Some(apply));
        }
        Ok(None)
    }
}
