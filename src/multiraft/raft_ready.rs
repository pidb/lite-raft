use tracing::error;

use raft::prelude::Ready;
use raft::LightReady;

use std::collections::hash_map::HashMap;
use std::collections::hash_set::HashSet;
use std::collections::VecDeque;
use std::marker::PhantomData;

use crate::proto::transmute_raft_entries;
use crate::proto::transmute_raft_entries_ref;
use crate::proto::transmute_raft_hard_state;
use crate::proto::transmute_raft_messages;
use crate::proto::transmute_raft_snapshot;
use crate::proto::Entry;
use crate::storage::MultiRaftStorage;
use crate::storage::RaftStorage;

use super::apply::Apply;
use super::apply::ApplyActorAddress;
use super::apply::ApplyTask;
use super::apply::ApplyTaskRequest;
use super::multiraft::RaftGroup;
use super::multiraft::NO_GORUP;
use super::node::NodeManager;
use super::transport;
use super::transport::MessageInterface;
use super::transport::Transport;

#[derive(Default, Debug)]
pub struct GroupWriteRequest {
    replica_id: u64,
    ready: Option<Ready>,
    light_ready: Option<LightReady>,
}
pub(crate) struct WriteReadyHandler<'life0, MI, TR, RS, MRS>
where
    MI: MessageInterface,
    TR: Transport<MI>,
    RS: RaftStorage,
    MRS: MultiRaftStorage<RS>,
{
    pub(crate) node_id: u64,
    pub(crate) groups: &'life0 mut HashMap<u64, RaftGroup<RS>>,
    pub(crate) node_mgr: &'life0 mut NodeManager,
    pub(crate) storage: &'life0 MRS,
    pub(crate) transport: &'life0 TR,
    pub(crate) apply_actor_address: &'life0 ApplyActorAddress,
    pub(crate) _m: PhantomData<MI>,
}

impl<'life0, MI, TR, RS, MRS> WriteReadyHandler<'life0, MI, TR, RS, MRS>
where
    MI: MessageInterface,
    TR: Transport<MI>,
    RS: RaftStorage,
    MRS: MultiRaftStorage<RS>,
{
    pub(crate) async fn on_groups_ready(&mut self, activity_groups: &HashSet<u64>) {
        // let mut ready_groups = HashMap::new();
        let mut ready_write_groups = HashMap::new();
        let mut apply_task_groups = HashMap::new();
        for group_id in activity_groups.iter() {
            if *group_id == NO_GORUP {
                continue;
            }

            // let group = match self.groups.get_mut(group_id) {
            //     None => {
            //         warn!(
            //             "write is bad, {} group dropped in {} node",
            //             group_id, self.node_id
            //         );
            //         continue;
            //     }
            //     Some(group) => group,
            // };

            if let Some(group) = self.groups.get_mut(group_id) {
                if !group.raft_group.has_ready() {
                    continue;
                }

                let mut group_ready = group.raft_group.ready();

                // we need to know which replica in raft group is ready.
                let replica_id = match self.storage.replica_in_node(*group_id, self.node_id).await {
                    Err(error) => {
                        error!(
                            "write is error, got {} group replica  of storage error {}",
                            group_id, error
                        );
                        continue;
                    }
                    Ok(replica) => match replica {
                        Some(replica_id) => replica_id,
                        None => {
                            // if we can't look up the replica in storage, but the group is ready,
                            // we know that one of the replicas must be ready, so we can repair the
                            // storage to store this replica.
                            let replica_id = group.raft_group.raft.id;
                            // TODO: store replica id
                            replica_id
                        }
                    },
                };

                // send out messages
                if !group_ready.messages().is_empty() {
                    transport::send_messages(
                        self.node_id,
                        self.storage,
                        self.transport,
                        &mut self.node_mgr,
                        *group_id,
                        transmute_raft_messages(group_ready.take_messages()),
                    )
                    .await;
                }

                // make apply task if need to apply commit entries
                if !group_ready.committed_entries().is_empty() {
                    let entries = transmute_raft_entries(group_ready.take_committed_entries());
                    let apply = WriteReadyHandler::<'life0, MI, TR, RS, MRS>::create_apply(
                        replica_id, group, entries,
                    );

                    apply_task_groups.insert(*group_id, ApplyTask::Apply(apply));
                }

                // make write task if need to write disk.
                ready_write_groups.insert(
                    *group_id,
                    GroupWriteRequest {
                        replica_id,
                        ready: Some(group_ready),
                        light_ready: None,
                    },
                );
            }
        }

        if !apply_task_groups.is_empty() {
            self.apply_actor_address
                .tx
                .send(ApplyTaskRequest {
                    groups: apply_task_groups,
                })
                .await;
        }

        let gwrs = self.handle_write(ready_write_groups).await;
        self.handle_write_finish(gwrs).await;
    }

    async fn handle_write(
        &mut self,
        mut ready_write_groups: HashMap<u64, GroupWriteRequest>,
    ) -> HashMap<u64, GroupWriteRequest> {
        // TODO(yuanchang.xu) Disk write flow control
        // let mut light_readys = HashMap::new();
        for (group_id, group_write_request) in ready_write_groups.iter_mut() {
            let group = self.groups.get_mut(&group_id).unwrap();
            let gs = match self
                .storage
                .group_storage(*group_id, group_write_request.replica_id)
                .await
            {
                Ok(gs) => match gs {
                    Some(gs) => gs,
                    None => {
                        continue;
                    }
                },
                Err(error) => {
                    continue;
                }
            };

            let mut ready = group_write_request.ready.take().unwrap();
            if *ready.snapshot() != raft::prelude::Snapshot::default() {
                let snapshot = ready.snapshot().clone();
                if let Err(_error) = gs.apply_snapshot(transmute_raft_snapshot(snapshot)) {}
            }

            if !ready.entries().is_empty() {
                let entries = ready.take_entries();
                if let Err(_error) = gs.append_entries(transmute_raft_entries_ref(&entries)) {}
            }

            if let Some(hs) = ready.hs() {
                let hs = transmute_raft_hard_state(hs.clone());
                if let Err(_error) = gs.set_hardstate(hs) {}
            }

            if !ready.persisted_messages().is_empty() {
                let persistent_msgs = ready.take_persisted_messages();
                transport::send_messages(
                    self.node_id,
                    self.storage,
                    self.transport,
                    &mut self.node_mgr,
                    *group_id,
                    transmute_raft_messages(persistent_msgs),
                )
                .await;
            }

            let light_ready = group.raft_group.advance(ready);
            group_write_request.light_ready = Some(light_ready);
        }

        ready_write_groups
    }

    async fn handle_write_finish(&mut self, ready_groups: HashMap<u64, GroupWriteRequest>) {
        let mut apply_task_groups = HashMap::new();
        for (group_id, mut gwr) in ready_groups.into_iter() {
            let mut_group = match self.groups.get_mut(&group_id) {
                None => continue,
                Some(g) => g,
            };

            let mut light_ready = gwr.light_ready.take().unwrap();
            let group_storage = self
                .storage
                .group_storage(group_id, gwr.replica_id)
                .await
                .unwrap()
                .unwrap();

            if let Some(commit) = light_ready.commit_index() {
                group_storage.set_commit(commit);
            }

            if !light_ready.messages().is_empty() {
                let messages = light_ready.take_messages();
                transport::send_messages(
                    self.node_id,
                    self.storage,
                    self.transport,
                    &mut self.node_mgr,
                    group_id,
                    transmute_raft_messages(messages),
                )
                .await;
            }

            if !light_ready.committed_entries().is_empty() {
                let group = self.groups.get_mut(&group_id).unwrap();

                let entries = transmute_raft_entries(light_ready.take_committed_entries());
                let apply = WriteReadyHandler::<'life0, MI, TR, RS, MRS>::create_apply(
                    gwr.replica_id,
                    group,
                    entries,
                );

                apply_task_groups.insert(group_id, ApplyTask::Apply(apply));
            }
        }

        if !apply_task_groups.is_empty() {
            self.apply_actor_address
                .tx
                .send(ApplyTaskRequest {
                    groups: apply_task_groups,
                })
                .await;
        }
    }

    fn create_apply(replica_id: u64, group: &mut RaftGroup<RS>, entries: Vec<Entry>) -> Apply {
        let current_term = group.raft_group.raft.term;
        let commit_index = group.raft_group.raft.raft_log.committed;
        let proposals = VecDeque::new();
        if !proposals.is_empty() {
            for entry in entries.iter() {
                match group
                    .proposals
                    .find_proposal(entry.term, entry.index, current_term)
                {
                    Err(error) => {
                        continue;
                    }
                    Ok(proposal) => match proposal {
                        None => continue,

                        Some(p) => proposals.push_back(p),
                    },
                };
            }
        }

        let entries_size = entries
            .iter()
            .map(|ent| ent.compute_size() as usize)
            .sum::<usize>();
        Apply {
            replica_id,
            group_id: group.group_id,
            term: current_term,
            commit_index,
            commit_term: 0, // TODO: get commit term
            entries,
            entries_size,
            proposals,
        }
    }
}
