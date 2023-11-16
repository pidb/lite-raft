use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::error::Error;
use crate::msg::ApplyConfChange;
use crate::prelude::ConfChangeType;
use crate::prelude::ConfState;
use crate::prelude::ReplicaDesc;
use crate::replica_cache::ReplicaCache;
use crate::storage::MultiRaftStorage;
use crate::storage::RaftStorage;
use crate::ProposeResponse;

use super::group::RaftGroup;
use super::NodeManager;

pub(super) struct ApplyConfDelegate<'a, RS, MRS, RES>
where
    RS: RaftStorage,
    MRS: MultiRaftStorage<RS>,
    RES: ProposeResponse,
{
    node_id: u64,
    group: &'a mut RaftGroup<RS, RES>,
    group_storage: &'a RS,
    node_manager: &'a mut NodeManager,
    replica_cache: &'a mut ReplicaCache<RS, MRS>,
}

impl<'a, RS, MRS, RES> ApplyConfDelegate<'a, RS, MRS, RES>
where
    RS: RaftStorage,
    MRS: MultiRaftStorage<RS>,
    RES: ProposeResponse,
{
    pub(super) fn new(
        node_id: u64,
        group: &'a mut RaftGroup<RS, RES>,
        group_storage: &'a RS,
        node_manager: &'a mut NodeManager,
        replica_cache: &'a mut ReplicaCache<RS, MRS>,
    ) -> Self {
        Self {
            node_id,
            group,
            group_storage,
            node_manager,
            replica_cache,
        }
    }

    pub(super) async fn handle(&mut self, mut view: ApplyConfChange) -> Result<ConfState, Error>
    where
        RS: RaftStorage,
        MRS: MultiRaftStorage<RS>,
        RES: ProposeResponse,
    {
        if view.change_request.is_none() && view.conf_change.leave_joint() {
            return self.apply(&view).await;
        }

        let group_id = self.group.group_id;

        // The leader communicates with the new member after the membership change,
        // sends the snapshot contains the member configuration, and then follower
        // install snapshot.
        // The Append Entries are then received by followers and committed, but the
        // new member configuration is already applied to the followers when the
        // snapshot is installed. In raft-rs, duplicate joint consensus is not allowed,
        // so we catch the error and skip.
        let conf = self.group.raft_group.raft.prs().conf().to_conf_state();
        if view.conf_change.enter_joint().is_some() && !conf.voters_outgoing.is_empty() {
            info!(
                "node {}: for group {} replica {} skip alreay entered joint conf_change {:?}",
                self.node_id, group_id, self.group.replica_id, view.conf_change
            );
            return Ok(conf);
        }

        let conf = self.apply(&view).await?;

        self.post_apply(&mut view).await;
        return Ok(conf);
    }

    async fn apply(&mut self, view: &ApplyConfChange) -> Result<ConfState, Error>
    where
        RS: RaftStorage,
        RES: ProposeResponse,
    {
        let group_id = view.group_id;

        let conf_state = match self.group.raft_group.apply_conf_change(&view.conf_change) {
            Err(err) => {
                error!(
                    "node {}: commit membership change error: group = {}, err = {}",
                    self.node_id, group_id, err,
                );
                return Err(Error::Raft(err));
            }
            Ok(conf_state) => conf_state,
        };

        self.group_storage.set_confstate(conf_state.clone())?;
        debug!(
            "node {}: applied conf_state {:?} for group {} replica{}",
            self.node_id, conf_state, group_id, self.group.replica_id
        );
        return Ok(conf_state);
    }

    async fn post_apply(&mut self, view: &mut ApplyConfChange)
    where
        RS: RaftStorage,
        MRS: MultiRaftStorage<RS>,
        RES: ProposeResponse,
    {
        let changes = view.change_request.take().unwrap().changes;
        assert_eq!(changes.len(), view.conf_change.changes.len());

        for (conf_change, change_request) in view.conf_change.changes.iter().zip(changes.iter()) {
            match conf_change.change_type() {
                ConfChangeType::AddNode => {
                    self.add_replica(change_request.node_id, change_request.replica_id)
                        .await
                }

                ConfChangeType::RemoveNode => {
                    self.remove_replica(change_request.node_id, change_request.replica_id)
                        .await
                }
                ConfChangeType::AddLearnerNode => unimplemented!(),
            }
        }
    }

    async fn add_replica(&mut self, change_node_id: u64, change_replica_id: u64)
    where
        RS: RaftStorage,
        MRS: MultiRaftStorage<RS>,
        RES: ProposeResponse,
    {
        let group_id = self.group.group_id;
        self.node_manager.add_group(change_node_id, group_id);

        // TODO: this call need transfer to user call, and if user call return errored,
        // the membership change should failed and user need to retry.
        // we need a channel to provider user notify actor it need call these code.
        // and we recv the notify can executing these code, if executed failed, we
        // response to user and membership change is failed.
        if let Err(err) = self
            .replica_cache
            .cache_replica_desc(
                group_id,
                ReplicaDesc {
                    group_id,
                    node_id: change_node_id,
                    replica_id: change_replica_id,
                },
                true,
            )
            .await
        {
            warn!(
                "node {}: the membership change request to add replica desc error: {} ",
                self.node_id, err
            );
        }

        self.group.add_track_node(change_node_id);
    }

    async fn remove_replica(&mut self, changed_node_id: u64, changed_replica_id: u64)
    where
        RS: RaftStorage,
        MRS: MultiRaftStorage<RS>,
        RES: ProposeResponse,
    {
        let group_id = self.group.group_id;
        let _ = self.group.remove_pending_proposals();
        self.group.remove_track_node(changed_node_id);
        // TODO: think remove if node has empty group_map.
        let _ = self.node_manager.remove_group(changed_node_id, group_id);

        if let Err(err) = self
            .replica_cache
            .remove_replica_desc(
                group_id,
                ReplicaDesc {
                    group_id,
                    node_id: changed_node_id,
                    replica_id: changed_replica_id,
                },
                true,
            )
            .await
        {
            warn!(
                "node {}: the membership change request to add replica desc error: {} ",
                self.node_id, err
            );
        }
    }
}
