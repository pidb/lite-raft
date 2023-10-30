use std::cmp;
use std::sync::Arc;

use raft::StateRole;
use tokio::sync::oneshot;

use crate::msg::MembershipRequest;
use crate::msg::ReadIndexRequest;
use crate::msg::WriteRequest;
use crate::multiraft::ProposeResponse;
use crate::multiraft::NO_LEADER;
use crate::prelude::GroupMetadata;
use crate::prelude::MultiRaftMessage;
use crate::prelude::MultiRaftMessageResponse;
use crate::prelude::ReplicaDesc;
use crate::protos::CreateGroupRequest;
use crate::protos::RemoveGroupRequest;

use super::error::Error;
use super::error::RaftGroupError;
use super::event::Event;
use super::group::RaftGroup;
use super::group::Status;
use super::multiraft::NO_NODE;
use super::node::NodeWorker;
use super::node::ResponseCallback;
use super::node::ResponseCallbackQueue;
use super::proposal::ProposalQueue;
use super::proposal::ReadIndexQueue;
use super::state::GroupState;
use super::storage::MultiRaftStorage;
use super::storage::RaftStorage;
use super::transport::Transport;
use super::ProposeRequest;

impl<TR, RS, MRS, REQ, RES> NodeWorker<TR, RS, MRS, REQ, RES>
where
    TR: Transport + Clone,
    RS: RaftStorage,
    MRS: MultiRaftStorage<RS>,
    REQ: ProposeRequest,
    RES: ProposeResponse,
{
    pub(super) fn handle_write_msg(&mut self, msg: WriteRequest<REQ, RES>) {
        let group_id = msg.group_id;
        if let Some(group) = self.groups.get_mut(&group_id) {
            self.active_groups.insert(group_id);
            if let Some(cb) = group.propose_write(msg) {
                self.pending_responses.push_back(cb)
            }
            return;
        }

        tracing::warn!(
            "node {}: proposal write failed, group {} does not exists",
            self.node_id,
            group_id
        );

        let cb = ResponseCallbackQueue::new_error_callback(
            msg.tx,
            Error::RaftGroup(RaftGroupError::Deleted(self.node_id, group_id)),
        );
        self.pending_responses.push_back(cb)
    }

    pub(super) fn handle_readindex_msg(
        &mut self,
        msg: ReadIndexRequest,
    ) -> Option<ResponseCallback> {
        let group_id = msg.group_id;
        match self.groups.get_mut(&group_id) {
            None => {
                tracing::warn!(
                    "node {}: proposal read_index failed, group {} does not exists",
                    self.node_id,
                    group_id,
                );
                return Some(ResponseCallbackQueue::new_error_callback(
                    msg.tx,
                    Error::RaftGroup(RaftGroupError::Deleted(self.node_id, group_id)),
                ));
            }
            Some(group) => {
                self.active_groups.insert(group_id);
                group.read_index_propose(msg)
            }
        }
    }

    pub(super) fn handle_membership_msg(
        &mut self,
        msg: MembershipRequest<RES>,
    ) -> Option<ResponseCallback> {
        let group_id = msg.group_id;
        match self.groups.get_mut(&group_id) {
            None => {
                tracing::warn!(
                    "node {}: proposal membership failed, group {} does not exists",
                    self.node_id,
                    group_id,
                );
                return Some(ResponseCallbackQueue::new_error_callback(
                    msg.tx,
                    Error::RaftGroup(RaftGroupError::Deleted(self.node_id, group_id)),
                ));
            }
            Some(group) => {
                self.active_groups.insert(group_id);
                group.propose_membership_change(msg)
            }
        }
    }

    pub(super) async fn handle_create_group_msg(
        &mut self,
        request: CreateGroupRequest,
        tx: oneshot::Sender<Result<(), Error>>,
    ) -> Option<ResponseCallback> {
        self.active_groups.insert(request.group_id);
        let res = self
            .create_raft_group(
                request.group_id,
                request.replica_id,
                request.replicas,
                Some(request.applied_hint),
                None,
            )
            .await;
        return Some(ResponseCallbackQueue::new_callback(tx, res));
    }

    pub(super) async fn handle_remove_group_msg(
        &mut self,
        request: RemoveGroupRequest,
        tx: oneshot::Sender<Result<(), Error>>,
    ) -> Option<ResponseCallback> {
        // marke delete
        let group_id = request.group_id;
        let group = match self.groups.get_mut(&group_id) {
            None => return Some(ResponseCallbackQueue::new_callback(tx, Ok(()))),
            Some(group) => group,
        };

        for proposal in group.proposals.drain(..) {
            proposal.tx.map(|tx| {
                tx.send(Err(Error::RaftGroup(RaftGroupError::Deleted(
                    self.node_id,
                    group_id,
                ))))
            });
        }

        group.status = Status::Delete;

        let replica_id = group.replica_id;
        match self
            .storage
            .get_group_metadata(group_id, replica_id)
            .await
            .unwrap()
        {
            None => {
                self.storage
                    .set_group_metadata(GroupMetadata {
                        group_id,
                        replica_id,
                        node_id: self.node_id,
                        create_timestamp: 0,
                        leader_id: group.leader.replica_id,
                        deleted: true,
                    })
                    .await
                    .unwrap();
            }
            Some(mut meta) => {
                if !meta.deleted {
                    meta.deleted = true;
                    self.storage.set_group_metadata(meta).await.unwrap();
                }
            }
        }

        // TODO: impl broadcast
        return Some(ResponseCallbackQueue::new_callback(tx, Ok(())));
    }

    #[tracing::instrument(
        name = "NodeActor::handle_raft_message",
        level = tracing::Level::TRACE,
        skip_all,
    )]
    pub(super) async fn handle_raft_message(
        &mut self,
        mut msg: MultiRaftMessage,
    ) -> Result<MultiRaftMessageResponse, Error> {
        println!(
            "node {}: handle_raft_message {:?}",
            self.node_id,
            msg.get_msg().msg_type()
        );
        if !self.groups.contains_key(&msg.group_id) {
            println!(
                "node {}: from group {} not exists",
                self.node_id, msg.group_id
            );
            let msg = msg.clone();
            let raft_msg = msg.msg.as_ref().expect("why message missing raft msg");
            // TODO: if group mark deleted, we need return error
            let _ = self
                .create_raft_group(
                    msg.group_id,
                    raft_msg.to,
                    msg.replicas.clone(),
                    None,
                    Some(msg.clone()),
                )
                .await
                .map_err(|err| {
                    tracing::error!(
                        "node {}: create group for replica {} error {}",
                        self.node_id,
                        raft_msg.to,
                        err
                    );
                    err
                })?;
        }

        let raft_msg = msg
            .msg
            .take()
            .expect("invalid message, raft Message should not be none.");

        let group_id = msg.group_id;
        let from_replica = ReplicaDesc {
            group_id,
            node_id: msg.from_node,
            replica_id: raft_msg.from,
        };
        let to_replica = ReplicaDesc {
            group_id,
            node_id: msg.to_node,
            replica_id: raft_msg.to,
        };

        // processing messages between replicas from other nodes to self node.
        tracing::trace!(
            "node {}: recv msg {:?} from {}: group = {}, from_replica = {}, to_replica = {}",
            self.node_id,
            raft_msg.msg_type(),
            msg.from_node,
            group_id,
            from_replica.replica_id,
            to_replica.replica_id,
        );

        let _ = self
            .replica_cache
            .cache_replica_desc(group_id, from_replica.clone(), self.cfg.replica_sync)
            .await?;

        let _ = self
            .replica_cache
            .cache_replica_desc(group_id, to_replica.clone(), self.cfg.replica_sync)
            .await?;

        if !self.node_manager.contains_node(&from_replica.node_id) {
            self.node_manager.add_group(from_replica.node_id, group_id);
        }

        // if a group exists, try to maintain groups on the node
        // if self.groups.contains_key(&group_id)
        //     && !self.node_manager.contains_node(&to_replica.node_id)
        // {
        //     self.node_manager.add_group(to_replica.node_id, group_id);
        // }

        let group = self
            .groups
            .get_mut(&group_id)
            .expect("unreachable: group always initialize or return error in the previouse code");

        if let Err(err) = group.raft_group.step(raft_msg) {
            tracing::warn!("node {}: step raf message error: {}", self.node_id, err);
        }
        self.active_groups.insert(group_id);
        Ok(MultiRaftMessageResponse {})
    }

    /// # Parameters
    /// - `msg`: If msg is Some, the raft group is initialized with a message
    /// from the leader. If `msg` is the leader msg (such as MsgAppend etc.),
    /// the internal state of the raft replica is initialized for `leader`,
    /// `Node manager`, etc. which allows the replica to `fanout_heartbeat`
    /// messages from the leader node.Without this initialization, the new
    /// raft replica may fail to receive the leader's heartbeat and initiate
    /// a new election distrubed.
    pub(super) async fn create_raft_group(
        &mut self,
        group_id: u64,
        replica_id: u64,
        replicas_desc: Vec<ReplicaDesc>,
        applied_hint: Option<u64>,
        init_msg: Option<MultiRaftMessage>,
    ) -> Result<(), Error> {
        if self.groups.contains_key(&group_id) {
            return Err(Error::RaftGroup(RaftGroupError::Exists(
                self.node_id,
                group_id,
            )));
        }

        if group_id == 0 {
            return Err(Error::BadParameter(
                "group id must be more than 0".to_owned(),
            ));
        }

        if replica_id == 0 {
            return Err(Error::BadParameter(
                "replica id must be more than 0".to_owned(),
            ));
        }

        let group_storage = self.storage.group_storage(group_id, replica_id).await?;
        let rs: raft::RaftState = group_storage
            .initial_state()
            .map_err(|err| Error::Raft(err))?;

        // select a suitable applied index from both storage and initial provided.
        let applied = cmp::max(
            group_storage.get_applied().unwrap_or(0),
            applied_hint.unwrap_or(0),
        );
        let committed_index = rs.hard_state.commit;
        let persisted_index = group_storage.last_index().unwrap();
        if applied > cmp::min(committed_index, persisted_index) {
            panic!(
                "provide hit applied is out of range [applied({}), min (committed({}), persisted({}))]",
                applied, committed_index, persisted_index
            );
        }

        let raft_cfg = raft::Config {
            id: replica_id,
            applied, // TODO: support hint skip
            election_tick: self.cfg.election_tick,
            heartbeat_tick: self.cfg.heartbeat_tick,
            max_size_per_msg: self.cfg.max_size_per_msg,
            max_inflight_msgs: self.cfg.max_inflight_msgs,
            batch_append: self.cfg.batch_append,
            pre_vote: true,
            ..Default::default()
        };
        let raft_store = group_storage.clone();
        let raft_group = raft::RawNode::with_default_logger(&raft_cfg, raft_store)
            .map_err(|err| Error::Raft(err))?;

        tracing::info!(
            "node {}: replica({}) of raft group({}) is created",
            self.node_id,
            group_id,
            replica_id
        );

        let mut leader: ReplicaDesc = ReplicaDesc::default();

        if let Some(init_msg) = init_msg {
            let mut gs_meta = self
                .storage
                .get_group_metadata(group_id, replica_id)
                .await?
                .expect("why missing group_storage metadata");

            let raft_msg = init_msg.msg.as_ref().unwrap();
            //  Persisted leader info of the current replica to prevent
            //  rejecting the leader heartbeat if it does not have the
            //  leader information after the replica restarts.
            if gs_meta.leader_id != raft_msg.from {
                gs_meta.leader_id = raft_msg.from;
                self.storage.set_group_metadata(gs_meta.clone()).await?;
                tracing::info!(
                    "node {}: persisted leader_id({}) to storage for replica({}) of raft group({}) from init msg",
                    self.node_id, raft_msg.from, replica_id, group_id
                );
            }

            // Save the leader and from_node information so that the replica can receive
            // the leader heartbeat after being created
            leader.replica_id = raft_msg.from;
            leader.node_id = init_msg.from_node;
            leader.group_id = init_msg.group_id;
            self.node_manager
                .add_group(init_msg.from_node, init_msg.group_id);
            tracing::info!(
                "node {}: initial leader({:?}) for replica({}) of raft group({}) from init msg",
                self.node_id,
                leader,
                replica_id,
                group_id
            );
        }

        //  initialize shared_state of group
        let shared_state = Arc::new(GroupState::from((
            replica_id,
            rs.hard_state.commit, /* commit_index */
            rs.hard_state.term,   /* commit_term */
            NO_LEADER,
            StateRole::Follower,
        )));
        let mut group = RaftGroup {
            node_id: self.cfg.node_id,
            group_id,
            replica_id,
            raft_group,
            node_ids: Vec::new(),
            proposals: ProposalQueue::new(replica_id),
            leader,
            status: Status::None,
            read_index_queue: ReadIndexQueue::new(),
            shared_state: shared_state.clone(),
            // applied_index: 0,
            // applied_term: 0,
            commit_index: rs.hard_state.commit,
            commit_term: rs.hard_state.term,
        };

        for replica_desc in replicas_desc.iter() {
            self.replica_cache
                .cache_replica_desc(group_id, replica_desc.clone(), true)
                .await?;
            // track the nodes which other members of the raft consensus group
            group.add_track_node(replica_desc.node_id);
            self.node_manager.add_group(replica_desc.node_id, group_id);
        }

        // TODO: check voters and replica_descs consistent

        // if voters are initialized in storage, we need to read
        // the voter from replica_desc to build the data structure
        let voters = rs.conf_state.voters;
        for voter_id in voters.iter() {
            // at this point, we maybe don't know the infomation about
            // the node which replica. this implies two facts:
            // 1. replicas_desc is empty, and the scheduler does not provide
            //    raft group location information
            // 2. replica_desc information corresponding to voter is not initialized
            //    for the storage
            // if so, we initialized these in subsequent way of raft message handler.
            if let Some(replica_desc) = self.replica_cache.replica_desc(group_id, *voter_id).await?
            {
                if replica_desc.node_id == NO_NODE {
                    continue;
                }
                group.add_track_node(replica_desc.node_id);
                self.node_manager.add_group(replica_desc.node_id, group_id);
            }
        }
        self.groups.insert(group_id, group);

        self.event_chan.push(Event::GroupCreate {
            group_id,
            replica_id,
        });

        let prev_shard_state = self.shared_states.insert(group_id, shared_state);

        assert_eq!(
            prev_shard_state.is_none(),
            true,
            "expect group {} shared state is empty, but goted",
            group_id
        );

        Ok(())
    }
}
