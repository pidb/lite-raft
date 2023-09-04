// use std::cmp;
// use std::collections::hash_map::HashMap;
// use std::collections::hash_map::Iter;
// use std::collections::HashSet;
// use std::collections::VecDeque;
// use std::sync::atomic::AtomicBool;
// use std::sync::Arc;
// use std::time::Duration;

// use raft::prelude::ConfState;
// use raft::StateRole;
// use tokio::sync::mpsc::channel;
// use tokio::sync::mpsc::unbounded_channel;
// use tokio::sync::mpsc::Receiver;
// use tokio::sync::mpsc::Sender;
// use tokio::sync::mpsc::UnboundedReceiver;
// use tokio::sync::mpsc::UnboundedSender;
// use tokio::sync::oneshot;
// use tracing::debug;
// use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;
// use tracing::Level;
// use tracing::Span;

use crate::multiraft::ProposeResponse;
// use crate::multiraft::NO_LEADER;
// use crate::prelude::ConfChangeType;
// use crate::prelude::GroupMetadata;
use crate::prelude::Message;
use crate::prelude::MessageType;
use crate::prelude::MultiRaftMessage;
use crate::prelude::MultiRaftMessageResponse;
// use crate::prelude::ReplicaDesc;

// use super::apply::ApplyActor;
// use super::config::Config;
// use super::error::ChannelError;
use super::error::Error;
// use super::error::RaftGroupError;
// use super::event::Event;
// use super::event::EventChannel;
// use super::group::RaftGroup;
// use super::group::RaftGroupWriteRequest;
// use super::group::Status;
// use super::msg::ApplyCommitMessage;
// use super::msg::ApplyData;
// use super::msg::ApplyMessage;
// use super::msg::ApplyResultMessage;
// use super::msg::CommitMembership;
// use super::msg::ManageMessage;
// use super::msg::ProposeMessage;
// use super::msg::QueryGroup;
use super::multiraft::NO_GORUP;
// use super::multiraft::NO_NODE;
use super::node::NodeWorker;
// use super::proposal::ProposalQueue;
// use super::proposal::ReadIndexQueue;
// use super::replica_cache::ReplicaCache;
// use super::rsm::StateMachine;
// use super::state::GroupState;
// use super::state::GroupStates;
use super::storage::MultiRaftStorage;
use super::storage::RaftStorage;
// use super::tick::Ticker;
use super::transport::Transport;
use super::ProposeRequest;

impl<TR, RS, MRS, WD, RES> NodeWorker<TR, RS, MRS, WD, RES>
where
    TR: Transport + Clone,
    RS: RaftStorage,
    MRS: MultiRaftStorage<RS>,
    WD: ProposeRequest,
    RES: ProposeResponse,
{
    /// The node sends heartbeats to other nodes instead
    /// of all raft groups on that node.
    pub(crate) fn merge_heartbeats(&self) {
        for (to_node, _) in self.node_manager.iter() {
            if *to_node == self.node_id {
                continue;
            }

            // coalesced heartbeat to all nodes. the heartbeat message is node
            // level message so from and to set 0 when sending, and the specific
            // value is set by message receiver.
            let mut raft_msg = Message::default();
            raft_msg.set_msg_type(MessageType::MsgHeartbeat);
            if let Err(err) = self.transport.send(MultiRaftMessage {
                group_id: NO_GORUP,
                from_node: self.node_id,
                to_node: *to_node,
                replicas: vec![],
                msg: Some(raft_msg),
            }) {
                tracing::error!(
                    "node {}: send heartbeat to {} error: {}",
                    self.node_id,
                    *to_node,
                    err
                )
            }
        }
    }

    /// Fanout heartbeats from other nodes to all raft groups on this node.
    pub(crate) async fn fanout_heartbeat(
        &mut self,
        msg: MultiRaftMessage,
    ) -> Result<MultiRaftMessageResponse, Error> {
        let from_node_id = msg.from_node;
        let to_node_id = msg.to_node;
        let mut fanouted_groups = 0;
        let mut fanouted_followers = 0;
        if let Some(from_node) = self.node_manager.get_node(&from_node_id) {
            for (group_id, _) in from_node.group_map.iter() {
                let group = match self.groups.get_mut(group_id) {
                    None => {
                        warn!("node {}: from node {} failed to fanout to group {} because does not exists", self.node_id, from_node_id, *group_id);
                        continue;
                    }
                    Some(group) => group,
                };

                fanouted_groups += 1;
                self.active_groups.insert(*group_id);

                if group.leader.node_id != from_node_id || msg.from_node == self.node_id {
                    continue;
                }

                if group.is_leader() {
                    warn!("node {}: received a heartbeat from the leader node {}, but replica {} of group {} also leader and there may have been a network partition", self.node_id, from_node_id, *group_id, group.replica_id);
                    continue;
                }

                // gets the replica stored in this node.
                let from_replica = match self
                    .replica_cache
                    .replica_for_node(*group_id, from_node_id)
                    .await
                {
                    Err(err) => {
                        warn!(
                            "find replcia in group {} on from_node {} in current node {} error: {}",
                            group_id, msg.from_node, self.node_id, err
                        );
                        continue;
                    }
                    Ok(val) => match val {
                        None => {
                            // warn!("the current node {} that look up replcia not found in group {} on from_node {}", self.node_id, group_id, msg.from_node);
                            continue;
                        }
                        Some(val) => val,
                    },
                };

                // FIXME: t30_membership single_step
                let to_replica = match self
                    .replica_cache
                    .replica_for_node(*group_id, to_node_id)
                    .await
                {
                    Err(err) => {
                        warn!(
                            "find replcia in group {} on to_node {} in current node {} error: {}",
                            group_id, msg.to_node, self.node_id, err
                        );
                        continue;
                    }
                    Ok(val) => match val {
                        None => {
                            // warn!("the current node {} that look up replcia not found in group {} on to_node {}", self.node_id, group_id, msg.to_node);
                            continue;
                        }
                        Some(val) => val,
                    },
                };

                fanouted_followers += 1;

                let mut step_msg = raft::prelude::Message::default();
                step_msg.set_msg_type(raft::prelude::MessageType::MsgHeartbeat);
                // FIX(test command)
                //
                // Although the heatbeat is not set without affecting correctness, but liveness
                // maybe cannot satisty. such as in test code 1) submit some commands 2) and
                // then wait apply and perform a heartbeat. but due to a heartbeat cannot set commit, so
                // no propose lead to test failed.
                // step_msg.commit = group.raft_group.raft.raft_log.committed;
                // step_msg.term = group.raft_group.raft.term; // FIX(t30_membership::test_remove)
                step_msg.from = from_replica.replica_id;
                step_msg.to = to_replica.replica_id;
                if group.is_candidate() || group.is_pre_candidate() {
                    info!("node {}: replica({}) of group({}) became candidate, the heartbeat message is not received by the leader({}) from node({})",
                         self.node_id,
                         group.replica_id,
                         *group_id,
                         group.leader.replica_id,
                         group.leader.node_id,
                     );
                    step_msg.term = group.raft_group.raft.term;
                }

                if let Err(err) = group.raft_group.step(step_msg) {
                    warn!(
                        "node {}: step heatbeat message error: {}",
                        self.node_id, err
                    );
                }
            }
        } else {
            // In this point, we receive heartbeat message from other nodes,
            // but we don't have the from_node raft group information, so we
            // don't know which raft group's replica we should fanout to.
            warn!(
                "missing raft groups at from_node {} fanout heartbeat",
                msg.from_node
            );
            self.node_manager.add_node(msg.from_node);
        }

        trace!(
            "node {}: fanouted {} groups and followers {}",
            self.node_id,
            fanouted_groups,
            fanouted_followers
        );

        let response_msg = {
            let mut raft_msg = Message::default();
            raft_msg.set_msg_type(MessageType::MsgHeartbeatResponse);
            MultiRaftMessage {
                group_id: NO_GORUP,
                from_node: self.node_id,
                to_node: from_node_id,
                replicas: vec![],
                msg: Some(raft_msg),
            }
        };

        let _ = self.transport.send(response_msg)?;
        Ok(MultiRaftMessageResponse {})
    }

    /// Fanout heartbeats response from other nodes to all raft groups on this node.
    pub(crate) async fn fanout_heartbeat_response(
        &mut self,
        msg: MultiRaftMessage,
    ) -> Result<MultiRaftMessageResponse, Error> {
        if let Some(node) = self.node_manager.get_node(&msg.from_node) {
            for (group_id, _) in node.group_map.iter() {
                let group = match self.groups.get_mut(group_id) {
                    None => {
                        warn!("node {}: from node {} failed to fanout response to group {} because does not exists", self.node_id, msg.from_node, *group_id);
                        continue;
                    }
                    Some(group) => group,
                };

                self.active_groups.insert(*group_id);

                if group.leader.node_id != self.node_id || msg.from_node == self.node_id {
                    continue;
                }

                // gets the replica stored in this node.
                let from_replica = match self
                    .storage
                    .replica_for_node(*group_id, msg.from_node)
                    .await
                {
                    Err(err) => {
                        warn!(
                            "find replcia in group {} on from_node {} in current node {} error: {}",
                            group_id, msg.from_node, self.node_id, err
                        );
                        continue;
                    }
                    Ok(val) => match val {
                        None => {
                            // warn!("the current node {} that look up replcia not found in group {} on from_node {}", self.node_id, group_id, msg.from_node);
                            continue;
                        }
                        Some(val) => val,
                    },
                };

                let to_replica = match self.storage.replica_for_node(*group_id, msg.to_node).await {
                    Err(err) => {
                        warn!(
                            "find replcia in group {} on to_node {} in current node {} error: {}",
                            group_id, msg.to_node, self.node_id, err
                        );
                        continue;
                    }
                    Ok(val) => match val {
                        None => {
                            // warn!("the current node {} that look up replcia not found in group {} on to_node {}", self.node_id, group_id, msg.to_node);
                            continue;
                        }
                        Some(val) => val,
                    },
                };

                let mut msg = raft::prelude::Message::default();
                msg.set_msg_type(raft::prelude::MessageType::MsgHeartbeatResponse);
                // msg.term = group.term();
                msg.from = from_replica.replica_id;
                msg.to = to_replica.replica_id;
                if let Err(err) = group.raft_group.step(msg) {
                    warn!(
                        "node {}: step heatbeat response message error: {}",
                        self.node_id, err
                    );
                }
            }
        } else {
            warn!(
                "missing raft groups at from_node {} fanout heartbeat response",
                msg.from_node
            );
            self.node_manager.add_node(msg.from_node);
        }
        Ok(MultiRaftMessageResponse {})
    }
}
