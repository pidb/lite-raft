use raft::Storage;
use tracing::error;
use tracing::trace;
use tracing::Level;

use raft_proto::prelude::Message;
use raft_proto::prelude::MessageType;
use raft_proto::prelude::MultiRaftMessage;

use super::error::Error;
use super::node::NodeManager;
use super::replica_cache::ReplicaCache;
use super::storage::MultiRaftStorage;

pub trait Transport: Send + Sync + 'static {
    fn send(&self, msg: MultiRaftMessage) -> Result<(), Error>;
}

/// Call `Transport` to send the messages.
pub async fn send_messages<TR, RS, MRS>(
    from_node_id: u64,
    transport: &TR,
    replica_cache: &mut ReplicaCache<RS, MRS>,
    node_mgr: &mut NodeManager,
    group_id: u64,
    msgs: Vec<Message>,
) where
    TR: Transport,
    RS: Storage,
    MRS: MultiRaftStorage<RS>,
{
    assert_ne!(from_node_id, 0);
    for msg in msgs {
        // if the context in the heartbeat message is not empty,
        // the read index heartbeat confirmation is being performed
        // and we cannot skip the message.
        let skip = match msg.msg_type() {
            MessageType::MsgHeartbeat => {
                if msg.context.is_empty() {
                    trace!(
                        "node {}: drop group = {}, {} -> {} individual heartbeat",
                        from_node_id,
                        group_id,
                        msg.from,
                        msg.to
                    );
                    true
                } else {
                    false
                }
            }

            MessageType::MsgHeartbeatResponse => {
                if msg.context.is_empty() {
                    trace!(
                        "node {}: drop group = {}, {} -> {} individual heartbeat response",
                        from_node_id,
                        group_id,
                        msg.from,
                        msg.to
                    );
                    true
                } else {
                    false
                }
            }
            _ => false,
        };

        if !skip {
            send_message(
                from_node_id,
                transport,
                replica_cache,
                node_mgr,
                group_id,
                msg,
            )
            .await
        }
    }
}

#[tracing::instrument(
    level = Level::TRACE,
    name = "transport::send_message",
    skip_all,
)]
async fn send_message<TR, RS, MRS>(
    from_node_id: u64,
    transport: &TR,
    replica_cache: &mut ReplicaCache<RS, MRS>,
    node_mgr: &mut NodeManager,
    group_id: u64,
    msg: Message,
) where
    TR: Transport,
    RS: Storage,
    MRS: MultiRaftStorage<RS>,
{
    // if we lose information about the target replica, we may not be able to
    // send any messages other than heartbeats, which may cause the raft group
    // quorum to be temporarily unavailable.
    //
    // but this is rare, and if it does happen, it is fixed later by
    // synchronization (TODO: unimpl)
    let to_replica = match replica_cache.replica_desc(group_id, msg.to).await {
        Err(err) => {
            error!(
                "node {}: from = {}, to = {} send {:?} to group failed, find to replica_desc error: {}",
                from_node_id, msg.from, msg.to, msg.msg_type(), err
            );
            return;
        }
        Ok(op) => match op {
            None => {
                error!(
                    "node {}: from = {}, to = {} send {:?} to group failed, to replica_desc not found",
                    from_node_id, msg.from, msg.to, msg.msg_type(),
                );
                return;
            }
            Some(data) => data,
        },
    };
    assert_ne!(to_replica.node_id, 0);

    trace!(
        "node {}: send raft msg to node {}: msg_type = {:?}, group = {}, from = {}, to = {}",
        from_node_id,
        to_replica.node_id,
        msg.msg_type(),
        group_id,
        msg.from,
        msg.to
    );
    if !node_mgr.contains_node(&to_replica.node_id) {
        node_mgr.add_group(to_replica.node_id, group_id);
    }

    let msg = MultiRaftMessage {
        group_id,
        from_node: from_node_id,
        to_node: to_replica.node_id,
        replicas: vec![],
        msg: Some(msg),
    };

    // FIXME: send trait should be return original msg when error occurred.
    if let Err(err) = transport.send(msg) {
        error!(
            "node {}: send raft msg to node {} error: group = {}, err = {:?}",
            from_node_id, to_replica.node_id, group_id, err
        );
    }
}


mod local;

pub use local::LocalTransport;