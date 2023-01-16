use futures::Future;

use raft::Storage;
use tracing::error;
use tracing::trace;
use tracing::warn;
use tracing::Level;

use raft_proto::prelude::Message;
use raft_proto::prelude::MessageType;
use raft_proto::prelude::RaftMessage;
use raft_proto::prelude::RaftMessageResponse;

use super::error::Error;
use super::node::NodeManager;
use super::replica_cache::ReplicaCache;
use super::storage::MultiRaftStorage;

pub trait RaftMessageDispatch: Send + Sync + 'static {
    type DispatchFuture<'life0>: Future<Output = Result<RaftMessageResponse, Error>> + Send
    where
        Self: 'life0;

    fn dispatch<'life0>(&'life0 self, msg: RaftMessage) -> Self::DispatchFuture<'life0>;
}

pub trait Transport: Send + Sync + 'static {
    fn send(&self, msg: RaftMessage) -> Result<(), Error>;
}

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
        match msg.msg_type() {
            MessageType::MsgHeartbeat => {
                trace!(
                    "node({}) group({}) replica({}) drop individual heartbeat message to replica({})",
                    from_node_id, group_id, msg.from, msg.to
                );
            }
            MessageType::MsgHeartbeatResponse => {
                trace!(
                    "node({}) group({}) replica({}) drop individual heartbeat message to replica({})",
                    from_node_id, group_id, msg.from, msg.to
                );
            }
            _ => {
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
}

#[tracing::instrument(
    level = Level::TRACE,
    name = "transport::send_message",
    skip(replica_cache, transport, node_mgr, group_id)
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
            warn!(
                "find to node replica description error {} for send {:?}",
                err, msg
            );
            return;
        }
        Ok(op) => match op {
            None => {
                warn!(
                    "can not find to node replica description for send {:?}",
                    msg
                );
                return;
            }
            Some(data) => data,
        },
    };
    assert_ne!(to_replica.node_id, 0);

    if !node_mgr.contains_node(&to_replica.node_id) {
        node_mgr.add_node(to_replica.node_id, group_id);
    }

    let msg = RaftMessage {
        group_id,
        from_node: from_node_id,
        to_node: to_replica.node_id,
        replicas: vec![],
        msg: Some(msg),
    };

    // FIXME: send trait should be return original msg when error occurred.
    if let Err(err) = transport.send(msg) {
        error!("call transport error {} for send msg", err);
    }
}
