use futures::Future;

use raft::Storage;
use tracing::info;

use super::error::Error;
use super::node::NodeManager;

use super::storage::MultiRaftStorage;
use raft_proto::prelude::Message;
use raft_proto::prelude::MessageType;
use raft_proto::prelude::RaftMessage;
use raft_proto::prelude::RaftMessageResponse;

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
    storage: &MRS,
    transport: &TR,
    node_mgr: &mut NodeManager,
    group_id: u64,
    msgs: Vec<Message>,
) where
    TR: Transport,
    RS: Storage,
    MRS: MultiRaftStorage<RS>,
{
    for msg in msgs {
        match msg.msg_type() {
            MessageType::MsgHeartbeat => {
                info!(
                    "node {} drop indvidual heartbeat message to replica {}",
                    from_node_id, msg.to
                );
            }
            MessageType::MsgHeartbeatResponse => {
                info!(
                    "node {} drop indvidual heartbeat response message to replica {}",
                    from_node_id, msg.to
                );
            }
            _ => send_message(storage, transport, node_mgr, group_id, msg).await,
        }
    }
}

pub async fn send_message<TR, RS, MRS>(
    storage: &MRS,
    transport: &TR,
    node_mgr: &mut NodeManager,
    group_id: u64,
    msg: Message,
) where
    TR: Transport,
    RS: Storage,
    MRS: MultiRaftStorage<RS>,
{
    let to_replica = storage
        .replica_desc(group_id, msg.to)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(to_replica.node_id, 0);

    let from_replica = storage
        .replica_desc(group_id, msg.from)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(from_replica.node_id, 0);

    info!("from_node {} -> to_node {} send message {:?}", from_replica.node_id, to_replica.node_id, msg);
    if !node_mgr.contains_node(&to_replica.node_id) {
        node_mgr.add_node(to_replica.node_id, group_id);
    }

    let msg = RaftMessage {
        group_id,
        from_node: from_replica.node_id,
        to_node: to_replica.node_id,
        msg: Some(msg),
    };
    transport.send(msg).unwrap();
}
