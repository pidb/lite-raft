use futures::Future;

use raft::Storage;
use tracing::info;

use super::error::Error;
use super::node::NodeManager;

use raft_proto::prelude::Message;
use raft_proto::prelude::MessageType;
use raft_proto::prelude::RaftMessage;
use raft_proto::prelude::RaftMessageResponse;
use super::storage::MultiRaftStorage;

pub trait MessageInterface: Send + Sync + 'static {
    type RaftMessageFuture<'life0>: Future<Output = Result<RaftMessageResponse, Error>> + Send
    where
        Self: 'life0;

    fn raft_message<'life0>(&'life0 self, msg: RaftMessage) -> Self::RaftMessageFuture<'life0>;
}

pub trait Transport<M>: Send + Sync + 'static
where
    M: MessageInterface,
{
    type ListenFuture<'life0>: Future<Output = Result<(), Error>>
    where
        Self: 'life0;

    fn listen<'life0>(
        &'life0 self,
        node_id: u64,
        addr: &'life0 str,
        msg_impl: M,
    ) -> Self::ListenFuture<'life0>;

    fn send(&self, msg: RaftMessage) -> Result<(), Error>;

    type StopFuture<'life0>: Future<Output = Result<(), Error>>
    where
        Self: 'life0;

    fn stop(&self, node_id: u64) -> Self::StopFuture<'_>;

    // fn close();
}

pub async fn send_messages<MI, TR, RS, MRS>(
    from_node_id: u64,
    storage: &MRS,
    transport: &TR,
    node_mgr: &mut NodeManager,
    group_id: u64,
    msgs: Vec<Message>,
) where
    MI: MessageInterface,
    TR: Transport<MI>,
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

pub async fn send_message<MI, TR, RS, MRS>(
    storage: &MRS,
    transport: &TR,
    node_mgr: &mut NodeManager,
    group_id: u64,
    msg: Message,
) where
    MI: MessageInterface,
    TR: Transport<MI>,
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
