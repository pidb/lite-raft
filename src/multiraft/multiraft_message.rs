use futures::Future;
use tokio::sync::oneshot;

use crate::proto::RaftMessage;
use crate::proto::RaftMessageResponse;

use super::error::Error;
use super::multiraft_actor::MultiRaftActorAddress;
use super::transport::MessageInterface;

#[derive(Clone)]
pub struct MultiRaftMessageSender {
    actor_address: MultiRaftActorAddress,
}

impl MessageInterface for MultiRaftMessageSender {
    type RaftMessageFuture<'life0> = impl Future<Output = Result<RaftMessageResponse, Error>> + Send + 'life0
    where
        Self: 'life0;

    fn raft_message<'life0>(&'life0 self, msg: RaftMessage) -> Self::RaftMessageFuture<'life0> {
        async move {
            let (tx, rx) = oneshot::channel();
            self.actor_address
                .raft_message_tx
                .send((msg, tx))
                .await
                .unwrap();
            rx.await.unwrap()
        }
    }
}
