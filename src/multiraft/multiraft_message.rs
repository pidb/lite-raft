use futures::Future;
use tokio::sync::oneshot;

use crate::proto::RaftMessage;
use crate::proto::RaftMessageResponse;
use crate::proto::RaftGroupManagementMessage;
use crate::proto::RaftGroupManagementMessageType;

use super::error::Error;
use super::multiraft_actor::MultiRaftActorAddress;
use super::transport::MessageInterface;

pub struct MultiRaftMessageSender {
    actor_address: MultiRaftActorAddress,
}


impl Clone for MultiRaftMessageSender {
    fn clone(&self) -> Self {
        Self {
            actor_address: self.actor_address.clone(),
        }
    }
}

impl MultiRaftMessageSender {
      pub async fn initial_raft_group(&self, msg: RaftGroupManagementMessage) -> Result<(), Error> {
        assert_eq!(
            msg.msg_type(),
            RaftGroupManagementMessageType::MsgInitialGroup
        );
        let (tx, rx) = oneshot::channel();
        if let Err(_error) = self.actor_address.manager_group_tx.send((msg, tx)).await {
            panic!("manager group receiver dropped")
        }

        match rx.await {
            Err(_error) => panic!("sender dopped"),
            Ok(res) => res,
        }
    }
}

impl MessageInterface for MultiRaftMessageSender {
    type RaftMessageFuture<'life0> = impl Future<Output = Result<RaftMessageResponse, Error>> + Send
    where
        Self: 'life0;

    fn raft_message<'life0>(&'life0 self, msg: RaftMessage) -> Self::RaftMessageFuture<'life0> {
        async move {
            self.actor_address.raft_message_tx.send(msg).await.unwrap();
            Ok(RaftMessageResponse::default())
        }
    }
}
