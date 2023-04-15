use std::collections::HashMap;
use std::sync::Arc;

use oceanraft::prelude::MultiRaftMessage;
use oceanraft::transport::{MultiRaftServiceClient, Transport};

#[derive(Clone)]
pub struct GRPCTransport {
    peers: Arc<HashMap<u64, String>>,
}

impl GRPCTransport {
    pub fn new (peers: Arc<HashMap<u64, String>>) -> Self {
        Self {
            peers
        }
    }
}

impl Transport for GRPCTransport {
    fn send(&self, msg: MultiRaftMessage) -> Result<(), oceanraft::Error> {
        let to = msg.to_node;
        let addr = self.peers.get(&to).unwrap().to_string();

        tokio::spawn(async move {
            let mut client = MultiRaftServiceClient::connect(addr.to_string())
                .await
                .unwrap();
            client.send(msg).await.unwrap();
        });

        Ok(())
    }
}
