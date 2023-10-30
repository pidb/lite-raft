use std::collections::HashMap;
use std::sync::Arc;

use oceanraft::prelude::MultiRaftMessage;
use oceanraft::transport::{MultiRaftServiceClient, Transport};

#[derive(Clone)]
pub struct GRPCTransport {
    peers: Arc<Vec<(u64, String)>>,
}

impl GRPCTransport {
    pub fn new(peers: Arc<Vec<(u64, String)>>) -> Self {
        Self { peers }
    }
}

impl Transport for GRPCTransport {
    fn send(&self, msg: MultiRaftMessage) -> Result<(), oceanraft::Error> {
        let to = msg.to_node;
        let addr = self
            .peers
            .iter()
            .find(|(id, _)| *id == to)
            .unwrap()
            .1
            .clone();

        tokio::spawn(async move {
            let client = MultiRaftServiceClient::connect(addr.to_string()).await;
            match client {
                Err(err) => {
                    // println!("connect({}) got err({:?})",addr.to_string(), err);
                }
                Ok(mut client) => {
                    if let Err(err) = client.send(msg).await {
                        println!("err({:?})", err);
                    }
                }
            }
        });

        Ok(())
    }
}
