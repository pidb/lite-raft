#![feature(impl_trait_in_assoc_type)]
use args::parse_nodes;
use grpc::PutResponse;
use std::error::Error;
use std::time::Duration;
use tonic::transport::Channel;
use tonic::{Response, Status};

use clap::Parser;

use crate::common::partition;
use crate::grpc::kv_service_client::KvServiceClient;
use crate::grpc::PutRequest;

mod args;
mod common;

pub mod grpc {
    tonic::include_proto!("kv");
}

struct ClientWrap {
    client: Option<KvServiceClient<Channel>>,
}

impl ClientWrap {
    pub fn new() -> Self {
        Self { client: None }
    }

    pub async fn connect(&mut self, addr: &str) -> Result<(), Box<dyn Error>> {
        if let Some(client) = self.client.take() {
            drop(client)
        }
        let client = KvServiceClient::connect(addr.to_string()).await?;
        self.client = Some(client);
        Ok(())
    }

    pub fn is_connected(&self) -> bool {
        if let Some(cc) = self.client.as_ref() {
            return true;
        }

        false
    }

    pub async fn put(
        &mut self,
        key: String,
        value: &[u8],
    ) -> Result<Response<PutResponse>, Status> {
        self.client
            .as_mut()
            .unwrap()
            .put(PutRequest {
                key,
                value: value.to_vec(),
            })
            .await
    }
}

#[tokio::main]
async fn main() {
    let arg = args::ClientArgs::parse();
    arg.validate().unwrap();
    let server_nums = arg.server_num;
    let peers = parse_nodes(&arg.nodes).unwrap();
    let mut client = ClientWrap::new();

    for i in 1..=10 {
        let key = format!("foo_{}", i);
        let val = format!("baz_{}", i);
        println!("put ({}, {}) to {}", key, val, partition(&key, server_nums));

        let mut leader: Option<String> = None;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if leader.is_some() {
                let leader_addr = leader.as_ref().unwrap();
                client.connect(&leader_addr).await.unwrap();
                let resp = client
                    .put(key.clone(), val.clone().as_bytes())
                    .await
                    .unwrap();
                if resp.into_inner().messages.contains("NotLeader") {
                    leader = None;
                } else {
                    break;
                }
            }

            for i in 1..=server_nums {
                // let addr = peers.get(&i).unwrap();
                let addr = peers.iter().find(|(k, _)| *k == i).unwrap().1.clone();
                client.connect(addr.as_str()).await.unwrap();
                let resp = client
                    .put(key.clone(), val.clone().as_bytes())
                    .await
                    .unwrap();
                if resp.into_inner().messages.contains("NotLeader") {
                    println!("{} NotLeader", addr);
                    continue;
                } else {
                    println!("set {} to leader", addr);
                    leader = Some(addr.clone());
                    break;
                }
            }

            if leader.is_some() {
                break;
            }
        }
    }
}
