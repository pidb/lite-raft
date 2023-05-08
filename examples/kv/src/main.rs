#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]
mod args;
mod server;
mod state_machine;
mod storage;
mod transport;

pub mod grpc {
    tonic::include_proto!("kv");
}

use std::time::Duration;

use args::ServerArgs;
use clap::Parser;

#[tokio::main]
async fn main() {
    let arg = ServerArgs::parse();
    if let Err(reason) = arg.validate() {
        panic!("{}", reason)
    }

    let mut server = server::KVServer::new(arg).await;
    server.event_consumer();
    server.start();
    tokio::time::sleep(Duration::from_secs(5)).await;
    if server.node_id == 1 {
        let mut members = vec![];
        for (peer_id, _) in server.peers.iter() {
            if *peer_id == server.node_id {
                continue;
            }

            members.push((*peer_id, *peer_id));
        }

        // for (peer_id, _) in server.peers.iter() {
        //     println!("group({}) add members({:?})", *peer_id, members);
        //     server.add_members(*peer_id, members.clone()).await;
        // }
    }
    server.join().await;
}
