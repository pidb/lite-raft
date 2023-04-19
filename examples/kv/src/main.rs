#![feature(type_alias_impl_trait)]

mod args;
mod server;
mod state_machine;
mod storage;
mod transport;

pub mod grpc {
    tonic::include_proto!("kv");
}

use args::ServerArgs;
use clap::Parser;

#[tokio::main]
async fn main() {
    let arg = ServerArgs::parse();
    if let Err(reason) = arg.validate() {
        panic!("{}", reason)
    }

    let mut server = server::KVServer::new(arg).await;
    server.start();
    server.join().await;
}
