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

use args::ServerArgs;
use clap::Parser;
use oceanraft::log;

#[tokio::main]
async fn main() {
    log::init_global_console_tracing("info");
    let arg = ServerArgs::parse();
    if let Err(reason) = arg.validate() {
        panic!("{}", reason)
    }

    let mut server = server::KVServer::new(arg).await.unwrap();
    server.event_consumer();
    server.start().await;

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    server.add_members().await.unwrap();

    server.join().await;
}
