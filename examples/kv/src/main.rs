#![feature(type_alias_impl_trait)]

mod server;
mod state_machine;
mod storage;

pub mod grpc {
    tonic::include_proto!("kv");
}

#[tokio::main]
async fn main() {
    let mut server = server::KVServer::new();
    server.start();
    server.join().await;
}
