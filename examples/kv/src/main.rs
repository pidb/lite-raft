use tonic::transport::Server;

mod server;
mod storage;
mod state_machine;



pub mod grpc {
    tonic::include_proto!("kv");
}

fn main() {
    let _grpc_serv = Server::builder();
    println!("hello world")
}