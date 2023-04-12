
use crate::grpc::kv_service_client::KvServiceClient;
use crate::grpc::PutRequest;
use crate::grpc::PutResponse;


pub mod grpc {
    tonic::include_proto!("kv");
}

#[tokio::main]
async fn main() {
    let mut client = KvServiceClient::connect("http://[::1]:50051").await.unwrap();
}