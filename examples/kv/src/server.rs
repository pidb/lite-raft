use tonic::Request;
use tonic::Response;
use tonic::Status;

use crate::grpc::kv_service_server::KvService;
use crate::grpc::PutRequest;
use crate::grpc::PutResponse;

pub struct KvServiceImpl;

#[tonic::async_trait]
impl KvService for KvServiceImpl {
    async fn put(
        &self,
        _request: Request<PutRequest>,
    ) -> Result<Response<PutResponse>, Status> {
        todo!()
    }
}