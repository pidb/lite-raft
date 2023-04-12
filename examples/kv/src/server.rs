use std::sync::Arc;

use oceanraft::MultiRaft;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use crate::grpc::kv_service_server::KvService;
use crate::grpc::kv_service_server::KvServiceServer;
use crate::grpc::PutRequest;
use crate::grpc::PutResponse;
use crate::state_machine::KVStateMachine;

/// Define propose data to oceanraft.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KVData {
    key: String,
    value: Vec<u8>,
}

/// Define propose response to oceanraft.
#[derive(Debug, Clone)]
pub struct KVResponse {
    pub index: u64,
    pub term: u64,
}

pub struct KvServiceImpl {
    // multiraft: Arc<MultiRaft<KVData, KVResponse, KVStateMachine>>,
}

#[tonic::async_trait]
impl KvService for KvServiceImpl {
    async fn put(&self, _request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        todo!()
    }
}

pub struct KVServer {
    // multiraft: Arc<MultiRaft<KVData, KVResponse, KVStateMachine>>,
    jh: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl KVServer {
    pub fn new() -> Self {
        Self { jh: None }
    }

    /// Start server in spearted tokio task.
    pub fn start(&mut self) {
        // let multiraft = self.multiraft.clone();
        let jh = tokio::spawn(async move {
            // TODO: as configuration
            let addr = "[::1]:50051".parse().unwrap();

            let kv_service = KvServiceServer::new(KvServiceImpl {});
            Server::builder().add_service(kv_service).serve(addr).await
        });

        self.jh = Some(jh)
    }

    pub async fn join(mut self) {
        self.jh.take().unwrap().await.unwrap();
    }
}
