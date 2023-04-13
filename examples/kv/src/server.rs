use std::collections::HashMap;
use std::sync::Arc;

use oceanraft::transport::MultiRaftServiceImpl;
use oceanraft::transport::MultiRaftServiceServer;
use oceanraft::Config;
use oceanraft::MultiRaft;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use crate::args::ServerArgs;
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
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let put_req = request.into_inner();
        println!("{:?}", put_req);
        Ok(Response::new(PutResponse::default()))
    }
}

pub struct KVServer {
    arg: ServerArgs,

    // Mapping nodes to network addr.
    peers: Arc<HashMap<u64, String>>,

    // multiraft: Arc<MultiRaft<KVData, KVResponse, KVStateMachine>>,
    jh: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl KVServer {
    pub fn new(arg: ServerArgs) -> Self {
        let peers = Arc::new(arg.parse_nodes().unwrap());
        let mut cfg = Config::default();
        cfg.node_id = arg.node_id;

        // MultiRaft::new(cfg, transport, storage, rsm, task_group, ticker);

        Self {
            arg,
            peers,
            jh: None,
        }
    }

    /// Start server in spearted tokio task.
    pub fn start(&mut self) {
        // let multiraft = self.multiraft.clone();
        let addr = self.arg.addr.clone();
        let jh = tokio::spawn(async move {
            let kv_service = KvServiceServer::new(KvServiceImpl {});
            let multiraft_service =
                MultiRaftServiceServer::new(MultiRaftServiceImpl::new(/*todo!*/));
            Server::builder()
                .add_service(kv_service)
                .add_service(multiraft_service)
                .serve(addr.parse().unwrap())
                .await
        });

        self.jh = Some(jh)
    }

    pub async fn join(mut self) {
        self.jh.take().unwrap().await.unwrap().unwrap();
    }
}
