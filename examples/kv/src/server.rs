use std::collections::HashMap;
use std::sync::Arc;

use oceanraft::prelude::CreateGroupRequest;
use oceanraft::prelude::Snapshot;
use oceanraft::storage::MultiRaftStorage;
use oceanraft::storage::RockStore;
use oceanraft::storage::RockStoreCore;
use oceanraft::storage::Storage;
use oceanraft::storage::StorageExt;
use oceanraft::task_group::TaskGroup;
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
use crate::storage::SledStorage;
use crate::transport::GRPCTransport;

use oceanraft::define_multiraft;

define_multiraft! {
    pub KVAppType:
        D =  KVData,
        R = KVResponse,
        M = KVStateMachine,
        S = RockStoreCore<SledStorage, SledStorage>,
        MS = RockStore<SledStorage, SledStorage>
}

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

    kv_storage: SledStorage,

    multiraft: Arc<MultiRaft<KVAppType, GRPCTransport>>,

    jh: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl KVServer {
    pub async fn new(arg: ServerArgs) -> Self {
        let peers = Arc::new(arg.parse_nodes().unwrap());
        let mut cfg = Config::default();
        cfg.node_id = arg.node_id;
        let kv_storage = SledStorage::new(&arg.kv_storage_path);
        let grpc_transport = GRPCTransport::new(peers.clone());
        let rock_storage = RockStore::new(
            arg.node_id,
            &arg.log_storage_path,
            kv_storage.clone(),
            kv_storage.clone(),
        );
        let kv_state_machine = KVStateMachine::new(rock_storage.clone());

        let gs = rock_storage.group_storage(1, 1).await.unwrap();
        if !gs.initial_state().unwrap().initialized() {
            println!("not snapshot..");
            let mut snap = Snapshot::default();
            snap.mut_metadata().mut_conf_state().voters = vec![1];
            snap.mut_metadata().index = 1;
            snap.mut_metadata().term = 1;
            gs.install_snapshot(snap).unwrap();
        }

        let multiraft = MultiRaft::<KVAppType, GRPCTransport>::new::<tokio::time::Interval>(
            cfg,
            grpc_transport,
            rock_storage,
            kv_state_machine,
            TaskGroup::new(),
            None,
        )
        .unwrap();

        if let Err(err) = multiraft
            .create_group(CreateGroupRequest {
                group_id: 1,
                replica_id: 1,
                replicas: vec![],
                applied_hint: 0,
            })
            .await
        {
            println!("{}", err)
        }

        Self {
            arg,
            peers,
            kv_storage,
            multiraft: Arc::new(multiraft),
            jh: None,
        }
    }

    pub fn member() {}

    /// Start server in spearted tokio task.
    pub fn start(&mut self) {
        // let multiraft = self.multiraft.clone();
        let addr = self.arg.addr.clone();
        let kv_service = KvServiceServer::new(KvServiceImpl {});
        let multiraft_service =
            MultiRaftServiceServer::new(MultiRaftServiceImpl::new(self.multiraft.message_sender()));
        let jh = tokio::spawn(async move {
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
