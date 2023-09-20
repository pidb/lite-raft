use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use anyhow::anyhow;
use oceanraft::prelude::CreateGroupRequest;
use oceanraft::prelude::ReplicaDesc;
use oceanraft::prelude::Snapshot;
use oceanraft::storage::MultiRaftStorage;
use oceanraft::storage::RockStore;
use oceanraft::storage::RockStoreCore;
use oceanraft::storage::Storage;
use oceanraft::storage::StorageExt;
use oceanraft::transport::MultiRaftServiceImpl;
use oceanraft::transport::MultiRaftServiceServer;
use oceanraft::Config;
use oceanraft::MultiRaft;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use crate::args::parse_nodes;
use crate::args::ServerArgs;
use crate::grpc::kv_service_server::KvService;
use crate::grpc::kv_service_server::KvServiceServer;
use crate::grpc::PutRequest;
use crate::grpc::PutResponse;
use crate::state_machine::KVStateMachine;
use crate::storage::MemKvStorage;
use crate::transport::GRPCTransport;

use oceanraft::define_multiraft;

define_multiraft! {
    pub KVAppType:
        D =  KVData,
        R = KVResponse,
        M = KVStateMachine,
        S = RockStoreCore<MemKvStorage, MemKvStorage>,
        MS = RockStore<MemKvStorage, MemKvStorage>
}

/// Define propose data to oceanraft.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KVData {
    pub key: String,
    pub value: Vec<u8>,
}

/// Define propose response to oceanraft.
#[derive(Debug, Clone)]
pub struct KVResponse {
    pub index: u64,
    pub term: u64,
}

pub struct KvServiceImpl {
    multiraft: Arc<MultiRaft<KVAppType, GRPCTransport>>,
}

#[tonic::async_trait]
impl KvService for KvServiceImpl {
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let put_req = request.into_inner();
        let group_id = partition(&put_req.key, 3);
        let res = self
            .multiraft
            .write(
                group_id,
                0,
                None,
                KVData {
                    key: put_req.key.clone(),
                    value: put_req.value.clone(),
                },
            )
            .await;
        println!("group_id = {}, req = {:?}", group_id, put_req);
        let mut resp = PutResponse::default();
        resp.messages = format!("{:?}", res);
        Ok(Response::new(resp))
    }
}

fn partition(key: &str, partition: u64) -> u64 {
    let mut h = DefaultHasher::new();
    key.hash(&mut h);
    let hv = h.finish();
    (hv % partition) + 1
}

pub struct KVServer {
    arg: ServerArgs,

    pub node_id: u64,

    // Mapping nodes to network addr.
    pub peers: Arc<HashMap<u64, String>>,

    kv_storage: MemKvStorage,

    multiraft: Arc<MultiRaft<KVAppType, GRPCTransport>>,

    jh: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl KVServer {
    pub async fn new(arg: ServerArgs) -> anyhow::Result<Self> {
        let peers = Arc::new(parse_nodes(&arg.nodes)?);
        let mut cfg = Config::default();
        cfg.node_id = arg.node_id;
        cfg.tick_interval = 100;

        let kv_storage = MemKvStorage::new();

        // create multiraft storage
        let rock_storage = RockStore::new(
            arg.node_id,
            &arg.log_storage_path,
            kv_storage.clone(),
            kv_storage.clone(),
        );

        // create multiraft bussiness statemachine
        let kv_state_machine = KVStateMachine::new(rock_storage.clone(), kv_storage.clone());

        // create multiraft transport
        let grpc_transport = GRPCTransport::new(peers.clone());

        // create multiraft instance
        let multiraft = MultiRaft::<KVAppType, GRPCTransport>::new(
            cfg,
            grpc_transport,
            rock_storage.clone(),
            kv_state_machine,
            None,
        )
        .map_err(|err| anyhow!("{}", err))?;

        let node_id = arg.node_id;
        let server = Self {
            arg: arg.clone(),
            peers: peers.clone(),
            node_id,
            kv_storage,
            multiraft: Arc::new(multiraft),
            jh: None,
        };

        // every node initial replica desc
        let mut replicas = vec![];
        for (peer_id, _) in peers.iter() {
            let node_id = *peer_id;
            let replica_id = *peer_id;
            for (group_id, _) in peers.iter() {
                let replica_desc = ReplicaDesc {
                    node_id,
                    group_id: *group_id,
                    replica_id,
                };

                println!(
                    "group({}) initial replica_desc({:?})",
                    group_id, replica_desc
                );
                replicas.push(replica_desc.clone());
                rock_storage
                    .set_replica_desc(*group_id, replica_desc)
                    .await
                    .unwrap();
            }
        }

        let replica_id = node_id;
        let voters = (1..=peers.len() as u64).collect::<Vec<_>>();
        for group_id in 1..=peers.len() as u64 {
            let gs = rock_storage
                .group_storage(group_id, replica_id)
                .await
                .unwrap();
            if !gs.initial_state().unwrap().initialized() {
                println!(
                    "node {}: create replica({}) of group({}) initial voters({:?})",
                    node_id, replica_id, group_id, voters
                );
                let mut snap = Snapshot::default();
                snap.mut_metadata().mut_conf_state().voters = voters.clone();
                snap.mut_metadata().index = 1;
                snap.mut_metadata().term = 1;
                gs.install_snapshot(snap).unwrap();

                if let Err(err) = server
                    .multiraft
                    .create_group(CreateGroupRequest {
                        group_id,
                        replica_id,
                        replicas: replicas.clone(),
                        applied_hint: 0,
                    })
                    .await
                {
                    println!("{}", err)
                }
            }
        }

        server
    }

    pub async fn add_members(
        &self,
        multiraft: &Weak<MultiRaft<KVAppType, GRPCTransport>>,
        storage: &RockStore<SledStorage, SledStorage>,
        group_id: u64,
        members: Vec<(u64, u64)>,
    ) {
        let mut data = MembershipChangeData::default();
        for (node_id, replica_id) in members.iter() {
            let mut change = SingleMembershipChange::default();
            change.set_node_id(*node_id);
            change.set_replica_id(*replica_id);
            change.set_change_type(ConfChangeType::AddNode);
            data.changes.push(change);
        }
        let term = None;
        let context = None;
        self.multiraft
            .membership(group_id, term, context, data)
            .await
            .unwrap();
    }

    pub fn event_consumer(&self) {
        let rx = self.multiraft.subscribe();
        tokio::spawn(async move {
            loop {
                let event = match rx.recv().await {
                    Err(_error) => break,
                    Ok(event) => event,
                };

                match event {
                    oceanraft::Event::LederElection(_event) => {
                        // TODO: check and add members if need
                    }
                    _ => {}
                }
            }
        });
    }

    /// Start server in spearted tokio task.
    pub fn start(&mut self) {
        self.multiraft.start();
        self.start_server();
    }

    fn start_server(&mut self) {
        // let multiraft = self.multiraft.clone();
        let addr = self.arg.addr.clone();
        let kv_service = KvServiceServer::new(KvServiceImpl {
            multiraft: self.multiraft.clone(),
        });
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
