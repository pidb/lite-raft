use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::RwLock;

use oceanraft::prelude::ConfState;
use oceanraft::storage::RaftSnapshotReader;
use oceanraft::storage::RaftSnapshotWriter;
use oceanraft::storage::Result;
use oceanraft::storage::RockStoreCore;

type MemStorage = Arc<RwLock<HashMap<String, Vec<u8>>>>;

#[derive(Clone)]
pub struct SledStorage {
    mem_map: MemStorage,
}

impl SledStorage {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            mem_map: MemStorage::default(),
        }
    }

    pub fn put(&self, key: String, value: Vec<u8>) {
        let mut wl = self.mem_map.write().unwrap();
        let _ = wl.entry(key).or_insert(value);
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>>{
        let rl = self.mem_map.read().unwrap();
        rl.get(key).map(|v| v.clone())
    }
}

impl RaftSnapshotReader for SledStorage {
    fn load_snapshot(&self, group_id: u64, replica_id: u64) -> Result<Vec<u8>> {
        Ok(vec![])
    }
}

impl RaftSnapshotWriter for SledStorage {
    fn build_snapshot(
        &self,
        group_id: u64,
        replica_id: u64,
        applied_index: u64,
        applied_term: u64,
        last_conf_state: ConfState,
    ) -> Result<()> {
        todo!()
    }

    fn install_snapshot(&self, group_id: u64, replica_id: u64, data: Vec<u8>) -> Result<()> {
        Ok(())
    }
}
