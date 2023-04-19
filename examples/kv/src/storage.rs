use std::path::Path;

use oceanraft::prelude::ConfState;
use oceanraft::storage::RaftSnapshotReader;
use oceanraft::storage::RaftSnapshotWriter;
use oceanraft::storage::Result;
use oceanraft::storage::RockStoreCore;

#[derive(Clone)]
pub struct SledStorage {}

impl SledStorage {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {}
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
