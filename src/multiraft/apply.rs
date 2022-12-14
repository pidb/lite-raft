use std::collections::HashMap;

#[derive(Default, Debug)]
pub struct GroupApplyRequest {
    pub term: u64,
    pub commit_index: u64,
    pub entries: Vec<raft::prelude::Entry>,
}

#[derive(Default, Debug)]
pub struct ApplyTaskRequest {
    pub groups: HashMap<u64, GroupApplyRequest>,
}


pub struct ApplyActor {
    
}