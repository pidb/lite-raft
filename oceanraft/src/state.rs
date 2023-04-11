use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::RwLock;

use raft::StateRole;

struct WrapStateRole(usize);

impl From<&StateRole> for WrapStateRole {
    fn from(value: &StateRole) -> Self {
        match value {
            StateRole::Follower => WrapStateRole(1),
            StateRole::PreCandidate => WrapStateRole(2),
            StateRole::Candidate => WrapStateRole(3),
            StateRole::Leader => WrapStateRole(4),
        }
    }
}

impl Into<StateRole> for WrapStateRole {
    fn into(self) -> StateRole {
        match self.0 {
            1 => StateRole::Follower,
            2 => StateRole::PreCandidate,
            3 => StateRole::Candidate,
            4 => StateRole::Leader,
            _ => unreachable!(),
        }
    }
}
pub struct GroupState {
    replica_id: AtomicU64,
    commit_index: AtomicU64,
    commit_term: AtomicU64,
    applied_term: AtomicU64,
    applied_index: AtomicU64,
    leader_id: AtomicU64,
    role: AtomicUsize,
}

impl Default for GroupState {
    fn default() -> Self {
        Self::new()
    }
}

impl From<(u64, u64, u64, u64, u64, u64, StateRole)> for GroupState {
    fn from(value: (u64, u64, u64, u64, u64, u64, StateRole)) -> Self {
        Self {
            replica_id: AtomicU64::new(value.0),
            commit_index: AtomicU64::new(value.1),
            commit_term: AtomicU64::new(value.2),
            applied_term: AtomicU64::new(value.3),
            applied_index: AtomicU64::new(value.4),
            leader_id: AtomicU64::new(value.5),
            role: AtomicUsize::new(WrapStateRole::from(&value.6).0),
        }
    }
}

impl GroupState {
    pub fn new() -> Self {
        Self {
            replica_id: AtomicU64::new(0),
            commit_index: AtomicU64::new(0),
            commit_term: AtomicU64::new(0),
            applied_term: AtomicU64::new(0),
            applied_index: AtomicU64::new(0),
            leader_id: AtomicU64::new(0),
            role: AtomicUsize::new(0),
        }
    }

    #[inline]
    #[allow(unused)]
    pub fn get_replica_id(&self) -> u64 {
        self.replica_id.load(Ordering::SeqCst)
    }

    #[inline]
    #[allow(unused)]
    pub fn set_replica_id(&self, val: u64) {
        self.replica_id.store(val, Ordering::SeqCst)
    }

    #[inline]
    #[allow(unused)]
    pub fn get_commit_index(&self) -> u64 {
        self.commit_index.load(Ordering::SeqCst)
    }

    #[inline]
    pub fn set_commit_index(&self, val: u64) {
        self.commit_index.store(val, Ordering::SeqCst)
    }

    #[inline]
    #[allow(unused)]
    pub fn get_commit_term(&self) -> u64 {
        self.commit_term.load(Ordering::SeqCst)
    }

    #[inline]
    pub fn set_commit_term(&self, val: u64) {
        self.commit_term.store(val, Ordering::SeqCst)
    }

    #[inline]
    #[allow(unused)]
    pub fn get_applied_term(&self) -> u64 {
        self.applied_term.load(Ordering::SeqCst)
    }

    #[inline]
    pub fn set_applied_term(&self, val: u64) {
        self.applied_term.store(val, Ordering::SeqCst)
    }

    #[inline]
    #[allow(unused)]
    pub fn get_applied_index(&self) -> u64 {
        self.applied_index.load(Ordering::SeqCst)
    }

    #[inline]
    #[allow(unused)]
    pub fn set_applied_index(&self, val: u64) {
        self.applied_index.store(val, Ordering::SeqCst)
    }

    #[inline]
    #[allow(unused)]
    pub fn get_leader_id(&self) -> u64 {
        self.leader_id.load(Ordering::SeqCst)
    }

    #[inline]
    pub fn set_leader_id(&self, val: u64) {
        self.leader_id.store(val, Ordering::SeqCst)
    }

    #[inline]
    pub fn set_role(&self, role: &StateRole) {
        self.role
            .store(WrapStateRole::from(role).0, Ordering::SeqCst)
    }

    #[inline]
    #[allow(unused)]
    pub fn get_role(&self) -> StateRole {
        WrapStateRole(self.role.load(Ordering::SeqCst)).into()
    }

    #[inline]
    #[allow(unused)]
    pub fn is_leader(&self) -> bool {
        self.get_role() == StateRole::Leader
    }
}

#[derive(Clone)]
pub struct GroupStates {
    states: Arc<RwLock<HashMap<u64, Arc<GroupState>>>>,
}

impl GroupStates {
    pub fn new() -> Self {
        Self {
            states: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    #[inline]
    #[allow(unused)]
    pub fn get(&self, group_id: u64) -> Option<Arc<GroupState>> {
        let rl = self.states.read().unwrap();
        rl.get(&group_id).map_or(None, |state| Some(state.clone()))
    }

    #[inline]
    pub fn insert(&self, group_id: u64, val: Arc<GroupState>) -> Option<Arc<GroupState>> {
        let mut wl = self.states.write().unwrap();
        wl.insert(group_id, val)
    }
}
