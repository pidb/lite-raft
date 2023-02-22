use prost::Message as _;
use raft::RaftState;
use raft_proto::ConfChangeI;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

use crate::prelude::ConfChange;
use crate::prelude::ConfChangeV2;
use crate::prelude::Entry;
use crate::prelude::EntryType;
use crate::prelude::MembershipChangeRequest;

use super::error::Error;

/// Commit membership change results.
///
/// If proposed change is ConfChange, the ConfChangeV2 is converted
/// from ConfChange. If ConfChangeV2 is used, changes contains multiple
/// requests, otherwise changes contains only one request.
#[derive(Debug)]
pub struct CommitMembership {
    pub entry_index: u64,
    pub conf_change: ConfChangeV2,
    pub change_request: MembershipChangeRequest,
}

#[derive(Debug)]
pub enum CommitEvent {
    None,
    Membership((CommitMembership, oneshot::Sender<Result<(), Error>>)),
}

impl Default for CommitEvent {
    fn default() -> Self {
        CommitEvent::None
    }
}

/// A LeaderElectionEvent is send when leader changed.
#[derive(Debug, Clone)]
pub struct LeaderElectionEvent {
    /// The id of the group where the leader belongs.
    pub group_id: u64,
    /// Current replica id. If current replica is the
    /// leader, then `replica_id` equal to `leader_id`.
    pub replica_id: u64,
    /// Current leader id.
    pub leader_id: u64,
}

#[derive(Debug)]
pub enum Event {
    LederElection(LeaderElectionEvent),
}

#[derive(Debug)]
pub struct ApplyNoOp {
    pub group_id: u64,
    pub entry_index: u64,
    pub entry_term: u64,
}

#[derive(Debug)]
pub struct ApplyNormal {
    pub group_id: u64,
    pub entry: Entry,
    pub is_conf_change: bool,
    pub tx: Option<oneshot::Sender<Result<(), Error>>>,
}

#[derive(Debug)]
pub struct ApplyMembership {
    pub group_id: u64,
    pub entry: Entry,
    pub tx: Option<oneshot::Sender<Result<(), Error>>>,
    pub commit_tx: UnboundedSender<CommitEvent>,
}

impl ApplyMembership {
    pub fn parse(&self) -> CommitMembership {
        match self.entry.entry_type() {
            EntryType::EntryNormal => unreachable!(),
            EntryType::EntryConfChange => {
                let mut conf_change = ConfChange::default();
                conf_change.merge(self.entry.data.as_ref()).unwrap();

                let mut change_request = MembershipChangeRequest::default();
                change_request.merge(self.entry.context.as_ref()).unwrap();

                CommitMembership {
                    entry_index: self.entry.index,
                    conf_change: conf_change.into_v2(),
                    change_request,
                }
            }
            EntryType::EntryConfChangeV2 => {
                let mut conf_change = ConfChangeV2::default();
                conf_change.merge(self.entry.data.as_ref()).unwrap();

                let mut change_request = MembershipChangeRequest::default();
                change_request.merge(self.entry.context.as_ref()).unwrap();

                CommitMembership {
                    entry_index: self.entry.index,
                    conf_change,
                    change_request,
                }
            }
        }
    }
    
    pub async fn done(&self) {
        let (tx, rx) = oneshot::channel();
        let commit = self.parse();
        self.commit_tx
            .send(CommitEvent::Membership((commit, tx)))
            .unwrap();
        rx.await.unwrap();
    }
}

#[derive(Debug)]
pub enum ApplyEvent {
    NoOp(ApplyNoOp),
    Normal(ApplyNormal),
    Membership(ApplyMembership),
}

impl ApplyEvent {
    pub(crate) fn entry_index(&self) -> u64 {
        match self {
            Self::NoOp(noop) => noop.entry_index,
            Self::Normal(normal) => normal.entry.index,
            Self::Membership(membership) => membership.entry.index,
        }
    }

    #[allow(unused)]
    pub(crate) fn entry_term(&self) -> u64 {
        match self {
            Self::NoOp(noop) => noop.entry_term,
            Self::Normal(normal) => normal.entry.term,
            Self::Membership(membership) => membership.entry.term,
        }
    }
}

/// MultiRaftEvent is sent by a multiraft actor, and the receiver 
/// performs specific actions within the system.
#[derive(Debug, Clone)]
pub enum MultiRaftEvent {
    /// Default value.
    None,

    /// Sent when consensus group is created.
    CreateGroup {
        group_id: u64,
        replica_id: u64,
        commit_index: u64,
        commit_term: u64,
        applied_index: u64,
        applied_term: u64,
    }
}

impl Default for MultiRaftEvent {
    fn default() -> Self {
        Self::None
    }
}
