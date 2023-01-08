use std::pin::Pin;

use futures::Future;

use raft::Storage;
use raft_proto::prelude::ConfChangeV2;
use raft_proto::prelude::Entry;
use raft_proto::prelude::MembershipChangeRequest;

use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;

use super::error::Error;
use super::multiraft_actor::MultiRaftActorContext;
use super::storage::MultiRaftStorage;

pub type MultiRaftAsyncCb<'r, RS: Storage, MRS: MultiRaftStorage<RS>> = Box<
    dyn FnOnce(
        &'r mut MultiRaftActorContext<RS, MRS>,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + 'r>>,
>;

/// Apply membership change results.
///
/// If proposed change is ConfChange, the ConfChangeV2 is converted
/// from ConfChange. If ConfChangeV2 is used, changes contains multiple
/// requests, otherwise changes contains only one request.
#[derive(Debug)]
pub struct MembershipChangeView {
    pub index: u64,
    pub conf_change: ConfChangeV2,
    pub change_request: MembershipChangeRequest,
}

#[derive(Debug)]
pub enum CallbackEvent {
    None,

    MembershipChange(MembershipChangeView, oneshot::Sender<Result<(), Error>>),
}

impl Default for CallbackEvent {
    fn default() -> Self {
        CallbackEvent::None
    }
}

#[derive(Debug)]
pub struct LeaderElectionEvent {
    pub group_id: u64,
    pub leader_id: u64,
    pub committed_term: u64,
}

#[derive(Debug)]
pub struct ApplyNormalEvent {
    pub group_id: u64,
    pub entry: Entry,
    pub is_conf_change: bool,
    pub tx: Option<oneshot::Sender<Result<(), Error>>>,
}

#[derive(Debug)]
pub struct ApplyMembershipChangeEvent {
    pub group_id: u64,
    pub entry: Entry,
    pub tx: Option<oneshot::Sender<Result<(), Error>>>,
    pub(crate) change_view: Option<MembershipChangeView>,
    pub(crate) callback_event_tx: Sender<CallbackEvent>,
}

impl ApplyMembershipChangeEvent {
    pub async fn commit_to_multiraft(&mut self) -> Result<(), Error> {
        if let Some(change_view) = self.change_view.take() {
            let (tx, rx) = oneshot::channel();
            self.callback_event_tx
                .send(CallbackEvent::MembershipChange(change_view, tx))
                .await
                .unwrap();
            rx.await.unwrap()
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
pub enum Event {
    LederElection(LeaderElectionEvent),

    ApplyNormal(ApplyNormalEvent),

    ApplyMembershipChange(ApplyMembershipChangeEvent),
}
