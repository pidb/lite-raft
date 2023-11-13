extern crate raft_proto;

use futures::Future;

use crate::multiraft::ProposeRequest;
use crate::multiraft::ProposeResponse;
use crate::rsm_event::ApplyEvent;
use crate::rsm_event::GroupCreateEvent;
use crate::rsm_event::LeaderElectionEvent;

pub trait StateMachine<W, R>: Send + Sync + 'static
where
    W: ProposeRequest,
    R: ProposeResponse,
{
    type ApplyFuture<'life0>: Send + Future<Output = ()> + 'life0
    where
        Self: 'life0;

    /// Called when a new entry is committed.
    fn apply<'life0>(&'life0 self, event: ApplyEvent<W, R>) -> Self::ApplyFuture<'life0>;

    type OnLeaderElectionFuture<'life0>: Send + Future<Output = ()> + 'life0
    where
        Self: 'life0;

    /// Called when a new leader is elected.
    fn on_leader_election<'life0>(
        &'life0 self,
        event: LeaderElectionEvent,
    ) -> Self::OnLeaderElectionFuture<'life0>;

    type OnGroupCreateFuture<'life0>: Send + Future<Output = ()> + 'life0
    where
        Self: 'life0;

    /// Called when a new group is created.
    fn on_group_create<'life0>(
        &'life0 self,
        event: GroupCreateEvent,
    ) -> Self::OnGroupCreateFuture<'life0>;
}
