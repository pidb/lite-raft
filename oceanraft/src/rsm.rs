extern crate raft_proto;

use futures::Future;
use tokio::sync::oneshot;

use crate::multiraft::ProposeResponse;
use crate::prelude::ConfState;
use crate::prelude::MembershipChangeData;

use super::error::Error;
use super::GroupState;
use super::ProposeRequest;

#[derive(Debug)]
pub struct ApplyNoOp {
    pub group_id: u64,
    pub index: u64,
    pub term: u64,
}

#[derive(Debug)]
pub struct ApplyNormal<REQ, RES>
where
    REQ: ProposeRequest,
    RES: ProposeResponse,
{
    pub group_id: u64,
    // pub entry: Entry,
    pub index: u64,
    pub term: u64,
    pub data: REQ,
    pub context: Option<Vec<u8>>,
    pub is_conf_change: bool,
    pub tx: Option<oneshot::Sender<Result<(RES, Option<Vec<u8>>), Error>>>, // TODO: consider the tx and apply data separation.
}

#[derive(Debug)]
pub struct ApplyMembership<RES: ProposeResponse> {
    pub group_id: u64,
    pub index: u64,
    pub term: u64,
    // pub conf_change: ConfChangeV2,
    pub change_data: Option<MembershipChangeData>,
    pub ctx: Option<Vec<u8>>,
    pub conf_state: ConfState,
    pub tx: Option<oneshot::Sender<Result<(RES, Option<Vec<u8>>), Error>>>,
}

#[derive(Debug)]
pub enum Apply<W, R>
where
    W: ProposeRequest,
    R: ProposeResponse,
{
    NoOp(ApplyNoOp),
    Normal(ApplyNormal<W, R>),
    Membership(ApplyMembership<R>),
}

impl<W, R> Apply<W, R>
where
    W: ProposeRequest,
    R: ProposeResponse,
{
    pub fn get_index(&self) -> u64 {
        match self {
            Self::NoOp(noop) => noop.index,
            Self::Normal(normal) => normal.index,
            Self::Membership(membership) => membership.index,
        }
    }

    #[allow(unused)]
    pub fn get_term(&self) -> u64 {
        match self {
            Self::NoOp(noop) => noop.term,
            Self::Normal(normal) => normal.term,
            Self::Membership(membership) => membership.term,
        }
    }
}

#[derive(Debug)]
pub struct LeaderElectionEvent {
    pub node_id: u64,
    pub group_id: u64,
    pub leader_id: u64,
    pub replica_id: u64,
    pub term: u64,
}

pub trait StateMachine<W, R>: Send + Sync + 'static
where
    W: ProposeRequest,
    R: ProposeResponse,
{
    type ApplyFuture<'life0>: Send + Future<Output = ()> + 'life0
    where
        Self: 'life0;

    fn apply<'life0>(
        &'life0 self,
        group_id: u64,
        replica_id: u64,
        state: &GroupState,
        applys: Vec<Apply<W, R>>,
    ) -> Self::ApplyFuture<'life0>;

    type OnLeaderElectionFuture<'life0>: Send + Future<Output = ()> + 'life0
    where
        Self: 'life0;

    /// Called when a new leader is elected.
    fn on_leader_election<'life0>(
        &'life0 self,
        event: LeaderElectionEvent,
    ) -> Self::OnLeaderElectionFuture<'life0>;
}
