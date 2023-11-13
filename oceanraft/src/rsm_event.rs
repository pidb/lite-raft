extern crate raft_proto;

use tokio::sync::oneshot;

use super::error::Error;
use crate::multiraft::ProposeRequest;
use crate::multiraft::ProposeResponse;
use crate::prelude::ConfState;
use crate::prelude::MembershipChangeData;

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
    pub index: u64,
    pub term: u64,
    pub data: REQ,
    pub context: Option<Vec<u8>>,
    pub is_conf_change: bool,
    // TODO: consider the tx and apply data separation.
    pub tx: Option<oneshot::Sender<Result<(RES, Option<Vec<u8>>), Error>>>,
}

#[derive(Debug)]
pub struct ApplyMembership<RES: ProposeResponse> {
    pub group_id: u64,
    pub index: u64,
    pub term: u64,
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

pub struct ApplyEvent<W, R>
where
    W: ProposeRequest,
    R: ProposeResponse,
{
    pub node_id: u64,
    pub group_id: u64,
    pub replica_id: u64,
    pub commit_index: u64,
    pub commit_term: u64,
    pub leader_id: u64,
    pub applys: Vec<Apply<W, R>>,
}

#[derive(Debug)]
pub struct LeaderElectionEvent {
    pub node_id: u64,
    pub group_id: u64,
    pub leader_id: u64,
    pub replica_id: u64,
    pub term: u64,
}

#[derive(Debug)]
pub struct GroupCreateEvent {
    pub node_id: u64,
    pub group_id: u64,
    pub replica_id: u64,
}
