extern crate raft_proto;

use futures::Future;
use tokio::sync::oneshot;

use crate::multiraft::WriteResponse;
use crate::prelude::ConfState;
use crate::prelude::MembershipChangeData;

use super::error::Error;
use super::GroupState;
use super::WriteData;

#[derive(Debug)]
pub struct ApplyNoOp {
    pub group_id: u64,
    pub index: u64,
    pub term: u64,
}

#[derive(Debug)]
pub struct ApplyNormal<W, R>
where
    W: WriteData,
    R: WriteResponse,
{
    pub group_id: u64,
    // pub entry: Entry,
    pub index: u64,
    pub term: u64,
    pub data: W,
    pub context: Option<Vec<u8>>,
    pub is_conf_change: bool,
    pub tx: Option<oneshot::Sender<Result<R, Error>>>, // TODO: consider the tx and apply data separation.
}

#[derive(Debug)]
pub struct ApplyMembership<RES: WriteResponse> {
    pub group_id: u64,
    pub index: u64,
    pub term: u64,
    // pub conf_change: ConfChangeV2,
    pub change_data: MembershipChangeData,
    pub conf_state: ConfState,
    pub tx: Option<oneshot::Sender<Result<RES, Error>>>,
}

#[derive(Debug)]
pub enum Apply<W, R>
where
    W: WriteData,
    R: WriteResponse,
{
    NoOp(ApplyNoOp),
    Normal(ApplyNormal<W, R>),
    Membership(ApplyMembership<R>),
}

impl<W, R> Apply<W, R>
where
    W: WriteData,
    R: WriteResponse,
{
    pub(crate) fn get_index(&self) -> u64 {
        match self {
            Self::NoOp(noop) => noop.index,
            Self::Normal(normal) => normal.index,
            Self::Membership(membership) => membership.index,
        }
    }

    #[allow(unused)]
    pub(crate) fn get_term(&self) -> u64 {
        match self {
            Self::NoOp(noop) => noop.term,
            Self::Normal(normal) => normal.term,
            Self::Membership(membership) => membership.term,
        }
    }

    #[allow(unused)]
    pub(crate) fn get_data(&self) -> W {
        match self {
            Self::NoOp(noop) => W::default(),
            Self::Normal(normal) => normal.data.clone(),
            Self::Membership(membership) => unimplemented!(),
        }
    }
}

pub trait StateMachine<W, R>: Send + Sync + 'static
where
    W: WriteData,
    R: WriteResponse,
{
    type ApplyFuture<'life0>: Send + Future<Output = ()> + 'life0
    where
        Self: 'life0;

    fn apply<'life0>(
        &'life0 self,
        group_id: u64,
        state: &GroupState,
        applys: Vec<Apply<W, R>>,
    ) -> Self::ApplyFuture<'life0>;
}
