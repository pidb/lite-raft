use super::ProposeRequest;

use crate::multiraft::ProposeResponse;
use crate::multiraft::NO_LEADER;
use crate::prelude::ConfChangeType;
use crate::prelude::GroupMetadata;
use crate::prelude::Message;
use crate::prelude::MessageType;
use crate::prelude::MultiRaftMessage;
use crate::prelude::MultiRaftMessageResponse;
use crate::prelude::ReplicaDesc;

use super::apply::ApplyActor;
use super::config::Config;
use super::error::ChannelError;
use super::error::Error;
use super::error::RaftGroupError;
use super::event::Event;
use super::event::EventChannel;
use super::group::RaftGroup;
use super::group::RaftGroupWriteRequest;
use super::group::Status;
use super::msg::ApplyCommitMessage;
use super::msg::ApplyData;
use super::msg::ApplyMessage;
use super::msg::ApplyResultMessage;
use super::msg::CommitMembership;
use super::msg::ManageMessage;
use super::msg::ProposeMessage;
use super::msg::QueryGroup;
use super::multiraft::NO_GORUP;
use super::multiraft::NO_NODE;
use super::proposal::ProposalQueue;
use super::proposal::ReadIndexQueue;
use super::replica_cache::ReplicaCache;
use super::rsm::StateMachine;
use super::state::GroupState;
use super::state::GroupStates;
use super::storage::MultiRaftStorage;
use super::storage::RaftStorage;
use super::tick::Ticker;
use super::transport::Transport;

use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

pub(crate) struct NodeHandle<W, R>
where
    W: ProposeRequest,
    R: ProposeResponse,
{
    // TODO: queue should have one per-group.
    pub propose_tx: Sender<ProposeMessage<W, R>>,
    pub campaign_tx: Sender<(u64, oneshot::Sender<Result<(), Error>>)>,
    pub raft_message_tx: Sender<(
        MultiRaftMessage,
        oneshot::Sender<Result<MultiRaftMessageResponse, Error>>,
    )>,
    pub manage_tx: Sender<ManageMessage>,
    pub query_group_tx: UnboundedSender<QueryGroup>,
}
