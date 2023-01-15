use std::collections::VecDeque;

use prost::Message;
use raft::RawNode;
use raft::StateRole;
use raft::Storage;
use raft::prelude::Entry;
use tokio::sync::oneshot;

use raft_proto::prelude::AppReadIndexRequest;
use raft_proto::prelude::AppWriteRequest;
use raft_proto::prelude::ReplicaDesc;

use super::apply_actor::Apply;
use super::util;
use super::error::Error;
use super::error::ProposalError;
use super::proposal::GroupProposalQueue;
use super::proposal::Proposal;
use super::proposal::ReadIndexProposal;

/// Represents a replica of a raft group.
pub struct RaftGroup<RS: Storage> {
    pub group_id: u64,
    pub replica_id: u64,
    pub raft_group: RawNode<RS>,
    // track the nodes which members ofq the raft consensus group
    pub node_ids: Vec<u64>,
    pub proposals: GroupProposalQueue,
    pub leader: ReplicaDesc,
    pub committed_term: u64,
}

impl<RS> RaftGroup<RS>
where
    RS: Storage,
{
    #[inline]
    pub fn is_leader(&self) -> bool {
        self.raft_group.raft.state == StateRole::Leader
    }

    #[inline]
    pub fn term(&self) -> u64 {
        self.raft_group.raft.term
    }

    #[inline]
    pub fn committed_term(&self) -> u64 {
        self.committed_term
    }

    #[inline]
    pub fn last_index(&self) -> u64 {
        self.raft_group.raft.raft_log.last_index()
    }

    /// Update the term of the latest entries committed during
    /// the term of the leader.
    #[inline]
    pub fn maybe_update_committed_term(&mut self, term: u64) {
        if self.committed_term != term && self.leader.replica_id != 0 {
            self.committed_term = term
        }
    }

    pub fn create_apply(&mut self, replica_id: u64, entries: Vec<Entry>) -> Apply {
        let current_term = self.raft_group.raft.term;
        let commit_index = self.raft_group.raft.raft_log.committed;
        let mut proposals = VecDeque::new();
        if !proposals.is_empty() {
            for entry in entries.iter() {
                match self
                    .proposals
                    .find_proposal(entry.term, entry.index, current_term)
                {
                    Err(error) => {
                        continue;
                    }
                    Ok(proposal) => match proposal {
                        None => continue,

                        Some(p) => proposals.push_back(p),
                    },
                };
            }
        }

        let entries_size = entries
            .iter()
            .map(|ent| util::compute_entry_size(ent))
            .sum::<usize>();
        Apply {
            replica_id,
            group_id: self.group_id,
            term: current_term,
            commit_index,
            commit_term: 0, // TODO: get commit term
            entries,
            entries_size,
            proposals,
        }
    }

    fn write_pre_propose(&mut self, request: &AppWriteRequest) -> Result<(), Error>
    where
        RS: Storage,
    {
        if request.data.is_empty() {
            return Err(Error::BadParameter(format!("write request data is empty")));
        }

        if !self.is_leader() {
            return Err(Error::Proposal(ProposalError::NotLeader(
                self.group_id,
                self.replica_id,
                self.raft_group.raft.leader_id,
            )));
        }

        if request.term != 0 && self.term() > request.term {
            return Err(Error::Proposal(ProposalError::Stale(request.term)));
        }

        Ok(())
    }

    pub fn write_propose(
        &mut self,
        request: AppWriteRequest,
        tx: oneshot::Sender<Result<(), Error>>,
    ) {
        if let Err(err) = self.write_pre_propose(&request) {
            tx.send(Err(err)).unwrap();
            return;
        }
        let term = self.term();

        // propose to raft gorup
        let expected_next_index = self.last_index() + 1;

        if let Err(err) = self.raft_group.propose(request.context, request.data) {
            tx.send(Err(Error::Proposal(ProposalError::Other(Box::new(err)))))
                .unwrap();
            return;
        }

        let index = self.last_index() + 1;
        if expected_next_index != index {
            tx.send(Err(Error::Proposal(ProposalError::Unexpected(index))))
                .unwrap();
            return;
        }

        let proposal = Proposal {
            index,
            term,
            is_conf_change: false,
            tx: Some(tx),
        };

        self.proposals.push(proposal).unwrap();
    }

    pub fn read_index_propose(
        &mut self,
        request: AppReadIndexRequest,
        tx: oneshot::Sender<Result<(), Error>>,
    ) {
        let uuid = uuid::Uuid::new_v4();
        let term = self.term();
        let read_context = match request.context {
            None => vec![],
            Some(ctx) => ctx.encode_length_delimited_to_vec(),
        };

        self.raft_group.read_index(read_context);

        let proposal = ReadIndexProposal {
            uuid,
            read_index: None,
            context: None,
            tx: Some(tx),
        };
    }
}
