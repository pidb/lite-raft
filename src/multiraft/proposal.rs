use std::collections::HashMap;
use std::collections::VecDeque;
use std::fmt::Debug;

use tokio::sync::oneshot;
use uuid::Uuid;

use super::error::Error;
use super::error::ProposalError;

use crate::proto::ReadIndexContext;

pub struct ReadIndexProposal<R: Debug> {
    pub uuid: Uuid,
    pub read_index: Option<u64>,
    pub context: Option<ReadIndexContext>,
    // if some, the R is sent to client via tx.
    pub tx: Option<oneshot::Sender<Result<R, Error>>>,
}

const SHRINK_CACHE_CAPACITY: usize = 64;

#[derive(Debug)]
pub struct Proposal<R: Debug> {
    // index when proposing to raft group
    pub index: u64,
    // current term when proposing to raft group.
    pub term: u64,
    // true if proposal is conf change type.
    pub is_conf_change: bool,
    // if some, the R is sent to client via tx.
    pub tx: Option<oneshot::Sender<Result<R, Error>>>,
}

#[derive(Debug)]
pub struct GroupProposalQueue<R: Debug> {
    pub replica_id: u64,
    pub queue: VecDeque<Proposal<R>>,
}

impl<R: Debug> GroupProposalQueue<R> {
    pub fn new(replica_id: u64) -> Self {
        GroupProposalQueue {
            replica_id,
            queue: VecDeque::new(),
        }
    }

    pub fn push(&mut self, proposal: Proposal<R>) -> Result<(), Error> {
        if let Some(last) = self.queue.back() {
            // The term must be increasing among all log entries and the index
            // must be increasing inside a given term
            if proposal.term < last.term {
                return Err(Error::Proposal(ProposalError::Stale(proposal.term)));
            }

            if proposal.index < last.index {
                return Err(Error::Proposal(ProposalError::Unexpected(proposal.index)));
            }
        }

        self.queue.push_back(proposal);
        Ok(())
    }

    /// Find proposal from the queue front according to the term and index. 
    /// If the proposal (term, ndex) of the queue front is greater than the 
    /// (term, index) parameter, None is returned. 
    fn pop(&mut self, term: u64, index: u64) -> Option<Proposal<R>> {
        self.queue.pop_front().and_then(|p| {
            // Comparing the term first then the index, because the term is
            // increasing among all log entries and the index is increasing
            // inside a given term

            // if term < p.term then is stable, we pop it.
            if (p.term, p.index) > (term, index) {
                self.queue.push_front(p);
                return None;
            }

            Some(p)
        })
    }

    /// Find proposal from the queue front according to the term and index. 
    /// If the proposal (term, ndex) of the queue front is greater than the 
    /// (term, index) parameter, None is returned. 
    /// If term is less than the proposal's term, the stale response is returned 
    /// and if index is less than the proposal's index, unexpected is returned
    pub fn find_proposal(
        &mut self,
        term: u64,
        index: u64,
        current_term: u64,
    ) -> Result<Option<Proposal<R>>, Error> {
        while let Some(proposal) = self.pop(term, index) {
            if proposal.term == term {
                // term matched.
                if proposal.index == index {
                    return Ok(Some(proposal));
                } else {
                    return Err(Error::Proposal(ProposalError::Unexpected(index)));
                }
            } else {
                proposal
                    .tx
                    .map(|tx| tx.send(Err(Error::Proposal(ProposalError::Stale(proposal.term)))));
                return Err(Error::Proposal(ProposalError::Stale(proposal.term)));
            }
        }

        Ok(None)
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn shrink(&mut self) {
        if self.queue.capacity() > SHRINK_CACHE_CAPACITY && self.queue.len() < SHRINK_CACHE_CAPACITY
        {
            self.queue.shrink_to_fit();
        }
    }
}

#[derive(Default, Debug)]
pub struct ProposalQueueManager<R: Debug> {
    groups: HashMap<u64, GroupProposalQueue<R>>,
}

impl<R: Debug> ProposalQueueManager<R> {
    pub fn create_group_proposal_queue<'a>(
        &'a mut self,
        group_id: u64,
        replica_id: u64,
    ) -> &'a mut GroupProposalQueue<R> {
        match self
            .groups
            .insert(group_id, GroupProposalQueue::new(replica_id))
        {
            None => {}
            Some(_) => panic!(
                "the previous proposal queue of group ({})  already exists",
                group_id
            ),
        }
        self.groups.get_mut(&group_id).unwrap()
    }

    #[inline]
    pub fn group_proposal_queue<'a>(&'a self, group_id: &u64) -> Option<&'a GroupProposalQueue<R>> {
        self.groups.get(group_id)
    }

    #[inline]
    pub fn mut_group_proposal_queue<'a>(
        &'a mut self,
        group_id: &u64,
    ) -> Option<&'a mut GroupProposalQueue<R>> {
        self.groups.get_mut(group_id)
    }

    #[inline]
    pub fn remove_group_proposal_queue<'a>(
        &'a mut self,
        group_id: &u64,
    ) -> Option<GroupProposalQueue<R>> {
        self.groups.remove(group_id)
    }
}

#[test]
fn test_proposal_queue() {
    let group_id = 1;
    let replica_id = 1;
    let (tx, _) = oneshot::channel();
    let proposals = vec![
        (1, 1, false, None, Ok(())),
        (2, 1, false, None, Ok(())),
        (3, 2, false, None, Ok(())),
        (
            4,
            1,
            false,
            Some(tx),
            Err(Error::Proposal(ProposalError::Stale(1))),
        ),
        (4, 2, false, None, Ok(())),
        (
            3,
            2,
            false,
            None,
            Err(Error::Proposal(ProposalError::Unexpected(3))),
        ),
    ];
    let mut mgr = ProposalQueueManager::default();
    let gq = mgr.create_group_proposal_queue(group_id, replica_id);

    // assert push
    for (index, term, is_conf_change, tx, result) in proposals.into_iter() {
        assert_eq!(
            gq.push(Proposal::<()> {
                index,
                term,
                is_conf_change,
                tx,
            }),
            result
        )
    }

    // expected proposals
    let proposals = vec![(1, 1), (2, 1), (3, 2), (4, 2)];

    // assert pop
    for (index, term) in proposals.iter() {
        let proposal = gq.pop(*term, *index).unwrap();
        assert_eq!(proposal.index, *index);
        assert!(proposal.term <= *term);
    }
}

#[test]
fn test_proposal_queue_find() {
    let group_id = 1;
    let replica_id = 1;
    let proposals = vec![
        (1, 1, false, None),
        (2, 1, false, None),
        (3, 2, false, None),
        (4, 2, false, None),
    ];
    let mut mgr = ProposalQueueManager::default();
    let gq = mgr.create_group_proposal_queue(group_id, replica_id);
    for (index, term, is_conf_change, tx) in proposals.into_iter() {
        gq.push(Proposal::<()> {
            index,
            term,
            is_conf_change,
            tx,
        })
        .unwrap();
    }

    let proposals = vec![
        (1, 1, Ok(Some((1, 1)))),
        (2, 1, Ok(Some((2, 1)))),
        (3, 1, Ok(None)), // expection due to term stale
        (3, 3, Err(Error::Proposal(ProposalError::Stale(2)))), // expection to due index out of range, should 3,2
        (5, 2, Err(Error::Proposal(ProposalError::Unexpected(5)))), // expection to due index out of range, should 4, 2
    ];

    // assert find_proposal
    let current_term = 2;
    for (index, term, result) in proposals.iter() {
        let proposal = gq
            .find_proposal(*term, *index, current_term)
            .map(|p| p.map(|p| (p.index, p.term)));

        assert_eq!(proposal, *result);
    }
}
