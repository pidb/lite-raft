use std::collections::vec_deque::Drain;
use std::collections::VecDeque;
use std::fmt::Debug;

use prost::Message;
use raft::ReadState;
use tokio::sync::oneshot;
use tracing::debug;
use tracing::error;
use uuid::Uuid;

use crate::prelude::ReadIndexContext;

use super::error::Error;
use super::error::WriteError;
use super::response::AppWriteResponse;

/// Shrink queue if queue capacity more than and len less than
/// this value.
const SHRINK_CACHE_CAPACITY: usize = 64;

pub struct ReadIndexProposal {
    pub uuid: Uuid,
    pub read_index: Option<u64>,
    pub context: Option<ReadIndexContext>,
    // if some, the R is sent to client via tx.
    pub tx: Option<oneshot::Sender<Result<(), Error>>>,
}

pub struct ReadIndexQueue {
    ready_cnt: usize,
    handle_cnt: usize,
    queue: VecDeque<ReadIndexProposal>,
}

impl ReadIndexQueue {
    pub fn new() -> ReadIndexQueue {
        Self {
            ready_cnt: 0,
            handle_cnt: 0,
            queue: VecDeque::new(),
        }
    }

    #[inline]
    #[allow(unused)]
    pub fn push_front(&mut self, proposal: ReadIndexProposal) {
        self.queue.push_back(proposal);
        self.ready_cnt += 1;
        self.handle_cnt -= 1;
    }

    #[inline]
    pub(crate) fn push_back(&mut self, proposal: ReadIndexProposal) {
        self.queue.push_back(proposal)
    }

    fn try_gc(&mut self) {
        // TODO: think move the shrink_to_fit operation  to background task?
        if self.queue.capacity() > SHRINK_CACHE_CAPACITY && self.queue.len() < SHRINK_CACHE_CAPACITY
        {
            self.queue.shrink_to_fit();
        }
    }
    pub(crate) fn pop_front(&mut self) -> Option<ReadIndexProposal> {
        if self.ready_cnt == 0 {
            return None;
        }

        self.ready_cnt -= 1;
        self.handle_cnt += 1;
        let item = self
            .queue
            .pop_front()
            .expect("read index queue empty but ready_cnt > 0");
        self.try_gc();
        Some(item)
    }

    pub(crate) fn advance_reads(&mut self, rss: Vec<ReadState>) {
        for rs in rss {
            let mut rctx = ReadIndexContext::default();
            let _ = rctx
                .merge_length_delimited(rs.request_ctx.as_ref())
                .unwrap();

            let rctx_uuid = Uuid::from_bytes(rctx.get_uuid().try_into().unwrap());
            // let rctx_uuid = Uuid::parse(&rctx.uuid).unwrap();
            match self.queue.get_mut(self.ready_cnt) {
                Some(read) if read.uuid == rctx_uuid => {
                    read.read_index = Some(rs.index);
                    read.context = Some(rctx);
                    self.ready_cnt += 1;
                }
                Some(read) => error!("unexpected uuid {} detected", read.uuid),
                None => error!("ready read {} but can not got related proposal", rs.index),
            }
        }
    }
}

#[derive(Debug)]
pub struct Proposal<RES: AppWriteResponse> {
    // index when proposing to raft group
    pub index: u64,
    // current term when proposing to raft group.
    pub term: u64,
    // true if proposal is conf change type.
    pub is_conf_change: bool,
    // if some, the R is sent to client via tx.
    pub tx: Option<oneshot::Sender<Result<RES, Error>>>,
}

#[derive(Debug)]
pub struct ProposalQueue<RES: AppWriteResponse> {
    pub replica_id: u64,
    pub queue: VecDeque<Proposal<RES>>,
}

impl<RES: AppWriteResponse> ProposalQueue<RES> {
    pub fn new(replica_id: u64) -> Self {
        ProposalQueue {
            replica_id,
            queue: VecDeque::new(),
        }
    }

    pub fn push(&mut self, proposal: Proposal<RES>) {
        if let Some(last) = self.queue.back() {
            // The term must be increasing among all log entries and the index
            // must be increasing inside a given term
            if proposal.term < last.term {
                panic!(
                    "bad proposal due to term jump backword {} -> {}",
                    last.term, proposal.term
                );
            }

            if proposal.index < last.index {
                panic!(
                    "bad proposal due to index jump backword {} -> {}",
                    last.index, proposal.index
                );
            }
        }

        self.queue.push_back(proposal);
    }

    fn try_gc(&mut self) {
        // TODO: think move the shrink_to_fit operation  to background task?
        if self.queue.capacity() > SHRINK_CACHE_CAPACITY && self.queue.len() < SHRINK_CACHE_CAPACITY
        {
            self.queue.shrink_to_fit();
        }
    }

    #[inline]
    pub fn drain<R>(&mut self, range: R) -> Drain<'_, Proposal<RES>>
    where
        R: std::ops::RangeBounds<usize>,
    {
        self.queue.drain(range)
    }

    /// Find proposal from the queue front according to the term and index.
    /// If the proposal (term, ndex) of the queue front is greater than the
    /// (term, index) parameter, None is returned.
    fn pop(&mut self, term: u64, index: u64) -> Option<Proposal<RES>> {
        self.queue.pop_front().and_then(|p| {
            self.try_gc();
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
    ) -> Option<Proposal<RES>> {
        while let Some(proposal) = self.pop(term, index) {
            if proposal.term == term {
                debug!("find proposal index {} = {}", proposal.index, index);
                // term matched.
                if proposal.index == index {
                    return Some(proposal);
                } else {
                    return None;
                }
            } else {
                proposal.tx.map(|tx| {
                    tx.send(Err(Error::Write(WriteError::Stale(
                        proposal.term,
                        current_term,
                    ))))
                });
                return None;
            }
        }

        None
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}

// #[test]
// fn test_proposal_queue() {
//     let group_id = 1;
//     let replica_id = 1;
//     let (tx, _) = oneshot::channel::<Result<(), Error>>();
//     let proposals = vec![
//         (1, 1, false, None, Ok(())),
//         (2, 1, false, None, Ok(())),
//         (3, 2, false, None, Ok(())),
//         (
//             4,
//             1,
//             false,
//             Some(tx),
//             Err(Error::Write(WriteError::Stale(1, 1))),
//         ),
//         (4, 2, false, None, Ok(())),
//         (
//             3,
//             2,
//             false,
//             None,
//             None,
//         ),
//     ];
//     let mut mgr = ProposalQueueManager::default();
//     let gq = mgr.create_group_proposal_queue(group_id, replica_id);

//     // assert push
//     for (index, term, is_conf_change, tx, result) in proposals.into_iter() {
//         gq.push(Proposal {
//             index,
//             term,
//             is_conf_change,
//             tx,
//         })
//     }

//     // expected proposals
//     let proposals = vec![(1, 1), (2, 1), (3, 2), (4, 2)];

//     // assert pop
//     for (index, term) in proposals.iter() {
//         let proposal = gq.pop(*term, *index).unwrap();
//         assert_eq!(proposal.index, *index);
//         assert!(proposal.term <= *term);
//     }
// }

// #[test]
// fn test_proposal_queue_find() {
//     let group_id = 1;
//     let replica_id = 1;
//     let proposals = vec![
//         (1, 1, false, None),
//         (2, 1, false, None),
//         (3, 2, false, None),
//         (4, 2, false, None),
//     ];
//     let mut mgr = ProposalQueueManager::<()>::default();
//     let gq = mgr.create_group_proposal_queue(group_id, replica_id);
//     for (index, term, is_conf_change, tx) in proposals.into_iter() {
//         gq.push(Proposal {
//             index,
//             term,
//             is_conf_change,
//             tx,
//         })
//     }

//     let proposals = vec![
//         (1, 1, Some((1, 1))),
//         (2, 1, Some((2, 1))),
//         (3, 1, None), // expection due to term stale
//         // (3, 3, Error::Proposal(ProposalError::Stale(2, 2))), // expection to due index out of range, should 3,2
//         // (5, 2, Error::Proposal(ProposalError::Unexpected(5))), // expection to due index out of range, should 4, 2
//         (3, 3, None), // expection to due index out of range, should 3,2
//         (5, 2, None), // expection to due index out of range, should 4, 2
//     ];

//     // TODO: re-impl
//     // assert find_proposal
//     // let current_term = 2;
//     // for (index, term, result) in proposals.iter() {
//     //     let proposal = gq
//     //         .find_proposal(*term, *index, current_term);

//     //     assert_eq!(proposal, *result);
//     // }
// }
