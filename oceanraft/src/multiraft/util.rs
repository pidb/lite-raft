use raft_proto::prelude::Entry;
use prost::Message;

#[inline]
pub fn compute_entry_size(ent: &Entry) -> usize {
    Message::encoded_len(ent)
}