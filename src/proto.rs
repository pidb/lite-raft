use std::mem::transmute;

include!(concat!(env!("OUT_DIR"), "/pirate.rs"));

//----------------------------------------------------------------------
// Static default reference
//----------------------------------------------------------------------
lazy_static::lazy_static! {
    static ref DEFAULT_RAFT_CONF_STATE_REF: ConfState = {
        ConfState::default()
    };

    static ref DEFAULT_RAFT_SNAPSHOT_REF: Snapshot = {
        Snapshot::default()
    };

    static ref DEFAULT_RAFT_SNAPSHOT_METADATA_REF: SnapshotMetadata = {
        SnapshotMetadata::default()
    };
}

impl ConfState {
    pub fn new() -> ConfState {
        ::std::default::Default::default()
    }
    #[inline]
    pub fn default_ref() -> &'static Self {
        &DEFAULT_RAFT_CONF_STATE_REF
    }
    #[inline]
    pub fn clear_voters(&mut self) {
        self.voters.clear();
    }
    #[inline]
    pub fn set_voters(&mut self, v: ::std::vec::Vec<u64>) {
        self.voters = v;
    }
    #[inline]
    pub fn get_voters(&self) -> &[u64] {
        &self.voters
    }
    #[inline]
    pub fn mut_voters(&mut self) -> &mut ::std::vec::Vec<u64> {
        &mut self.voters
    }
    #[inline]
    pub fn take_voters(&mut self) -> ::std::vec::Vec<u64> {
        ::std::mem::replace(&mut self.voters, ::std::vec::Vec::new())
    }
    #[inline]
    pub fn clear_learners(&mut self) {
        self.learners.clear();
    }
    #[inline]
    pub fn set_learners(&mut self, v: ::std::vec::Vec<u64>) {
        self.learners = v;
    }
    #[inline]
    pub fn get_learners(&self) -> &[u64] {
        &self.learners
    }
    #[inline]
    pub fn mut_learners(&mut self) -> &mut ::std::vec::Vec<u64> {
        &mut self.learners
    }
    #[inline]
    pub fn take_learners(&mut self) -> ::std::vec::Vec<u64> {
        ::std::mem::replace(&mut self.learners, ::std::vec::Vec::new())
    }
    #[inline]
    pub fn clear_voters_outgoing(&mut self) {
        self.voters_outgoing.clear();
    }
    #[inline]
    pub fn set_voters_outgoing(&mut self, v: ::std::vec::Vec<u64>) {
        self.voters_outgoing = v;
    }
    #[inline]
    pub fn get_voters_outgoing(&self) -> &[u64] {
        &self.voters_outgoing
    }
    #[inline]
    pub fn mut_voters_outgoing(&mut self) -> &mut ::std::vec::Vec<u64> {
        &mut self.voters_outgoing
    }
    #[inline]
    pub fn take_voters_outgoing(&mut self) -> ::std::vec::Vec<u64> {
        ::std::mem::replace(&mut self.voters_outgoing, ::std::vec::Vec::new())
    }
    #[inline]
    pub fn clear_learners_next(&mut self) {
        self.learners_next.clear();
    }
    #[inline]
    pub fn set_learners_next(&mut self, v: ::std::vec::Vec<u64>) {
        self.learners_next = v;
    }
    #[inline]
    pub fn get_learners_next(&self) -> &[u64] {
        &self.learners_next
    }
    #[inline]
    pub fn mut_learners_next(&mut self) -> &mut ::std::vec::Vec<u64> {
        &mut self.learners_next
    }
    #[inline]
    pub fn take_learners_next(&mut self) -> ::std::vec::Vec<u64> {
        ::std::mem::replace(&mut self.learners_next, ::std::vec::Vec::new())
    }
    #[inline]
    pub fn clear_auto_leave(&mut self) {
        self.auto_leave = false
    }
    #[inline]
    pub fn set_auto_leave(&mut self, v: bool) {
        self.auto_leave = v;
    }
    #[inline]
    pub fn get_auto_leave(&self) -> bool {
        self.auto_leave
    }
}

impl From<(Vec<u64>, Vec<u64>)> for ConfState {
    fn from(value: (Vec<u64>, Vec<u64>)) -> Self {
        ConfState {
            voters: value.0,
            learners: value.1,
            ..Default::default()
        }
    }
}

impl SnapshotMetadata {
    pub fn new() -> SnapshotMetadata {
        ::std::default::Default::default()
    }
    #[inline]
    pub fn default_ref() -> &'static Self {
        &DEFAULT_RAFT_SNAPSHOT_METADATA_REF
    }
    #[inline]
    pub fn has_conf_state(&self) -> bool {
        self.conf_state.is_some()
    }
    #[inline]
    pub fn clear_conf_state(&mut self) {
        self.conf_state = ::std::option::Option::None
    }
    #[inline]
    pub fn set_conf_state(&mut self, v: ConfState) {
        self.conf_state = ::std::option::Option::Some(v);
    }
    #[inline]
    pub fn get_conf_state(&self) -> &ConfState {
        match self.conf_state.as_ref() {
            Some(v) => v,
            None => ConfState::default_ref(),
        }
    }
    #[inline]
    pub fn mut_conf_state(&mut self) -> &mut ConfState {
        if self.conf_state.is_none() {
            self.conf_state = ::std::option::Option::Some(ConfState::default());
        }
        self.conf_state.as_mut().unwrap()
    }
    #[inline]
    pub fn take_conf_state(&mut self) -> ConfState {
        self.conf_state.take().unwrap_or_else(ConfState::default)
    }
    #[inline]
    pub fn clear_index(&mut self) {
        self.index = 0
    }
    #[inline]
    pub fn set_index(&mut self, v: u64) {
        self.index = v;
    }
    #[inline]
    pub fn get_index(&self) -> u64 {
        self.index
    }
    #[inline]
    pub fn clear_term(&mut self) {
        self.term = 0
    }
    #[inline]
    pub fn set_term(&mut self, v: u64) {
        self.term = v;
    }
    #[inline]
    pub fn get_term(&self) -> u64 {
        self.term
    }
}

impl Snapshot {
    pub fn new() -> Snapshot {
        ::std::default::Default::default()
    }

    #[inline]
    pub fn default_ref() -> &'static Self {
        &DEFAULT_RAFT_SNAPSHOT_REF
    }
    #[inline]
    pub fn clear_data(&mut self) {
        self.data.clear();
    }
    #[inline]
    pub fn set_data(&mut self, v: ::prost::alloc::vec::Vec<u8>) {
        self.data = v;
    }
    #[inline]
    pub fn get_data(&self) -> &[u8] {
        &self.data
    }
    #[inline]
    pub fn mut_data(&mut self) -> &mut ::prost::alloc::vec::Vec<u8> {
        &mut self.data
    }
    #[inline]
    pub fn take_data(&mut self) -> ::prost::alloc::vec::Vec<u8> {
        ::std::mem::replace(&mut self.data, Default::default())
    }
    #[inline]
    pub fn has_metadata(&self) -> bool {
        self.metadata.is_some()
    }
    #[inline]
    pub fn clear_metadata(&mut self) {
        self.metadata = ::std::option::Option::None
    }
    #[inline]
    pub fn set_metadata(&mut self, v: SnapshotMetadata) {
        self.metadata = ::std::option::Option::Some(v);
    }
    #[inline]
    pub fn get_metadata(&self) -> &SnapshotMetadata {
        match self.metadata.as_ref() {
            Some(v) => v,
            None => SnapshotMetadata::default_ref(),
        }
    }
    #[inline]
    pub fn mut_metadata(&mut self) -> &mut SnapshotMetadata {
        if self.metadata.is_none() {
            self.metadata = ::std::option::Option::Some(SnapshotMetadata::default());
        }
        self.metadata.as_mut().unwrap()
    }
    #[inline]
    pub fn take_metadata(&mut self) -> SnapshotMetadata {
        self.metadata
            .take()
            .unwrap_or_else(SnapshotMetadata::default)
    }
}

impl Entry {
    #[inline]
    pub fn compute_size(&self) -> u64 {
        ::prost::Message::encoded_len(self) as u64
    }
}

// impl ::protobuf::Message for Entry {
//     fn is_initialized(&self) -> bool {
//         true
//     }

//     fn merge_from(&mut self, _is: &mut ::protobuf::CodedInputStream) -> ::protobuf::Result<()> {
//         unimplemented!();
//     }

//     fn parse_from(is: &mut protobuf::CodedInputStream) -> protobuf::Result<Self> {
//         unimplemented!()
//     }

//     fn compute_size(&self) -> u64 {
//         ::prost::Message::encoded_len(self) as u64
//     }

//     fn write_to_with_cached_sizes(
//         &self,
//         _os: &mut ::protobuf::CodedOutputStream,
//     ) -> ::protobuf::Result<()> {
//         unimplemented!();
//     }

//     fn cached_size(&self) -> u32 {
//         unimplemented!()
//     }

//     fn write_to(&self, os: &mut protobuf::CodedOutputStream) -> protobuf::Result<()> {
//         unimplemented!()
//     }

//     // fn get_cached_size(&self) -> u32 {
//     //     ::prost::Message::encoded_len(self) as u32
//     // }
//     // fn as_any(&self) -> &dyn ::std::any::Any {
//     //     self as &dyn ::std::any::Any
//     // }
//     // fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
//     //     Self::descriptor_static()
//     // }
//     fn new() -> Self {
//         Self::default()
//     }
//     fn default_instance() -> &'static Entry {
//         ::lazy_static::lazy_static! {
//             static ref INSTANCE: Entry = Entry::default();
//         }
//         &*INSTANCE
//     }

//     // fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
//     //     unimplemented!();
//     // }
//     fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
//         unimplemented!();
//     }
//     fn write_to_bytes(&self) -> ::protobuf::Result<Vec<u8>> {
//         let mut buf = Vec::new();
//         if ::prost::Message::encode(self, &mut buf).is_err() {
//             return Err(::protobuf::Error::WireError(
//                 ::protobuf::Error::WireError::Other,
//             ));
//         }
//         Ok(buf)
//     }
//     fn merge_from_bytes(&mut self, bytes: &[u8]) -> ::protobuf::Result<()> {
//         if ::prost::Message::merge(self, bytes).is_err() {
//             return Err(::protobuf::Error::WireError(
//                 ::protobuf::Error::WireError::Other,
//             ));
//         }
//         Ok(())
//     }
// }

/// A number to represent that there is no limit.
pub const NO_LIMIT: u64 = u64::MAX;

/// Truncates the list of entries down to a specific byte-length of
/// all entries together.
///
/// # Examples
///
/// ```
/// use raft::{util::limit_size, prelude::*};
///
/// let template = {
///     let mut entry = Entry::default();
///     entry.data = "*".repeat(100).into_bytes().into();
///     entry
/// };
///
/// // Make a bunch of entries that are ~100 bytes long
/// let mut entries = vec![
///     template.clone(),
///     template.clone(),
///     template.clone(),
///     template.clone(),
///     template.clone(),
/// ];
///
/// assert_eq!(entries.len(), 5);
/// limit_size(&mut entries, Some(220));
/// assert_eq!(entries.len(), 2);
///
/// // `entries` will always have at least 1 Message
/// limit_size(&mut entries, Some(0));
/// assert_eq!(entries.len(), 1);
/// ```
pub fn limit_entry_size(entries: &mut Vec<Entry>, max: Option<u64>) {
    if entries.len() <= 1 {
        return;
    }
    let max = match max {
        None | Some(NO_LIMIT) => return,
        Some(max) => max,
    };

    let mut size = 0;
    let limit = entries
        .iter()
        .take_while(|&e| {
            if size == 0 {
                size += u64::from(e.compute_size());
                return true;
            }
            size += u64::from(e.compute_size());
            size <= max
        })
        .count();

    entries.truncate(limit);
}


#[inline]
pub fn transmute_raft_messages(msgs: Vec<raft::prelude::Message>) -> Vec<Message> {
    unsafe { transmute(msgs) }
}

#[inline]
pub fn transmute_raft_hard_state(hs: raft::prelude::HardState) -> HardState {
    unsafe { transmute(hs) }
}

#[inline]
pub fn transmute_raft_snapshot(snapshot: raft::prelude::Snapshot) -> Snapshot {
    unsafe { transmute(snapshot) }
}

#[inline]
pub fn transmute_entries(entries: Vec<Entry>) -> Vec<raft::prelude::Entry> {
    unsafe { transmute(entries) }
}


#[inline]
pub fn transmute_raft_entries(entries: Vec<raft::prelude::Entry>) -> Vec<Entry> {
    unsafe { transmute(entries) }
}


#[inline]
pub fn transmute_raft_entries_ref<'a>(entries: &'a Vec<raft::prelude::Entry>) -> &'a Vec<Entry> {
    unsafe { std::mem::transmute(entries) }
}

#[inline]
pub fn transmute_raft_snapshot_metadata(
    snapshot_metadata: raft::prelude::SnapshotMetadata,
) -> SnapshotMetadata {
    unsafe { transmute(snapshot_metadata) }
}

