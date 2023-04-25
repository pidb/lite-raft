#![feature(impl_trait_in_assoc_type)]

#[allow(dead_code)]
#[allow(unknown_lints)]
#[allow(clippy::all)]
#[allow(renamed_and_removed_lints)]
#[allow(bare_trait_objects)]
mod protos {
    include!(concat!(env!("OUT_DIR"), "/multiraft.rs"));
    use raft::prelude::*;

    impl ReplicaDesc {
        pub fn new_() -> ReplicaDesc {
            ::std::default::Default::default()
        }

        #[inline]
        pub fn default_ref() -> &'static Self {
            ::protobuf::Message::default_instance()
        }

        #[inline]
        pub fn clear_node_id(&mut self) {
            self.node_id = 0
        }

        #[inline]
        pub fn set_node_id(&mut self, v: u64) {
            self.node_id = v;
        }

        #[inline]
        pub fn get_node_id(&self) -> u64 {
            self.node_id
        }

        #[inline]
        pub fn clear_replica_id(&mut self) {
            self.replica_id = 0
        }

        #[inline]
        pub fn set_replica_id(&mut self, v: u64) {
            self.replica_id = v;
        }

        #[inline]
        pub fn get_replica_id(&self) -> u64 {
            self.replica_id
        }
    }

    impl ::protobuf::Clear for ReplicaDesc {
        fn clear(&mut self) {
            ::prost::Message::clear(self);
        }
    }

    impl ::protobuf::Message for ReplicaDesc {
        fn compute_size(&self) -> u32 {
            ::prost::Message::encoded_len(self) as u32
        }

        fn get_cached_size(&self) -> u32 {
            ::prost::Message::encoded_len(self) as u32
        }

        fn as_any(&self) -> &dyn ::std::any::Any {
            self as &dyn ::std::any::Any
        }

        fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
            Self::descriptor_static()
        }

        fn new() -> Self {
            Self::default()
        }

        fn default_instance() -> &'static ReplicaDesc {
            ::lazy_static::lazy_static! {
                static ref INSTANCE: ReplicaDesc = ReplicaDesc::default();
            }
            &*INSTANCE
        }

        fn is_initialized(&self) -> bool {
            true
        }

        fn write_to_with_cached_sizes(
            &self,
            _os: &mut ::protobuf::CodedOutputStream,
        ) -> ::protobuf::ProtobufResult<()> {
            unimplemented!();
        }

        fn merge_from(
            &mut self,
            _is: &mut ::protobuf::CodedInputStream,
        ) -> ::protobuf::ProtobufResult<()> {
            unimplemented!();
        }

        fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
            unimplemented!();
        }

        fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
            unimplemented!();
        }

        fn write_to_bytes(&self) -> ::protobuf::ProtobufResult<Vec<u8>> {
            let mut buf = Vec::new();
            if ::prost::Message::encode(self, &mut buf).is_err() {
                return Err(::protobuf::ProtobufError::WireError(
                    ::protobuf::error::WireError::Other,
                ));
            }
            Ok(buf)
        }

        fn merge_from_bytes(&mut self, bytes: &[u8]) -> ::protobuf::ProtobufResult<()> {
            if ::prost::Message::merge(self, bytes).is_err() {
                return Err(::protobuf::ProtobufError::WireError(
                    ::protobuf::error::WireError::Other,
                ));
            }
            Ok(())
        }
    }

    impl MultiRaftMessage {
        pub fn new_() -> MultiRaftMessage {
            ::std::default::Default::default()
        }

        #[inline]
        pub fn default_ref() -> &'static Self {
            ::protobuf::Message::default_instance()
        }

        #[inline]
        pub fn clear_group_id(&mut self) {
            self.group_id = 0
        }

        #[inline]
        pub fn set_group_id(&mut self, v: u64) {
            self.group_id = v;
        }

        #[inline]
        pub fn get_group_id(&self) -> u64 {
            self.group_id
        }

        #[inline]
        pub fn clear_from_node(&mut self) {
            self.from_node = 0
        }

        #[inline]
        pub fn set_from_node(&mut self, v: u64) {
            self.from_node = v;
        }

        #[inline]
        pub fn get_from_node(&self) -> u64 {
            self.from_node
        }

        #[inline]
        pub fn clear_to_node(&mut self) {
            self.to_node = 0
        }

        #[inline]
        pub fn set_to_node(&mut self, v: u64) {
            self.to_node = v;
        }

        #[inline]
        pub fn get_to_node(&self) -> u64 {
            self.to_node
        }

        #[inline]
        pub fn clear_replicas(&mut self) {
            self.replicas.clear();
        }

        #[inline]
        pub fn set_replicas(&mut self, v: ::std::vec::Vec<ReplicaDesc>) {
            self.replicas = v;
        }

        #[inline]
        pub fn get_replicas(&self) -> &[ReplicaDesc] {
            &self.replicas
        }

        #[inline]
        pub fn mut_replicas(&mut self) -> &mut ::std::vec::Vec<ReplicaDesc> {
            &mut self.replicas
        }

        #[inline]
        pub fn take_replicas(&mut self) -> ::std::vec::Vec<ReplicaDesc> {
            ::std::mem::replace(&mut self.replicas, ::std::vec::Vec::new())
        }

        #[inline]
        pub fn has_msg(&self) -> bool {
            self.msg.is_some()
        }

        #[inline]
        pub fn clear_msg(&mut self) {
            self.msg = ::std::option::Option::None
        }

        #[inline]
        pub fn set_msg(&mut self, v: Message) {
            self.msg = ::std::option::Option::Some(v);
        }

        #[inline]
        pub fn get_msg(&self) -> &Message {
            match self.msg.as_ref() {
                Some(v) => v,
                None => Message::default_ref(),
            }
        }

        #[inline]
        pub fn mut_msg(&mut self) -> &mut Message {
            if self.msg.is_none() {
                self.msg = ::std::option::Option::Some(Message::default());
            }
            self.msg.as_mut().unwrap()
        }

        #[inline]
        pub fn take_msg(&mut self) -> Message {
            self.msg.take().unwrap_or_else(Message::default)
        }
    }

    impl ::protobuf::Clear for MultiRaftMessage {
        fn clear(&mut self) {
            ::prost::Message::clear(self);
        }
    }

    impl ::protobuf::Message for MultiRaftMessage {
        fn compute_size(&self) -> u32 {
            ::prost::Message::encoded_len(self) as u32
        }

        fn get_cached_size(&self) -> u32 {
            ::prost::Message::encoded_len(self) as u32
        }

        fn as_any(&self) -> &dyn ::std::any::Any {
            self as &dyn ::std::any::Any
        }

        fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
            Self::descriptor_static()
        }

        fn new() -> Self {
            Self::default()
        }

        fn default_instance() -> &'static MultiRaftMessage {
            ::lazy_static::lazy_static! {
                static ref INSTANCE: MultiRaftMessage = MultiRaftMessage::default();
            }
            &*INSTANCE
        }

        fn is_initialized(&self) -> bool {
            true
        }

        fn write_to_with_cached_sizes(
            &self,
            _os: &mut ::protobuf::CodedOutputStream,
        ) -> ::protobuf::ProtobufResult<()> {
            unimplemented!();
        }

        fn merge_from(
            &mut self,
            _is: &mut ::protobuf::CodedInputStream,
        ) -> ::protobuf::ProtobufResult<()> {
            unimplemented!();
        }

        fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
            unimplemented!();
        }

        fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
            unimplemented!();
        }

        fn write_to_bytes(&self) -> ::protobuf::ProtobufResult<Vec<u8>> {
            let mut buf = Vec::new();
            if ::prost::Message::encode(self, &mut buf).is_err() {
                return Err(::protobuf::ProtobufError::WireError(
                    ::protobuf::error::WireError::Other,
                ));
            }
            Ok(buf)
        }

        fn merge_from_bytes(&mut self, bytes: &[u8]) -> ::protobuf::ProtobufResult<()> {
            if ::prost::Message::merge(self, bytes).is_err() {
                return Err(::protobuf::ProtobufError::WireError(
                    ::protobuf::error::WireError::Other,
                ));
            }
            Ok(())
        }
    }

    impl MultiRaftMessageResponse {
        pub fn new_() -> MultiRaftMessageResponse {
            ::std::default::Default::default()
        }

        #[inline]
        pub fn default_ref() -> &'static Self {
            ::protobuf::Message::default_instance()
        }
    }

    impl ::protobuf::Clear for MultiRaftMessageResponse {
        fn clear(&mut self) {
            ::prost::Message::clear(self);
        }
    }

    impl ::protobuf::Message for MultiRaftMessageResponse {
        fn compute_size(&self) -> u32 {
            ::prost::Message::encoded_len(self) as u32
        }

        fn get_cached_size(&self) -> u32 {
            ::prost::Message::encoded_len(self) as u32
        }

        fn as_any(&self) -> &dyn ::std::any::Any {
            self as &dyn ::std::any::Any
        }

        fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
            Self::descriptor_static()
        }

        fn new() -> Self {
            Self::default()
        }

        fn default_instance() -> &'static MultiRaftMessageResponse {
            ::lazy_static::lazy_static! {
                static ref INSTANCE: MultiRaftMessageResponse = MultiRaftMessageResponse::default();
            }
            &*INSTANCE
        }

        fn is_initialized(&self) -> bool {
            true
        }

        fn write_to_with_cached_sizes(
            &self,
            _os: &mut ::protobuf::CodedOutputStream,
        ) -> ::protobuf::ProtobufResult<()> {
            unimplemented!();
        }

        fn merge_from(
            &mut self,
            _is: &mut ::protobuf::CodedInputStream,
        ) -> ::protobuf::ProtobufResult<()> {
            unimplemented!();
        }

        fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
            unimplemented!();
        }

        fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
            unimplemented!();
        }

        fn write_to_bytes(&self) -> ::protobuf::ProtobufResult<Vec<u8>> {
            let mut buf = Vec::new();
            if ::prost::Message::encode(self, &mut buf).is_err() {
                return Err(::protobuf::ProtobufError::WireError(
                    ::protobuf::error::WireError::Other,
                ));
            }
            Ok(buf)
        }

        fn merge_from_bytes(&mut self, bytes: &[u8]) -> ::protobuf::ProtobufResult<()> {
            if ::prost::Message::merge(self, bytes).is_err() {
                return Err(::protobuf::ProtobufError::WireError(
                    ::protobuf::error::WireError::Other,
                ));
            }
            Ok(())
        }
    }

    impl SingleMembershipChange {
        pub fn new_() -> SingleMembershipChange {
            ::std::default::Default::default()
        }

        #[inline]
        pub fn default_ref() -> &'static Self {
            ::protobuf::Message::default_instance()
        }

        #[inline]
        pub fn clear_node_id(&mut self) {
            self.node_id = 0
        }

        #[inline]
        pub fn set_node_id(&mut self, v: u64) {
            self.node_id = v;
        }

        #[inline]
        pub fn get_node_id(&self) -> u64 {
            self.node_id
        }

        #[inline]
        pub fn clear_replica_id(&mut self) {
            self.replica_id = 0
        }

        #[inline]
        pub fn set_replica_id(&mut self, v: u64) {
            self.replica_id = v;
        }

        #[inline]
        pub fn get_replica_id(&self) -> u64 {
            self.replica_id
        }

        #[inline]
        pub fn clear_change_type(&mut self) {
            self.change_type = 0
        }

        #[inline]
        pub fn get_change_type(&self) -> ConfChangeType {
            match ConfChangeType::from_i32(self.change_type) {
                Some(e) => e,
                None => panic!("Unknown enum variant: {}", self.change_type),
            }
        }
    }

    impl ::protobuf::Clear for SingleMembershipChange {
        fn clear(&mut self) {
            ::prost::Message::clear(self);
        }
    }

    impl ::protobuf::Message for SingleMembershipChange {
        fn compute_size(&self) -> u32 {
            ::prost::Message::encoded_len(self) as u32
        }

        fn get_cached_size(&self) -> u32 {
            ::prost::Message::encoded_len(self) as u32
        }

        fn as_any(&self) -> &dyn ::std::any::Any {
            self as &dyn ::std::any::Any
        }

        fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
            Self::descriptor_static()
        }

        fn new() -> Self {
            Self::default()
        }

        fn default_instance() -> &'static SingleMembershipChange {
            ::lazy_static::lazy_static! {
                static ref INSTANCE: SingleMembershipChange = SingleMembershipChange::default();
            }
            &*INSTANCE
        }

        fn is_initialized(&self) -> bool {
            true
        }

        fn write_to_with_cached_sizes(
            &self,
            _os: &mut ::protobuf::CodedOutputStream,
        ) -> ::protobuf::ProtobufResult<()> {
            unimplemented!();
        }

        fn merge_from(
            &mut self,
            _is: &mut ::protobuf::CodedInputStream,
        ) -> ::protobuf::ProtobufResult<()> {
            unimplemented!();
        }

        fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
            unimplemented!();
        }

        fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
            unimplemented!();
        }

        fn write_to_bytes(&self) -> ::protobuf::ProtobufResult<Vec<u8>> {
            let mut buf = Vec::new();
            if ::prost::Message::encode(self, &mut buf).is_err() {
                return Err(::protobuf::ProtobufError::WireError(
                    ::protobuf::error::WireError::Other,
                ));
            }
            Ok(buf)
        }

        fn merge_from_bytes(&mut self, bytes: &[u8]) -> ::protobuf::ProtobufResult<()> {
            if ::prost::Message::merge(self, bytes).is_err() {
                return Err(::protobuf::ProtobufError::WireError(
                    ::protobuf::error::WireError::Other,
                ));
            }
            Ok(())
        }
    }

    impl MembershipChangeData {
        pub fn new_() -> MembershipChangeData {
            ::std::default::Default::default()
        }

        #[inline]
        pub fn default_ref() -> &'static Self {
            ::protobuf::Message::default_instance()
        }

        #[inline]
        pub fn clear_changes(&mut self) {
            self.changes.clear();
        }

        #[inline]
        pub fn set_changes(&mut self, v: ::std::vec::Vec<SingleMembershipChange>) {
            self.changes = v;
        }

        #[inline]
        pub fn get_changes(&self) -> &[SingleMembershipChange] {
            &self.changes
        }

        #[inline]
        pub fn mut_changes(&mut self) -> &mut ::std::vec::Vec<SingleMembershipChange> {
            &mut self.changes
        }

        #[inline]
        pub fn take_changes(&mut self) -> ::std::vec::Vec<SingleMembershipChange> {
            ::std::mem::replace(&mut self.changes, ::std::vec::Vec::new())
        }

        #[inline]
        pub fn clear_replicas(&mut self) {
            self.replicas.clear();
        }

        #[inline]
        pub fn set_replicas(&mut self, v: ::std::vec::Vec<ReplicaDesc>) {
            self.replicas = v;
        }

        #[inline]
        pub fn get_replicas(&self) -> &[ReplicaDesc] {
            &self.replicas
        }

        #[inline]
        pub fn mut_replicas(&mut self) -> &mut ::std::vec::Vec<ReplicaDesc> {
            &mut self.replicas
        }

        #[inline]
        pub fn take_replicas(&mut self) -> ::std::vec::Vec<ReplicaDesc> {
            ::std::mem::replace(&mut self.replicas, ::std::vec::Vec::new())
        }
    }

    impl ::protobuf::Clear for MembershipChangeData {
        fn clear(&mut self) {
            ::prost::Message::clear(self);
        }
    }

    impl ::protobuf::Message for MembershipChangeData {
        fn compute_size(&self) -> u32 {
            ::prost::Message::encoded_len(self) as u32
        }

        fn get_cached_size(&self) -> u32 {
            ::prost::Message::encoded_len(self) as u32
        }

        fn as_any(&self) -> &dyn ::std::any::Any {
            self as &dyn ::std::any::Any
        }

        fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
            Self::descriptor_static()
        }

        fn new() -> Self {
            Self::default()
        }

        fn default_instance() -> &'static MembershipChangeData {
            ::lazy_static::lazy_static! {
                static ref INSTANCE: MembershipChangeData = MembershipChangeData::default();
            }
            &*INSTANCE
        }

        fn is_initialized(&self) -> bool {
            true
        }

        fn write_to_with_cached_sizes(
            &self,
            _os: &mut ::protobuf::CodedOutputStream,
        ) -> ::protobuf::ProtobufResult<()> {
            unimplemented!();
        }

        fn merge_from(
            &mut self,
            _is: &mut ::protobuf::CodedInputStream,
        ) -> ::protobuf::ProtobufResult<()> {
            unimplemented!();
        }

        fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
            unimplemented!();
        }

        fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
            unimplemented!();
        }

        fn write_to_bytes(&self) -> ::protobuf::ProtobufResult<Vec<u8>> {
            let mut buf = Vec::new();
            if ::prost::Message::encode(self, &mut buf).is_err() {
                return Err(::protobuf::ProtobufError::WireError(
                    ::protobuf::error::WireError::Other,
                ));
            }
            Ok(buf)
        }

        fn merge_from_bytes(&mut self, bytes: &[u8]) -> ::protobuf::ProtobufResult<()> {
            if ::prost::Message::merge(self, bytes).is_err() {
                return Err(::protobuf::ProtobufError::WireError(
                    ::protobuf::error::WireError::Other,
                ));
            }
            Ok(())
        }
    }
}
pub mod prelude {
    // pub use crate::multiraft::MultiRaft;
    pub use crate::protos::*;
    pub use raft::prelude::*;
}

mod apply;
mod config;
mod error;
mod event;
mod group;
mod msg;
mod multiraft;
mod node;
mod proposal;
mod replica_cache;
mod rsm;
mod state;

pub mod storage;
pub mod task_group;
pub mod tick;
pub mod transport;
pub mod utils;

pub use config::Config;
pub use error::{Error, MultiRaftStorageError, ProposeError, RaftCoreError, RaftGroupError};
pub use event::{Event, LeaderElectionEvent};
pub use multiraft::{
    MultiRaft, MultiRaftMessageSender, MultiRaftMessageSenderImpl, MultiRaftTypeSpecialization,
    ProposeData, ProposeResponse,
};
pub use rsm::{Apply, ApplyMembership, ApplyNoOp, ApplyNormal, StateMachine};
pub use state::{GroupState, GroupStates};
