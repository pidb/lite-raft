use flexbuffers::FlexbufferSerializer;
use flexbuffers::Reader;
use prost::Message;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;

use super::error::DeserializationError;
use super::error::SerializationError;
use super::prelude::Entry;
use super::Error;

/// Defers evaluation of a block of code until the end of the scope.
#[doc(hidden)]
#[macro_export]
macro_rules! defer {
    ($($body:tt)*) => {
        let _guard = {
            pub struct Guard<F: FnOnce()>(Option<F>);

            impl<F: FnOnce()> Drop for Guard<F> {
                fn drop(&mut self) {
                    (self.0).take().map(|f| f());
                }
            }

            Guard(Some(|| {
                let _ = { $($body)* };
            }))
        };
    };
}

/// This macro defines a struct and implements the `MultiRaftTypeSpecialization` trait for it.
/// It is used to define multiple associated types for the struct.
///
/// # Examples
///
/// ```rust
/// use oceanraft::MultiRaftTypeSpecialization;
/// use oceanraft::define_multiraft;
/// use oceanraft::storage::RockStoreCore;
/// use oceanraft::storage::RockStore;
/// define_multiraft!{
///     #[derive(Debug)]
///     pub struct MyMultiRaft:
///         // Define these associated types using this macro:
///         D = AppProposeData,
///         R = AppProposeResponse,
///         M = AppStateMachine,
///         S = RockStoreCore<AppSnapshotReader, AppSnapshotWriter>,
///         MS = RockStore<AppSnapshotReader, AppSnapshotWriter>
/// }
/// ```
#[doc(hidden)]
#[macro_export]
macro_rules! define_multiraft {
    // $type_meta provide comments, dervie and any other possible meta information for the type.
    // $type_vis provide visibility to the type, such as pub.
    // $type_name provide name id to the type.
    // $associated_type_name_def is the associated generic type defined by MultiRaftTypeSpecialization.
    // $associated_type_name_impl is associated generic type impl.
    // $associated_type_impl_meta for $type_name  provide comments, dervie and any other possible meta information for the type.
    ( $(#[$type_meta:meta])* $type_vis:vis $type_name:ident: $($(#[$associated_type_impl_meta:meta])* $associated_type_name_def:ident = $associated_type_name_impl:ty),+ ) => {
        $(#[$type_meta])*
        $type_vis struct $type_name {}

        impl $crate::MultiRaftTypeSpecialization for $type_name {
            $(
                $(#[$associated_type_impl_meta])*
                type $associated_type_name_def = $associated_type_name_impl;
            )+
        }
    };
}

/// Compute the entry size without a length delimiter with proto3.
#[inline]
pub fn compute_entry_size(ent: &Entry) -> usize {
    ent.encoded_len()
}

/// Compute the array of entries size without a length delimiter with proto3.
#[inline]
#[allow(unused)]
pub fn compute_entries_size(ents: &[Entry]) -> usize {
    ents.iter().map(|ent| ent.encoded_len()).sum()
}

/// Zero copy serialization using flexbuffer, data needs to implement `Serialize` trait.
/// If Ok, `FlexbufferSerializer` is returned and the user can call `take_buffer` to get
/// the data.
#[inline]
pub fn flexbuffer_serialize<S>(data: &S) -> Result<FlexbufferSerializer, Error>
where
    S: Serialize,
{
    let mut ser = FlexbufferSerializer::new();
    data.serialize(&mut ser)
        .map_err(|err| Error::Serialization(SerializationError::Flexbuffer(err)))?;
    Ok(ser)
}

/// Zero copy deserialization using flexbuffer. if Ok, `D` of implementation `DeserializeOwned`
/// is returned.
#[inline]
pub fn flexbuffer_deserialize<D>(data: &[u8]) -> Result<D, Error>
where
    D: DeserializeOwned,
{
    let reader = Reader::get_root(data).unwrap(); // TODO: add erro to Other

    D::deserialize(reader)
        .map_err(|err| Error::Deserialization(DeserializationError::Flexbuffer(err)))
}

pub use defer;
