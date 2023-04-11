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
