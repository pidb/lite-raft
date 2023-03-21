use std::fmt::Debug;

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;

pub trait WriteData: Clone + Send + Sync + Serialize + DeserializeOwned + 'static {
    // type EncodeError: Error + Send + Sync;
    // fn encode(&self) -> Result<Vec<u8>, Self::EncodeError>;
}

impl<T> WriteData for T where
    T: Clone + Send + Sync + Serialize + for<'a> Deserialize<'a> + 'static
{
}

/// Only use for tests
#[derive(thiserror::Error, Debug)]
#[allow(unused)]
pub(crate) enum EmptyWriteDataError {
    #[error("encode")]
    Encode,
}

/// Only use for tests
// #[derive(Clone, serde::Serialize, serde::Deserialize)]
// pub(crate) struct EmptyWriteData;

// impl WriteData for EmptyWriteData {
// }

pub trait WriteResponse: Debug + Clone + Send + Sync + 'static {}

impl<R> WriteResponse for R where R: Debug + Clone + Send + Sync + 'static {}
