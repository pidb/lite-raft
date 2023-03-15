use std::error::Error;

pub trait Encode {
    type EncodeError: Error + Send + Sync;
    fn encode(&mut self) -> Result<Vec<u8>, Self::EncodeError>;
}

pub trait Decode {
    type DecodeError: Error + Send + Sync;
    fn decode(&mut self, bytes: &mut [u8]) -> Result<(), Self::DecodeError>;
}

pub trait WriteData: Clone + Send + Sync + Encode + Decode + 'static {}

/// Only use for tests
#[derive(thiserror::Error, Debug)]
pub(crate) enum EmptyWriteDataError {
    #[error("encode")]
    Encode,
    #[error("decode")]
    Decode,
}

/// Only use for tests
#[derive(Clone)]
pub(crate) struct EmptyWriteData;

impl Encode for EmptyWriteData {
    type EncodeError = EmptyWriteDataError;
    fn encode(&mut self) -> Result<Vec<u8>, Self::EncodeError> {
        Ok(vec![])
    }
}

impl Decode for EmptyWriteData {
    type DecodeError = EmptyWriteDataError;
    fn decode(&mut self, _: &mut [u8]) -> Result<(), Self::DecodeError> {
        Ok(())
    }
}

impl WriteData for EmptyWriteData {}
