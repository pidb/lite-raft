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