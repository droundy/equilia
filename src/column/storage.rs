//! A backend storage.
//!
//! This module will eventually be private.

pub mod bytes;
use bytes::Bytes;

#[derive(Debug, Clone)]
pub(crate) enum Storage {
    Bytes(Bytes),
}

impl From<Vec<u8>> for Storage {
    fn from(value: Vec<u8>) -> Self {
        Storage::Bytes(value.into())
    }
}

impl From<&[u8]> for Storage {
    fn from(value: &[u8]) -> Self {
        Self::from(value.to_vec())
    }
}

impl super::encoding::ReadEncoded for Storage {
    fn seek(&mut self, offset: u64) -> Result<(), super::encoding::StorageError> {
        match self {
            Storage::Bytes(b) => b.seek(offset),
        }
    }

    fn tell(&self) -> Result<u64, super::encoding::StorageError> {
        match self {
            Storage::Bytes(b) => b.tell(),
        }
    }

    fn read_exact_at(
        &self,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<(), super::encoding::StorageError> {
        match self {
            Storage::Bytes(b) => b.read_exact_at(buf, offset),
        }
    }
}
