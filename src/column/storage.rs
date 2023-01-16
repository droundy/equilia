//! A backend storage.
//!
//! This module will eventually be private.

mod bytes;
mod file;
use bytes::Bytes;
use file::File;

use super::encoding::StorageError;

#[derive(Debug, Clone)]
pub(crate) enum Storage {
    Bytes(Bytes),
    File(File),
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

impl Storage {
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self, StorageError> {
        Ok(Self::File(File::open(path)?))
    }
}

impl TryFrom<std::fs::File> for Storage {
    type Error = StorageError;
    fn try_from(value: std::fs::File) -> Result<Self, Self::Error> {
        Ok(Self::File(File::try_from(value)?))
    }
}

impl super::encoding::ReadEncoded for Storage {
    fn seek(&mut self, offset: u64) -> Result<(), super::encoding::StorageError> {
        match self {
            Storage::Bytes(b) => b.seek(offset),
            Storage::File(f) => f.seek(offset),
        }
    }

    fn tell(&self) -> Result<u64, super::encoding::StorageError> {
        match self {
            Storage::Bytes(b) => b.tell(),
            Storage::File(f) => f.tell(),
        }
    }

    fn read_exact_at(
        &self,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<(), super::encoding::StorageError> {
        match self {
            Storage::Bytes(b) => b.read_exact_at(buf, offset),
            Storage::File(f) => f.read_exact_at(buf, offset),
        }
    }
}
