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

impl std::io::Read for Storage {
    #[inline(always)]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Storage::Bytes(b) => b.read(buf),
        }
    }
    #[inline(always)]
    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        match self {
            Storage::Bytes(b) => b.read_exact(buf),
        }
    }
}
