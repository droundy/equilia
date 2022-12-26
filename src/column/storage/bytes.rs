//! A byte buffer for reading

use std::sync::Arc;

use crate::column::encoding::StorageError;

/// A read-only in-memory buffer
#[derive(Debug, Clone)]
pub struct Bytes {
    buffer: Arc<[u8]>,
    offset: usize,
}

impl From<Vec<u8>> for Bytes {
    fn from(value: Vec<u8>) -> Self {
        Bytes {
            buffer: Arc::from(value.into_boxed_slice()),
            offset: 0,
        }
    }
}

impl From<&[u8]> for Bytes {
    fn from(value: &[u8]) -> Self {
        Self::from(value.to_vec())
    }
}

impl crate::column::encoding::ReadEncoded for Bytes {
    fn seek(&mut self, offset: u64) -> Result<(), crate::column::encoding::StorageError> {
        if offset <= self.buffer.len() as u64 {
            self.offset = offset as usize;
            Ok(())
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "failed to seek").into())
        }
    }
    fn tell(&self) -> Result<u64, crate::column::encoding::StorageError> {
        Ok(self.offset as u64)
    }
    fn read_exact_at(
        &self,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<(), crate::column::encoding::StorageError> {
        if offset as usize + buf.len() > self.buffer.len() {
            Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "failed to read_exact",
            )))
        } else {
            buf.clone_from_slice(&self.buffer[offset as usize..offset as usize + buf.len()]);
            Ok(())
        }
    }
}
