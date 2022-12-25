//! A byte buffer for reading

use std::sync::Arc;

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

impl std::io::Read for Bytes {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.buffer.len() - self.offset < buf.len() {
            buf.clone_from_slice(&self.buffer[self.offset..]);
            let copied = self.buffer.len() - self.offset;
            self.offset = self.buffer.len();
            Ok(copied)
        } else {
            buf.clone_from_slice(&self.buffer[self.offset..self.offset + buf.len()]);
            self.offset += buf.len();
            Ok(buf.len())
        }
    }
    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        if self.buffer.len() - self.offset < buf.len() {
            Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "failed to read_exact",
            ))
        } else {
            buf.clone_from_slice(&self.buffer[self.offset..self.offset + buf.len()]);
            self.offset += buf.len();
            Ok(())
        }
    }
}

impl crate::column::encoding::Read for Bytes {
    fn seek(&mut self, offset: u64) -> Result<(), crate::column::encoding::StorageError> {
        if offset < self.buffer.len() as u64 {
            self.offset = offset as usize;
            Ok(())
        } else {
            Err(
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "failed to read_exact")
                    .into(),
            )
        }
    }
    fn tell(&mut self) -> Result<u64, crate::column::encoding::StorageError> {
        Ok(self.offset as u64)
    }
}
