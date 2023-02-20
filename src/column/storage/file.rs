//! A byte buffer for reading

use std::sync::Arc;

use crate::column::encoding::StorageError;

/// A read-only file that supports concurrent reads. (unix-only)
#[derive(Debug, Clone)]
pub struct File {
    file: Arc<std::fs::File>,
    offset: u64,
    length: u64,
}

impl File {
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self, StorageError> {
        Self::try_from(std::fs::File::open(path)?)
    }
}

impl TryFrom<std::fs::File> for File {
    type Error = StorageError;
    fn try_from(value: std::fs::File) -> Result<Self, Self::Error> {
        let file = Arc::new(value);
        let length = file.metadata()?.len();
        Ok(File {
            file,
            length,
            offset: 0,
        })
    }
}

impl crate::column::encoding::ReadEncoded for File {
    fn seek(&mut self, offset: u64) -> Result<(), crate::column::encoding::StorageError> {
        if offset <= self.length {
            self.offset = offset;
            Ok(())
        } else {
            Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "failed to seek").into())
        }
    }
    fn tell(&self) -> Result<u64, crate::column::encoding::StorageError> {
        Ok(self.offset)
    }
    fn read_exact_at(
        &self,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<(), crate::column::encoding::StorageError> {
        if offset + buf.len() as u64 > self.length {
            Err(StorageError::Io(
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "failed to read_exact"),
                Vec::new(),
            ))
        } else {
            use std::os::unix::fs::FileExt;
            self.file.read_exact_at(buf, offset)?;
            Ok(())
        }
    }
}
