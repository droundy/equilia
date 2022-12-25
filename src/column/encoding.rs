//! Encoding for columns
//!
//! This module will eventually be private.

use thiserror::Error;

const U16_CODE: u8 = 253;
const U32_CODE: u8 = 254;
const U64_CODE: u8 = 255;

/// Write a `u64` in as few bytes as possible.
pub fn write_u64(buf: &mut Vec<u8>, v: u64) {
    if v < U16_CODE as u64 {
        buf.push(v as u8);
    } else if v < u16::MAX as u64 {
        buf.push(U16_CODE);
        buf.extend((v as u16).to_be_bytes());
    } else if v < u32::MAX as u64 {
        buf.push(U32_CODE);
        buf.extend((v as u32).to_be_bytes());
    } else {
        buf.push(U64_CODE);
        buf.extend(v.to_be_bytes());
    }
}

/// Read a `u64` that was written with [`write_u64`].
pub fn read_u64(mut buf: &[u8]) -> Result<(u64, &[u8]), ()> {
    let b = buf.first().copied().ok_or(())?;
    buf = &buf[1..];
    match b {
        U16_CODE => {
            let (array, rest) = try_split_array::<2>(buf).ok_or(())?;
            Ok((u16::from_be_bytes(array) as u64, rest))
        }
        U32_CODE => {
            let (array, rest) = try_split_array::<4>(buf).ok_or(())?;
            Ok((u32::from_be_bytes(array) as u64, rest))
        }
        U64_CODE => {
            let (array, rest) = try_split_array::<8>(buf).ok_or(())?;
            Ok((u64::from_be_bytes(array), rest))
        }
        _ => Ok((b as u64, buf)),
    }
}

fn try_split_array<const N: usize>(buf: &[u8]) -> Option<([u8; N], &[u8])> {
    if buf.len() < N {
        None
    } else {
        Some((buf[..N].try_into().unwrap(), &buf[N..]))
    }
}

/// An error of any sort
#[derive(Debug, Error)]
pub enum StorageError {
    /// An IO error
    #[error("Io error: {0}")]
    Io(#[from] std::io::Error),
}

/// A thing that could be the backing store for a column
pub trait ReadExt: std::io::Read {
    /// Move to this offset from beginning
    fn seek(&mut self, offset: u64) -> Result<(), StorageError>;
    /// Find the offset from beginning
    fn tell(&mut self) -> Result<u64, StorageError>;

    /// Reads a single `u8` value.
    fn read_u8(&mut self) -> Result<u8, StorageError> {
        let mut v = [0];
        self.read_exact(&mut v)?;
        Ok(v[0])
    }
    /// Reads a single 2-byte `u16` value.
    fn read_u16(&mut self) -> Result<u16, StorageError> {
        let mut v = [0; 2];
        self.read_exact(&mut v)?;
        Ok(u16::from_be_bytes(v))
    }
    /// Reads a single 4-byte `u32` value.
    fn read_u32(&mut self) -> Result<u32, StorageError> {
        let mut v = [0; 4];
        self.read_exact(&mut v)?;
        Ok(u32::from_be_bytes(v))
    }
    /// Reads a single 8-byte `u64` value.
    fn read_u64(&mut self) -> Result<u64, StorageError> {
        let mut v = [0; 8];
        self.read_exact(&mut v)?;
        Ok(u64::from_be_bytes(v))
    }
    /// Reads an encoded unsigned value, which might take up to 9 bytes.
    fn read_usigned(&mut self) -> Result<u64, StorageError> {
        let b = self.read_u8()?;
        match b {
            U16_CODE => self.read_u16().map(|v| v as u64),
            U32_CODE => self.read_u32().map(|v| v as u64),
            U64_CODE => self.read_u64(),
            _ => Ok(b as u64),
        }
    }
}
impl<T: std::io::Read + std::io::Seek> ReadExt for T {
    fn seek(&mut self, offset: u64) -> Result<(), StorageError> {
        std::io::Seek::seek(self, std::io::SeekFrom::Start(offset))?;
        Ok(())
    }
    fn tell(&mut self) -> Result<u64, StorageError> {
        std::io::Seek::seek(self, std::io::SeekFrom::Current(0)).map_err(StorageError::from)
    }
}

/// An extension trait for our encoding
pub trait WriteExt: std::io::Write {
    /// Writes a byte
    fn write_u8(&mut self, v: u8) -> Result<(), StorageError> {
        self.write_all(&[v]).map_err(StorageError::from)
    }
    /// Writes a 2-byte u16
    fn write_u16(&mut self, v: u16) -> Result<(), StorageError> {
        self.write_all(&v.to_be_bytes()).map_err(StorageError::from)
    }
    /// Writes a 4-byte u32
    fn write_u32(&mut self, v: u32) -> Result<(), StorageError> {
        self.write_all(&v.to_be_bytes()).map_err(StorageError::from)
    }
    /// Writes a 8-byte u64
    fn write_u64(&mut self, v: u64) -> Result<(), StorageError> {
        self.write_all(&v.to_be_bytes()).map_err(StorageError::from)
    }
    /// Writes an encoded unsigned value, which might take up to 9 bytes.
    fn write_unsigned(&mut self, v: u64) -> Result<(), StorageError> {
        if v < U16_CODE as u64 {
            self.write_u8(v as u8)
        } else if v < u16::MAX as u64 {
            self.write_u8(U16_CODE)?;
            self.write_u16(v as u16)
        } else if v < u32::MAX as u64 {
            self.write_u8(U32_CODE)?;
            self.write_u32(v as u32)
        } else {
            self.write_u8(U64_CODE)?;
            self.write_u64(v)
        }
    }
}

impl<T: std::io::Write> WriteExt for T {}
