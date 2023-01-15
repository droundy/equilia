//! Encoding for columns
//!
//! This module will eventually be private.

use thiserror::Error;

const U16_CODE: u8 = 253;
const U32_CODE: u8 = 254;
const U64_CODE: u8 = 255;

/// Size to store a u64 as
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BitWidth {
    /// Zero bits, value must be 1
    IsOne = 0,
    /// 1 byte
    U8 = 1,
    /// 2 bytes
    U16 = 2,
    /// 4 bytes
    U32 = 4,
    /// 8 bytes
    U64 = 8,
    /// Variable number of bytes
    Variable = 255,
}
impl BitWidth {
    /// Deserialize a BitWidth
    pub const fn new(v: u8) -> Option<BitWidth> {
        use BitWidth::*;
        match () {
            _ if v == IsOne as u8 => Some(IsOne),
            _ if v == U8 as u8 => Some(U8),
            _ if v == U16 as u8 => Some(U16),
            _ if v == U32 as u8 => Some(U32),
            _ if v == U64 as u8 => Some(U64),
            _ if v == Variable as u8 => Some(Variable),
            _ => None,
        }
    }

    /// The maximum representable value
    pub const fn max(self) -> u64 {
        match self {
            BitWidth::IsOne => 1,
            BitWidth::U8 => u8::MAX as u64,
            BitWidth::U16 => u16::MAX as u64,
            BitWidth::U32 => u32::MAX as u64,
            BitWidth::U64 => u64::MAX,
            BitWidth::Variable => u64::MAX,
        }
    }
}

/// An error of any sort
#[derive(Debug, Error)]
pub enum StorageError {
    /// An IO error
    #[error("Io error: {0}")]
    Io(#[from] std::io::Error),
    /// Bad magic
    #[error("Bad magic: {}", pretty_magic(.0))]
    BadMagic(u64),
    /// Out of bounds
    #[error("Out of bounds")]
    OutOfBounds,
}

fn pretty_magic(m: &u64) -> String {
    if let Ok(s) = std::str::from_utf8(&m.to_be_bytes()) {
        s.to_owned()
    } else {
        format!("{:x}", m)
    }
}

/// A thing that could be the backing store for a column
pub trait ReadEncoded {
    /// Move to this offset from beginning
    fn seek(&mut self, offset: u64) -> Result<(), StorageError>;
    /// Find the offset from beginning
    fn tell(&self) -> Result<u64, StorageError>;

    /// Read bytes at a given offset
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), StorageError>;

    /// Read bytes at a given offset
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), StorageError> {
        let offset = self.advance(buf.len() as u64)?;
        self.read_exact_at(buf, offset)
    }

    /// Increment the current offset
    fn advance(&mut self, size: u64) -> Result<u64, StorageError> {
        let offset = self.tell()?;
        self.seek(offset + size)?;
        Ok(offset)
    }

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
    /// Reads a single 8-byte `u64` value.
    #[inline(always)]
    fn read_bitwidth(&mut self, bitwidth: BitWidth) -> Result<u64, StorageError> {
        match bitwidth {
            BitWidth::IsOne => Ok(1),
            BitWidth::U8 => self.read_u8().map(|v| v as u64),
            BitWidth::U16 => self.read_u16().map(|v| v as u64),
            BitWidth::U32 => self.read_u32().map(|v| v as u64),
            BitWidth::U64 => self.read_u64(),
            BitWidth::Variable => self.read_usigned(),
        }
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

/// An extension trait for our encoding
pub trait WriteEncoded: std::io::Write {
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
    /// Write the value with the specified number of bytes
    ///
    /// This returns an error if the value does not fit in the specified range.
    fn write_bitwidth(&mut self, bitwidth: BitWidth, v: u64) -> Result<(), StorageError> {
        match bitwidth {
            BitWidth::IsOne => {
                if v != 1 {
                    Err(StorageError::OutOfBounds)
                } else {
                    Ok(())
                }
            }
            BitWidth::U8 => {
                if let Ok(v) = v.try_into() {
                    self.write_u8(v)
                } else {
                    Err(StorageError::OutOfBounds)
                }
            }
            BitWidth::U16 => {
                if let Ok(v) = v.try_into() {
                    self.write_u16(v)
                } else {
                    Err(StorageError::OutOfBounds)
                }
            }
            BitWidth::U32 => {
                if let Ok(v) = v.try_into() {
                    self.write_u32(v)
                } else {
                    Err(StorageError::OutOfBounds)
                }
            }
            BitWidth::U64 => self.write_u64(v),
            BitWidth::Variable => self.write_unsigned(v),
        }
    }
}

impl<T: std::io::Write> WriteEncoded for T {}
