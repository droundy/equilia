//! Our column formats.
//!
//! This module will eventually be private.

use encoding::{ReadEncoded, StorageError};
use storage::Storage;

use self::encoding::WriteEncoded;

mod boolcolumn;
pub mod encoding;
pub mod storage;
mod u64_16;
mod u64_32;
mod u64column;

pub(crate) use boolcolumn::BoolColumn;
pub(crate) use u64_16::U64_16Column;
pub(crate) use u64_32::U64_32Column;
pub(crate) use u64column::U64Column;

/// A raw column
pub struct RawColumn {
    inner: RawColumnInner,
}

fn run_length_encode<T: PartialEq + Clone>(elems: &[T]) -> Vec<(T, u64)> {
    let mut out = Vec::new();
    if let Some(mut previous) = elems.first() {
        let mut count = 0;
        for v in elems.iter() {
            if v == previous {
                count += 1;
            } else {
                out.push((previous.clone(), count));
                count = 1;
                previous = v;
            }
        }
        if count > 0 {
            out.push((previous.clone(), count));
        }
    }
    out
}

impl From<&[bool]> for RawColumn {
    fn from(bools: &[bool]) -> Self {
        RawColumn {
            inner: RawColumnInner::Bool(BoolColumn::from(bools)),
        }
    }
}

impl From<&[u64]> for RawColumn {
    fn from(vals: &[u64]) -> Self {
        let max = vals.iter().copied().max().unwrap_or_default();
        let min = vals.iter().copied().min().unwrap_or_default();
        let inner = if max - min > u32::MAX as u64 {
            RawColumnInner::U64(U64Column::from(vals))
        } else if max - min > u16::MAX as u64 {
            RawColumnInner::U64(U64Column::from(vals))
        } else {
            RawColumnInner::U64_16(U64_16Column::from(vals))
        };
        RawColumn { inner }
    }
}

const BOOL_MAGIC: u64 = u64::from_be_bytes(*b"__bool__");
const U64_MAGIC: u64 = u64::from_be_bytes(*b"__u64___");
const U64_32_MAGIC: u64 = u64::from_be_bytes(*b"__u64_32");
const U64_16_MAGIC: u64 = u64::from_be_bytes(*b"__u64_16");

impl RawColumn {
    /// This isn't what we'll really want to use, but might be useful for
    /// testing?
    ///
    /// It also illustrates how some common logic can be abstracted away into a
    /// helper function like the `column_to_vec` below.
    pub fn read_bools(&self) -> Result<Vec<bool>, StorageError> {
        match &self.inner {
            RawColumnInner::Bool(b) => column_to_vec(b),
            RawColumnInner::U64(_) => panic!("does not hold bools"),
            RawColumnInner::U64_16(_) => panic!("does not hold bools"),
            RawColumnInner::U64_32(_) => panic!("does not hold bools"),
        }
    }
    /// This isn't what we'll really want to use, but might be useful for
    /// testing?
    ///
    /// It also illustrates how some common logic can be abstracted away into a
    /// helper function like the `column_to_vec` below.
    pub fn read_u64(&self) -> Result<Vec<u64>, StorageError> {
        match &self.inner {
            RawColumnInner::U64(b) => column_to_vec(b),
            RawColumnInner::U64_32(b) => column_to_vec(b),
            RawColumnInner::U64_16(b) => column_to_vec(b),
            RawColumnInner::Bool(_) => panic!("does not hold u64"),
        }
    }

    /// Decode these bytes as a `RawColumn`
    pub fn decode(buf: Vec<u8>) -> Result<Self, StorageError> {
        Self::open_storage(Storage::from(buf))
    }

    /// Open a column file
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self, StorageError> {
        Self::open_storage(Storage::open(path)?)
    }

    pub(crate) fn open_storage(mut storage: Storage) -> Result<Self, StorageError> {
        let magic = storage.read_u64()?;
        storage.seek(0)?;
        let inner = match magic {
            BOOL_MAGIC => RawColumnInner::Bool(BoolColumn::open(storage)?),
            U64_32_MAGIC => RawColumnInner::U64_32(U64_32Column::open(storage)?),
            U64_16_MAGIC => RawColumnInner::U64_16(U64_16Column::open(storage)?),
            U64_MAGIC => RawColumnInner::U64(U64Column::open(storage)?),
            _ => return Err(StorageError::BadMagic(magic)),
        };
        Ok(RawColumn { inner })
    }
}

impl TryFrom<std::fs::File> for RawColumn {
    type Error = StorageError;
    fn try_from(value: std::fs::File) -> Result<Self, Self::Error> {
        let storage = Storage::try_from(value)?;
        Self::open_storage(storage)
    }
}

fn column_to_vec<C: IsRawColumn>(column: &C) -> Result<Vec<C::Element>, StorageError> {
    let mut out = Vec::new();
    for chunk in column.clone() {
        let chunk = chunk?;
        for _ in chunk.range {
            out.push(chunk.value.clone());
        }
    }
    Ok(out)
}

pub(crate) enum RawColumnInner {
    Bool(BoolColumn),
    U64(U64Column),
    U64_32(U64_32Column),
    U64_16(U64_16Column),
}

/// A chunk of identical values.
#[derive(Debug, PartialEq, Eq)]
pub struct Chunk<T> {
    value: T,
    range: std::ops::Range<u64>,
}

/// A specific format for a [`RawColumn`].
///
/// Note that this type doubles as a kind of iterator, but a weird one where the
/// values are borrowed from the iterator not the data itself.
pub(crate) trait IsRawColumn:
    Sized + Clone + Iterator<Item = Result<Chunk<Self::Element>, StorageError>> + TryFrom<Storage>
{
    type Element: Clone;
    /// Create a column from a set of values and run lengths
    ///
    /// Eventually we'll want to be able to write to a file instead
    /// of an in-memory buffer.
    ///
    /// Implementations may assume that two sequential T will not be equal.
    fn encode<W: WriteEncoded>(
        out: &mut W,
        input: &[(Self::Element, u64)],
    ) -> Result<(), StorageError>;

    /// Read the header of an encoded column
    ///
    /// Note that this does not either read or validate the entire [`Storage`].
    fn open(storage: Storage) -> Result<Self, StorageError>;

    /// Get the current file offset
    fn tell(&self) -> Result<u64, StorageError>;

    /// Seek to the file offset with the specified value and row number
    fn seek(
        &mut self,
        offset: u64,
        row_number: u64,
        value: impl AsRef<Self::Element>,
    ) -> Result<(), StorageError>;

    /// Returns the (cached) number of rows
    fn num_rows(&self) -> u64;
    /// Returns the (cached) number of chunks
    fn num_chunks(&self) -> u64;
    /// Returns the (cached) maximum value
    fn max(&self) -> Self::Element;
    /// Returns the (cached) minimum value
    fn min(&self) -> Self::Element;
}
