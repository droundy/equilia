//! Our column formats.
//!
//! This module will eventually be private.

use encoding::{ReadEncoded, StorageError};
use storage::Storage;

use self::encoding::WriteEncoded;

mod boolcolumn;
pub mod bytes;
pub mod encoding;
pub mod storage;
pub mod u64_generic;

pub(crate) use boolcolumn::BoolColumn;

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
        let longest_run = run_length_encode(vals)
            .into_iter()
            .map(|x| x.1)
            .max()
            .unwrap_or_default();
        let inner = if max - min > u32::MAX as u64 {
            if longest_run < 2 {
                RawColumnInner::U64V1(u64_generic::VariableOne::from(vals))
            } else {
                RawColumnInner::U64VV(u64_generic::VariableVariable::from(vals))
            }
        } else if max - min > u16::MAX as u64 {
            if longest_run < 2 {
                RawColumnInner::U64_32_1(u64_generic::U32One::from(vals))
            } else {
                RawColumnInner::U64_32(u64_generic::U32Variable::from(vals))
            }
        } else if max - min > u8::MAX as u64 {
            if longest_run < 2 {
                RawColumnInner::U64_16_1(u64_generic::U16One::from(vals))
            } else {
                RawColumnInner::U64_16(u64_generic::U16Variable::from(vals))
            }
        } else {
            if longest_run < 2 {
                RawColumnInner::U64_8_1(u64_generic::U8One::from(vals))
            } else {
                RawColumnInner::U64_8(u64_generic::U8Variable::from(vals))
            }
        };
        RawColumn { inner }
    }
}

impl From<&[Vec<u8>]> for RawColumn {
    fn from(vals: &[Vec<u8>]) -> Self {
        // let longest_run = run_length_encode(vals)
        //     .into_iter()
        //     .map(|x| x.1)
        //     .max()
        //     .unwrap_or_default();
        let inner = RawColumnInner::BytesVVV(bytes::VVV::from(vals));
        RawColumn { inner }
    }
}

const BOOL_MAGIC: u64 = u64::from_be_bytes(*b"__bool__");
const U64_GENERIC_MAGIC: u64 = u64::from_be_bytes(*b"00u64gen");
const BYTES_GENERIC_MAGIC: u64 = u64::from_be_bytes(*b"000bytes");

impl RawColumn {
    /// This isn't what we'll really want to use, but might be useful for
    /// testing?
    ///
    /// It also illustrates how some common logic can be abstracted away into a
    /// helper function like the `column_to_vec` below.
    pub fn read_bools(&self) -> Result<Vec<bool>, StorageError> {
        match &self.inner {
            RawColumnInner::Bool(b) => column_to_vec(b),
            RawColumnInner::BytesVVV(_) => panic!("does not hold bools"),
            RawColumnInner::U64VV(_) => panic!("does not hold bools"),
            RawColumnInner::U64_8(_) => panic!("does not hold bools"),
            RawColumnInner::U64_8_1(_) => panic!("does not hold bools"),
            RawColumnInner::U64_16(_) => panic!("does not hold bools"),
            RawColumnInner::U64_16_1(_) => panic!("does not hold bools"),
            RawColumnInner::U64_32(_) => panic!("does not hold bools"),
            RawColumnInner::U64_32_1(_) => panic!("does not hold bools"),
            RawColumnInner::U64V1(_) => panic!("does not hold bools"),
        }
    }
    /// This isn't what we'll really want to use, but might be useful for
    /// testing?
    ///
    /// It also illustrates how some common logic can be abstracted away into a
    /// helper function like the `column_to_vec` below.
    pub fn read_u64(&self) -> Result<Vec<u64>, StorageError> {
        match &self.inner {
            RawColumnInner::U64VV(b) => column_to_vec(b),
            RawColumnInner::U64_32(b) => column_to_vec(b),
            RawColumnInner::U64_32_1(b) => column_to_vec(b),
            RawColumnInner::U64_16(b) => column_to_vec(b),
            RawColumnInner::U64_16_1(b) => column_to_vec(b),
            RawColumnInner::U64_8(b) => column_to_vec(b),
            RawColumnInner::U64_8_1(b) => column_to_vec(b),
            RawColumnInner::U64V1(b) => column_to_vec(b),
            RawColumnInner::Bool(_) => panic!("does not hold u64"),
            RawColumnInner::BytesVVV(_) => panic!("does not hold u64"),
        }
    }
    /// This isn't what we'll really want to use, but might be useful for
    /// testing?
    ///
    /// It also illustrates how some common logic can be abstracted away into a
    /// helper function like the `column_to_vec` below.
    pub fn read_bytes(&self) -> Result<Vec<Vec<u8>>, StorageError> {
        match &self.inner {
            RawColumnInner::U64VV(_) => panic!("does not hold bytes"),
            RawColumnInner::U64_32(_) => panic!("does not hold bytes"),
            RawColumnInner::U64_32_1(_) => panic!("does not hold bytes"),
            RawColumnInner::U64_16(_) => panic!("does not hold bytes"),
            RawColumnInner::U64_16_1(_) => panic!("does not hold bytes"),
            RawColumnInner::U64_8(_) => panic!("does not hold bytes"),
            RawColumnInner::U64_8_1(_) => panic!("does not hold bytes"),
            RawColumnInner::U64V1(_) => panic!("does not hold bytes"),
            RawColumnInner::Bool(_) => panic!("does not hold bytes"),
            RawColumnInner::BytesVVV(c) => column_to_vec(c),
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

            bytes::VVV::MAGIC => RawColumnInner::BytesVVV(bytes::VVV::open(storage)?),

            u64_generic::U32Variable::MAGIC => {
                RawColumnInner::U64_32(u64_generic::U32Variable::open(storage)?)
            }
            u64_generic::U32One::MAGIC => {
                RawColumnInner::U64_32_1(u64_generic::U32One::open(storage)?)
            }
            u64_generic::U16Variable::MAGIC => {
                RawColumnInner::U64_16(u64_generic::U16Variable::open(storage)?)
            }
            u64_generic::U16One::MAGIC => {
                RawColumnInner::U64_16_1(u64_generic::U16One::open(storage)?)
            }
            u64_generic::U8Variable::MAGIC => {
                RawColumnInner::U64_8(u64_generic::U8Variable::open(storage)?)
            }
            u64_generic::U8One::MAGIC => {
                RawColumnInner::U64_8_1(u64_generic::U8One::open(storage)?)
            }
            u64_generic::VariableOne::MAGIC => {
                RawColumnInner::U64V1(u64_generic::VariableOne::open(storage)?)
            }
            u64_generic::VariableVariable::MAGIC => {
                RawColumnInner::U64VV(u64_generic::VariableVariable::open(storage)?)
            }
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

    BytesVVV(bytes::VVV),

    U64VV(u64_generic::VariableVariable),
    U64V1(u64_generic::VariableOne),
    U64_32(u64_generic::U32Variable),
    U64_32_1(u64_generic::U32One),
    U64_16(u64_generic::U16Variable),
    U64_16_1(u64_generic::U16One),
    U64_8(u64_generic::U8Variable),
    U64_8_1(u64_generic::U8One),
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
