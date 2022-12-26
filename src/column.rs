//! Our column formats.
//!
//! This module will eventually be private.

use encoding::{ReadEncoded, StorageError};
use storage::Storage;

use self::encoding::WriteEncoded;

pub mod encoding;
pub mod storage;

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

impl From<&[bool]> for BoolColumn {
    fn from(bools: &[bool]) -> Self {
        let mut bytes = Vec::<u8>::new();
        BoolColumn::encode(&mut bytes, &run_length_encode(bools)).unwrap();
        println!("encoded is {bytes:?}");
        let storage = Storage::from(bytes);
        BoolColumn::open(storage).unwrap()
    }
}

impl From<&[bool]> for RawColumn {
    fn from(bools: &[bool]) -> Self {
        RawColumn {
            inner: RawColumnInner::Bool(BoolColumn::from(bools)),
        }
    }
}

impl RawColumn {
    /// This isn't what we'll really want to use, but might be useful for
    /// testing?
    ///
    /// It also illustrates how some common logic can be abstracted away into a
    /// helper function like the `column_to_vec` below.
    pub fn read_bools(&self) -> Result<Vec<bool>, StorageError> {
        match &self.inner {
            RawColumnInner::Bool(b) => column_to_vec(b),
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
}

#[derive(Clone)]
pub(crate) struct BoolColumn {
    storage: Storage,
    current_row: u64,
    num_rows: u64,
    last: bool,
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
}

/// An offset into a Column
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Offset(usize);

impl Iterator for BoolColumn {
    type Item = Result<Chunk<bool>, StorageError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.transposed_next().transpose()
    }
}

impl BoolColumn {
    fn transposed_next(&mut self) -> Result<Option<Chunk<bool>>, StorageError> {
        if self.current_row == self.num_rows {
            return Ok(None);
        }
        let num = self.storage.read_usigned()?;
        let current_row = self.current_row;
        self.current_row = current_row + num;
        self.last = !self.last;
        Ok(Some(Chunk {
            value: self.last,
            range: current_row..self.current_row,
        }))
    }
}

const BOOL_MAGIC: u64 = u64::from_be_bytes(*b"__bool__");
impl IsRawColumn for BoolColumn {
    type Element = bool;
    fn encode<W: WriteEncoded>(
        out: &mut W,
        input: &[(Self::Element, u64)],
    ) -> Result<(), StorageError> {
        if input.is_empty() {
            return Ok(());
        }
        out.write_u64(BOOL_MAGIC)?;
        out.write_unsigned(input.iter().map(|x| x.1).sum())?;
        out.write_u8(!input[0].0 as u8)?;
        for (_, num) in input.iter() {
            out.write_unsigned(*num)?;
        }
        Ok(())
    }

    fn open(mut storage: Storage) -> Result<Self, StorageError> {
        println!("offset starts at {}", storage.tell().unwrap());
        let magic = storage.read_u64()?;
        println!("after magic {}", storage.tell().unwrap());
        if magic != BOOL_MAGIC {
            return Err(StorageError::BadMagic(magic));
        }
        let num_rows = storage.read_usigned()?;
        let last = storage.read_u8()? == 1;
        Ok(BoolColumn {
            storage,
            current_row: 0,
            num_rows,
            last,
        })
    }

    fn tell(&self) -> Result<u64, StorageError> {
        self.storage.tell()
    }

    fn seek(
        &mut self,
        offset: u64,
        row_number: u64,
        value: impl AsRef<Self::Element>,
    ) -> Result<(), StorageError> {
        self.current_row = row_number;
        self.last = !*value.as_ref();
        self.storage.seek(offset)
    }
}

impl TryFrom<Storage> for BoolColumn {
    type Error = StorageError;
    fn try_from(mut storage: Storage) -> Result<Self, Self::Error> {
        let num_rows = storage.read_usigned()?;
        let last = storage.read_u8()? == 1;
        Ok(BoolColumn {
            storage,
            last,
            num_rows,
            current_row: 0,
        })
    }
}

#[test]
fn encode_bools() {
    let bools = [true, true, false, true, true, true];
    let bc = BoolColumn::from(&bools[..]);
    let c = RawColumn {
        inner: RawColumnInner::Bool(bc.clone()),
    };
    assert_eq!(c.read_bools().unwrap().as_slice(), &bools);

    let mut encoded: Vec<u8> = Vec::new();
    let chunks: Vec<(bool, u64)> = bc
        .clone()
        .map(|chunk| {
            let chunk = chunk.unwrap();
            (chunk.value, chunk.range.end - chunk.range.start)
        })
        .collect();
    <BoolColumn as IsRawColumn>::encode(&mut encoded, chunks.as_slice()).unwrap();

    let storage = Storage::from(encoded.clone());
    let bc2 = BoolColumn::open(storage.clone()).unwrap();
    assert_eq!(
        bc2.map(|x| x.unwrap()).collect::<Vec<_>>(),
        bc.map(|x| x.unwrap()).collect::<Vec<_>>()
    );
    let c2 = RawColumn::decode(encoded).unwrap();
    assert_eq!(c2.read_bools().unwrap().as_slice(), &bools);

    let mut f = tempfile::tempfile().unwrap();
    <BoolColumn as IsRawColumn>::encode(&mut f, chunks.as_slice()).unwrap();
    let c = RawColumn::try_from(f).unwrap();
    assert_eq!(c.read_bools().unwrap().as_slice(), &bools);
}
