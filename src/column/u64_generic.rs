//! Will be private
use super::{
    encoding::BitWidth, Chunk, IsRawColumn, ReadEncoded, Storage, StorageError, WriteEncoded,
    U64_GENERIC_MAGIC,
};

#[derive(Clone)]
pub(crate) struct U64<const F: u64> {
    storage: Storage,
    current_row: u64,
    n_rows: u64,
    n_chunks: u64,
    v_max: u64,
    v_min: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Format {
    value: BitWidth,
    runlength: BitWidth,
}

impl TryFrom<u64> for Format {
    type Error = StorageError;
    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::from_bytes(value)
    }
}

impl Format {
    const fn from_bytes(value: u64) -> Result<Self, StorageError> {
        let bytes = value.to_be_bytes();
        let Some(value) = BitWidth::new(bytes[0]) else {
            return Err(StorageError::OutOfBounds);
        };
        let Some(runlength) = BitWidth::new(bytes[1]) else {
            return Err(StorageError::OutOfBounds);
        };
        Ok(Format { value, runlength })
    }
}

impl<const F: u64> From<&[u64]> for U64<F> {
    /// Create a column
    fn from(vals: &[u64]) -> Self {
        let mut bytes = Vec::<u8>::new();
        Self::encode(&mut bytes, &super::run_length_encode(vals)).unwrap();
        let storage = Storage::from(bytes);
        Self::open(storage).unwrap()
    }
}
impl<const F: u64> Iterator for U64<F> {
    type Item = Result<Chunk<u64>, StorageError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.transposed_next().transpose()
    }
}

impl<const F: u64> U64<F> {
    fn transposed_next(&mut self) -> Result<Option<Chunk<u64>>, StorageError> {
        if self.current_row == self.n_rows {
            return Ok(None);
        }
        let format = Format::from_bytes(F)?;
        let num = self.storage.read_bitwidth(format.runlength)?;
        let value = self.v_min + self.storage.read_bitwidth(format.value)? as u64;
        let current_row = self.current_row;
        self.current_row = current_row + num;

        Ok(Some(Chunk {
            value,
            range: current_row..self.current_row,
        }))
    }
}
impl<const F: u64> IsRawColumn for U64<F> {
    type Element = u64;

    fn num_rows(&self) -> u64 {
        self.n_rows
    }
    fn num_chunks(&self) -> u64 {
        self.n_chunks
    }
    fn max(&self) -> Self::Element {
        self.v_max
    }
    fn min(&self) -> Self::Element {
        self.v_min
    }

    fn encode<W: WriteEncoded>(
        out: &mut W,
        input: &[(Self::Element, u64)],
    ) -> Result<(), StorageError> {
        if input.is_empty() {
            return Ok(());
        }
        out.write_u64(U64_GENERIC_MAGIC)?;
        out.write_u64(F)?;
        out.write_u64(input.iter().map(|x| x.1).sum())?;
        out.write_u64(input.len() as u64)?;
        let min = input.iter().map(|(v, _)| *v).min().unwrap_or(0);
        let max = input.iter().map(|(v, _)| *v).max().unwrap_or(0);
        if max - min > u32::MAX as u64 {
            return Err(StorageError::OutOfBounds);
        }
        out.write_u64(min)?;
        out.write_u64(max)?;
        let format = Format::from_bytes(F)?;
        for &(v, num) in input.iter() {
            out.write_bitwidth(format.runlength, num)?;
            out.write_bitwidth(format.value, v - min)?;
        }
        Ok(())
    }

    fn open(mut storage: Storage) -> Result<Self, StorageError> {
        println!("offset starts at {}", storage.tell().unwrap());
        let magic = storage.read_u64()?;
        println!("after magic {}", storage.tell().unwrap());
        if magic != U64_GENERIC_MAGIC {
            return Err(StorageError::BadMagic(magic));
        }
        if F != storage.read_u64()? {
            // FIXME need another error type here
            return Err(StorageError::BadMagic(magic));
        }
        let n_rows = storage.read_u64()?;
        let n_chunks = storage.read_u64()?;
        let v_min = storage.read_u64()?;
        let v_max = storage.read_u64()?;
        Ok(U64 {
            storage,
            n_chunks,
            current_row: 0,
            n_rows,
            v_max,
            v_min,
        })
    }

    fn tell(&self) -> Result<u64, StorageError> {
        self.storage.tell()
    }

    fn seek(
        &mut self,
        offset: u64,
        row_number: u64,
        _value: impl AsRef<Self::Element>,
    ) -> Result<(), StorageError> {
        self.current_row = row_number;
        self.storage.seek(offset)
    }
}

impl<const F: u64> TryFrom<Storage> for U64<F> {
    type Error = StorageError;
    fn try_from(storage: Storage) -> Result<Self, Self::Error> {
        Self::open(storage)
    }
}

// #[test]
// fn encode_u64() {
//     use super::RawColumn;

//     let bools = [1, 1, 1, 1, 2, 2, 16, 1, u32::MAX as u64 + 1];
//     let bc = U64::from(&bools[..]);
//     let c = RawColumn::from(&bools[..]);
//     assert_eq!(c.read_u64().unwrap().as_slice(), &bools);

//     let mut encoded: Vec<u8> = Vec::new();
//     let chunks: Vec<(u64, u64)> = bc
//         .clone()
//         .map(|chunk| {
//             let chunk = chunk.unwrap();
//             (chunk.value, chunk.range.end - chunk.range.start)
//         })
//         .collect();
//     <U64 as IsRawColumn>::encode(&mut encoded, chunks.as_slice()).unwrap();

//     let storage = Storage::from(encoded.clone());
//     let bc2 = U64::open(storage.clone()).unwrap();
//     assert_eq!(
//         bc2.map(|x| x.unwrap()).collect::<Vec<_>>(),
//         bc.map(|x| x.unwrap()).collect::<Vec<_>>()
//     );
//     let c2 = RawColumn::decode(encoded).unwrap();
//     assert_eq!(c2.read_u64().unwrap().as_slice(), &bools);

//     let mut f = tempfile::tempfile().unwrap();
//     <U64 as IsRawColumn>::encode(&mut f, chunks.as_slice()).unwrap();
//     let c = RawColumn::try_from(f).unwrap();
//     assert_eq!(c.read_u64().unwrap().as_slice(), &bools);
// }
