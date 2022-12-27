use super::{Chunk, IsRawColumn, ReadEncoded, Storage, StorageError, WriteEncoded, U64_32_MAGIC};

#[derive(Clone)]
pub(crate) struct U64_32Column {
    storage: Storage,
    current_row: u64,
    n_rows: u64,
    n_chunks: u64,
    v_max: u64,
    v_min: u64,
}

impl From<&[u64]> for U64_32Column {
    fn from(vals: &[u64]) -> Self {
        let mut bytes = Vec::<u8>::new();
        U64_32Column::encode(&mut bytes, &super::run_length_encode(vals)).unwrap();
        println!("encoded is {bytes:?}");
        let storage = Storage::from(bytes);
        U64_32Column::open(storage).unwrap()
    }
}
impl Iterator for U64_32Column {
    type Item = Result<Chunk<u64>, StorageError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.transposed_next().transpose()
    }
}

impl U64_32Column {
    fn transposed_next(&mut self) -> Result<Option<Chunk<u64>>, StorageError> {
        if self.current_row == self.n_rows {
            return Ok(None);
        }
        let num = self.storage.read_usigned()?;
        let value = self.v_min + self.storage.read_u32()? as u64;
        let current_row = self.current_row;
        self.current_row = current_row + num;

        Ok(Some(Chunk {
            value,
            range: current_row..self.current_row,
        }))
    }
}
impl IsRawColumn for U64_32Column {
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
        out.write_u64(U64_32_MAGIC)?;
        out.write_u64(input.iter().map(|x| x.1).sum())?;
        out.write_u64(input.len() as u64)?;
        let min = input.iter().map(|(v, _)| *v).min().unwrap_or(0);
        let max = input.iter().map(|(v, _)| *v).max().unwrap_or(0);
        if max - min > u32::MAX as u64 {
            return Err(StorageError::OutOfBounds);
        }
        out.write_u64(min)?;
        out.write_u64(max)?;
        for &(v, num) in input.iter() {
            out.write_unsigned(num)?;
            out.write_u32((v - min) as u32)?;
        }
        Ok(())
    }

    fn open(mut storage: Storage) -> Result<Self, StorageError> {
        println!("offset starts at {}", storage.tell().unwrap());
        let magic = storage.read_u64()?;
        println!("after magic {}", storage.tell().unwrap());
        if magic != U64_32_MAGIC {
            return Err(StorageError::BadMagic(magic));
        }
        let n_rows = storage.read_u64()?;
        let n_chunks = storage.read_u64()?;
        let v_min = storage.read_u64()?;
        let v_max = storage.read_u64()?;
        Ok(U64_32Column {
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

impl TryFrom<Storage> for U64_32Column {
    type Error = StorageError;
    fn try_from(storage: Storage) -> Result<Self, Self::Error> {
        Self::open(storage)
    }
}

#[test]
fn encode_u64() {
    use super::{RawColumn, RawColumnInner};

    let bools = [1, 1, 1, 1, 2, 2, 16, 1];
    let bc = U64_32Column::from(&bools[..]);
    let c = RawColumn {
        inner: RawColumnInner::U64_32(bc.clone()),
    };
    assert_eq!(c.read_u64().unwrap().as_slice(), &bools);

    let mut encoded: Vec<u8> = Vec::new();
    let chunks: Vec<(u64, u64)> = bc
        .clone()
        .map(|chunk| {
            let chunk = chunk.unwrap();
            (chunk.value, chunk.range.end - chunk.range.start)
        })
        .collect();
    <U64_32Column as IsRawColumn>::encode(&mut encoded, chunks.as_slice()).unwrap();

    let storage = Storage::from(encoded.clone());
    let bc2 = U64_32Column::open(storage.clone()).unwrap();
    assert_eq!(
        bc2.map(|x| x.unwrap()).collect::<Vec<_>>(),
        bc.map(|x| x.unwrap()).collect::<Vec<_>>()
    );
    let c2 = RawColumn::decode(encoded).unwrap();
    assert_eq!(c2.read_u64().unwrap().as_slice(), &bools);

    let mut f = tempfile::tempfile().unwrap();
    <U64_32Column as IsRawColumn>::encode(&mut f, chunks.as_slice()).unwrap();
    let c = RawColumn::try_from(f).unwrap();
    assert_eq!(c.read_u64().unwrap().as_slice(), &bools);
}
