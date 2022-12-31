use super::U64_DENSE_MAGIC as MAGIC;
use super::{Chunk, IsRawColumn, ReadEncoded, Storage, StorageError, WriteEncoded};

#[derive(Clone)]
pub(crate) struct Column {
    storage: Storage,
    current_row: u64,
    n_rows: u64,
    n_chunks: u64,
    v_max: u64,
    v_min: u64,
}

impl From<&[u64]> for Column {
    fn from(vals: &[u64]) -> Self {
        let mut bytes = Vec::<u8>::new();
        Column::encode(&mut bytes, &super::run_length_encode(vals)).unwrap();
        println!("encoded is {bytes:?}");
        let storage = Storage::from(bytes);
        Column::open(storage).unwrap()
    }
}
impl Iterator for Column {
    type Item = Result<Chunk<u64>, StorageError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.transposed_next().transpose()
    }
}

impl Column {
    fn transposed_next(&mut self) -> Result<Option<Chunk<u64>>, StorageError> {
        if self.current_row == self.n_rows {
            return Ok(None);
        }
        let value = self.storage.read_u8()? as u64 + self.v_min;
        let current_row = self.current_row;
        self.current_row = current_row + 1;

        Ok(Some(Chunk {
            value,
            range: current_row..self.current_row,
        }))
    }
}
impl IsRawColumn for Column {
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
        out.write_u64(MAGIC)?;
        out.write_u64(input.iter().map(|x| x.1).sum())?;
        out.write_u64(input.len() as u64)?;
        let min = input.iter().map(|(v, _)| *v).min().unwrap_or(0);
        let max = input.iter().map(|(v, _)| *v).max().unwrap_or(0);
        if max - min > u8::MAX as u64 {
            return Err(StorageError::OutOfBounds);
        }
        out.write_u64(min)?;
        out.write_u64(max)?;
        for &(v, num) in input.iter() {
            for _ in 0..num {
                out.write_u8((v - min) as u8)?;
            }
        }
        Ok(())
    }

    fn open(mut storage: Storage) -> Result<Self, StorageError> {
        let magic = storage.read_u64()?;
        if magic != MAGIC {
            return Err(StorageError::BadMagic(magic));
        }
        let n_rows = storage.read_u64()?;
        let n_chunks = storage.read_u64()?;
        let v_min = storage.read_u64()?;
        let v_max = storage.read_u64()?;
        Ok(Column {
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

impl TryFrom<Storage> for Column {
    type Error = StorageError;
    fn try_from(storage: Storage) -> Result<Self, Self::Error> {
        Self::open(storage)
    }
}

#[test]
fn encode_u64() {
    use super::RawColumn;

    let bools = [1, 1, 1, 1, 2, 2, 16, 1];
    let bc = Column::from(&bools[..]);
    let c = RawColumn::from(&bools[..]);
    assert_eq!(c.read_u64().unwrap().as_slice(), &bools);

    let mut encoded: Vec<u8> = Vec::new();
    let chunks: Vec<(u64, u64)> = bc
        .clone()
        .map(|chunk| {
            let chunk = chunk.unwrap();
            (chunk.value, chunk.range.end - chunk.range.start)
        })
        .collect();
    <Column as IsRawColumn>::encode(&mut encoded, chunks.as_slice()).unwrap();

    let storage = Storage::from(encoded.clone());
    let bc2 = Column::open(storage.clone()).unwrap();
    assert_eq!(
        bc2.map(|x| x.unwrap()).collect::<Vec<_>>(),
        bc.map(|x| x.unwrap()).collect::<Vec<_>>()
    );
    let c2 = RawColumn::decode(encoded).unwrap();
    assert_eq!(c2.read_u64().unwrap().as_slice(), &bools);

    let mut f = tempfile::tempfile().unwrap();
    <Column as IsRawColumn>::encode(&mut f, chunks.as_slice()).unwrap();
    let c = RawColumn::try_from(f).unwrap();
    assert_eq!(c.read_u64().unwrap().as_slice(), &bools);
}
