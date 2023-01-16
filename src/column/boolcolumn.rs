use super::{Chunk, IsRawColumn, ReadEncoded, Storage, StorageError, WriteEncoded, BOOL_MAGIC};

#[derive(Clone)]
pub(crate) struct BoolColumn {
    storage: Storage,
    current_row: u64,
    n_rows: u64,
    n_chunks: u64,
    last: bool,
}

impl From<&[bool]> for BoolColumn {
    fn from(bools: &[bool]) -> Self {
        let mut bytes = Vec::<u8>::new();
        BoolColumn::encode(&mut bytes, &super::run_length_encode(bools)).unwrap();
        println!("encoded is {bytes:?}");
        let storage = Storage::from(bytes);
        BoolColumn::open(storage).unwrap()
    }
}
impl Iterator for BoolColumn {
    type Item = Result<Chunk<bool>, StorageError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.transposed_next().transpose()
    }
}

impl BoolColumn {
    fn transposed_next(&mut self) -> Result<Option<Chunk<bool>>, StorageError> {
        if self.current_row == self.n_rows {
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
impl IsRawColumn for BoolColumn {
    type Element = bool;

    fn num_rows(&self) -> u64 {
        self.n_rows
    }
    fn num_chunks(&self) -> u64 {
        self.n_chunks
    }
    fn max(&self) -> Self::Element {
        self.n_chunks > 1 || !self.last
    }
    fn min(&self) -> Self::Element {
        !(self.n_chunks > 1) && self.last
    }

    fn encode<W: WriteEncoded>(
        out: &mut W,
        input: &[(Self::Element, u64)],
    ) -> Result<(), StorageError> {
        if input.is_empty() {
            return Ok(());
        }
        out.write_u64(BOOL_MAGIC)?;
        out.write_unsigned(input.iter().map(|x| x.1).sum())?;
        out.write_unsigned(input.len() as u64)?;
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
        let n_rows = storage.read_usigned()?;
        let n_chunks = storage.read_usigned()?;
        let last = storage.read_u8()? == 1;
        Ok(BoolColumn {
            storage,
            current_row: 0,
            n_chunks,
            n_rows,
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
    fn try_from(storage: Storage) -> Result<Self, Self::Error> {
        Self::open(storage)
    }
}

#[test]
fn encode_bools() {
    use super::{RawColumn, RawColumnInner};

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
