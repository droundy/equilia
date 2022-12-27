use super::{Chunk, IsRawColumn, ReadEncoded, Storage, StorageError, WriteEncoded, U64_MAGIC};

#[derive(Clone)]
pub(crate) struct U64Column {
    storage: Storage,
    current_row: u64,
    num_rows: u64,
    last: bool,
}

impl From<&[bool]> for U64Column {
    fn from(bools: &[bool]) -> Self {
        let mut bytes = Vec::<u8>::new();
        U64Column::encode(&mut bytes, &super::run_length_encode(bools)).unwrap();
        println!("encoded is {bytes:?}");
        let storage = Storage::from(bytes);
        U64Column::open(storage).unwrap()
    }
}
impl Iterator for U64Column {
    type Item = Result<Chunk<bool>, StorageError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.transposed_next().transpose()
    }
}

impl U64Column {
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
impl IsRawColumn for U64Column {
    type Element = bool;
    fn encode<W: WriteEncoded>(
        out: &mut W,
        input: &[(Self::Element, u64)],
    ) -> Result<(), StorageError> {
        if input.is_empty() {
            return Ok(());
        }
        out.write_u64(U64_MAGIC)?;
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
        if magic != U64_MAGIC {
            return Err(StorageError::BadMagic(magic));
        }
        let num_rows = storage.read_usigned()?;
        let last = storage.read_u8()? == 1;
        Ok(U64Column {
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

impl TryFrom<Storage> for U64Column {
    type Error = StorageError;
    fn try_from(mut storage: Storage) -> Result<Self, Self::Error> {
        let num_rows = storage.read_usigned()?;
        let last = storage.read_u8()? == 1;
        Ok(U64Column {
            storage,
            last,
            num_rows,
            current_row: 0,
        })
    }
}

#[test]
fn encode_bools() {
    use super::{RawColumn, RawColumnInner};

    let bools = [true, true, false, true, true, true];
    let bc = U64Column::from(&bools[..]);
    let c = RawColumn {
        inner: RawColumnInner::U64(bc.clone()),
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
    <U64Column as IsRawColumn>::encode(&mut encoded, chunks.as_slice()).unwrap();

    let storage = Storage::from(encoded.clone());
    let bc2 = U64Column::open(storage.clone()).unwrap();
    assert_eq!(
        bc2.map(|x| x.unwrap()).collect::<Vec<_>>(),
        bc.map(|x| x.unwrap()).collect::<Vec<_>>()
    );
    let c2 = RawColumn::decode(encoded).unwrap();
    assert_eq!(c2.read_bools().unwrap().as_slice(), &bools);

    let mut f = tempfile::tempfile().unwrap();
    <U64Column as IsRawColumn>::encode(&mut f, chunks.as_slice()).unwrap();
    let c = RawColumn::try_from(f).unwrap();
    assert_eq!(c.read_bools().unwrap().as_slice(), &bools);
}
