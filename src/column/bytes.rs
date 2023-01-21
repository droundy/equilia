//! Will be private
use super::{
    encoding::BitWidth, Chunk, IsRawColumn, ReadEncoded, Storage, StorageError, WriteEncoded,
    BYTES_GENERIC_MAGIC,
};

#[derive(Clone)]
pub(crate) struct Bytes<const F: u64> {
    storage: Storage,
    current_row: u64,
    previous: Vec<u8>,
    n_rows: u64,
    n_chunks: u64,
    l_min: u64,
    v_min: Vec<u8>,
    v_max: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Format {
    length: BitWidth,
    prefix: BitWidth,
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
        let Some(length) = BitWidth::new(bytes[0]) else {
            return Err(StorageError::OutOfBounds("invalid length bitwidth"));
        };
        let Some(runlength) = BitWidth::new(bytes[1]) else {
            return Err(StorageError::OutOfBounds("invalide runlength bitwidth"));
        };
        let Some(prefix) = BitWidth::new(bytes[2]) else {
            return Err(StorageError::OutOfBounds("invalid prefix bitwidth"));
        };
        Ok(Format {
            length,
            prefix,
            runlength,
        })
    }
}

pub(crate) type VVV = Bytes<
    {
        Format {
            length: BitWidth::Variable,
            runlength: BitWidth::Variable,
            prefix: BitWidth::Variable,
        }
        .to_bytes()
    },
>;

pub(crate) type FVV = Bytes<
    {
        Format {
            length: BitWidth::IsZero,
            runlength: BitWidth::Variable,
            prefix: BitWidth::Variable,
        }
        .to_bytes()
    },
>;

impl Format {
    const fn to_bytes(self) -> u64 {
        let mut bytes = [0; 8];
        bytes[0] = self.length as u8;
        bytes[1] = self.runlength as u8;
        bytes[2] = self.prefix as u8;
        u64::from_be_bytes(bytes)
    }
}

impl<const F: u64> From<&[Vec<u8>]> for Bytes<F> {
    /// Create a column
    fn from(vals: &[Vec<u8>]) -> Self {
        let mut bytes = Vec::<u8>::new();
        Self::encode(&mut bytes, &super::run_length_encode(vals)).expect("error encoding");
        let storage = Storage::from(bytes);
        Self::open(storage).unwrap()
    }
}
impl<const F: u64> Iterator for Bytes<F> {
    type Item = Result<Chunk<Vec<u8>>, StorageError>;
    fn next(&mut self) -> Option<Self::Item> {
        self.transposed_next().transpose()
    }
}

impl<const F: u64> Bytes<F> {
    pub(crate) const MAGIC: u64 = F + BYTES_GENERIC_MAGIC;
    fn transposed_next(&mut self) -> Result<Option<Chunk<Vec<u8>>>, StorageError> {
        if self.current_row == self.n_rows {
            return Ok(None);
        }
        let format = Format::from_bytes(F)?;
        let num = self.storage.read_bitwidth(format.runlength)?;
        let length = self.l_min + self.storage.read_bitwidth(format.length)?;
        let prefix = self.storage.read_bitwidth(format.prefix)?;

        self.previous.truncate(prefix as usize);
        for _ in 0..(length - prefix) as usize {
            self.previous.push(0);
        }
        self.storage
            .read_exact(&mut self.previous[prefix as usize..length as usize])?;

        let value = self.previous.clone();
        let current_row = self.current_row;
        self.current_row = current_row + num;

        Ok(Some(Chunk {
            value,
            range: current_row..self.current_row,
        }))
    }
}
impl<const F: u64> IsRawColumn for Bytes<F> {
    type Element = Vec<u8>;

    fn num_rows(&self) -> u64 {
        self.n_rows
    }
    fn num_chunks(&self) -> u64 {
        self.n_chunks
    }
    fn max(&self) -> Self::Element {
        self.v_max.clone()
    }
    fn min(&self) -> Self::Element {
        self.v_min.clone()
    }

    fn encode<W: WriteEncoded>(
        out: &mut W,
        input: &[(Self::Element, u64)],
    ) -> Result<(), StorageError> {
        let format = Format::from_bytes(F)?;
        if input.is_empty() {
            return Ok(());
        }
        out.write_u64(Self::MAGIC)?;
        out.write_u64(input.iter().map(|x| x.1).sum())?;
        out.write_u64(input.len() as u64)?;
        let mut min = if input.is_empty() {
            Vec::new()
        } else {
            input[0].0.clone()
        };
        let mut max = Vec::new();
        let mut min_l = u64::MAX;
        let mut max_l = 0;

        for v in input.iter() {
            if v.0 < min {
                min = v.0.clone();
            }
            if v.0 > max {
                max = v.0.clone();
            }
            max_l = std::cmp::max(max_l, v.0.len() as u64);
            min_l = std::cmp::min(min_l, v.0.len() as u64);
        }
        if max_l - min_l > format.length.max() {
            return Err(StorageError::OutOfBounds("oops"));
        }
        out.write_u64(min_l)?;
        out.write_bitwidth(format.length, min.len() as u64 - min_l)?;
        out.write_all(&min)?;
        out.write_bitwidth(format.length, max.len() as u64 - min_l)?;
        out.write_all(&max)?;
        let mut prev = &(Vec::new(), 0);
        for v in input.iter() {
            out.write_bitwidth(format.runlength, v.1)?;
            out.write_bitwidth(format.length, v.0.len() as u64 - min_l)?;
            let prefix = std::cmp::min(prefix(&prev.0, &v.0) as u64, format.prefix.max());
            out.write_bitwidth(format.prefix, prefix)?;
            out.write_all(&v.0[prefix as usize..])?;
            prev = v;
        }
        Ok(())
    }

    fn open(mut storage: Storage) -> Result<Self, StorageError> {
        let format = Format::from_bytes(F)?;
        let magic = storage.read_u64()?;
        if magic != Self::MAGIC {
            return Err(StorageError::BadMagic(magic));
        }
        let n_rows = storage.read_u64()?;
        let n_chunks = storage.read_u64()?;
        let l_min = storage.read_u64()?;

        let len_min = storage.read_bitwidth(format.length)? + l_min;
        let mut v_min = vec![0; len_min as usize];
        storage.read_exact(v_min.as_mut_slice())?;

        let len_max = storage.read_bitwidth(format.length)? + l_min;
        let mut v_max = vec![0; len_max as usize];
        storage.read_exact(v_max.as_mut_slice())?;
        Ok(Bytes {
            storage,
            n_chunks,
            current_row: 0,
            n_rows,
            previous: Vec::new(),
            l_min,
            v_min,
            v_max,
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

fn prefix(xs: &[u8], ys: &[u8]) -> usize {
    let off = std::iter::zip(xs.chunks_exact(128), ys.chunks_exact(128))
        .take_while(|(x, y)| x == y)
        .count()
        * 128;
    off + std::iter::zip(&xs[off..], &ys[off..])
        .take_while(|(x, y)| x == y)
        .count()
}

impl<const F: u64> TryFrom<Storage> for Bytes<F> {
    type Error = StorageError;
    fn try_from(storage: Storage) -> Result<Self, Self::Error> {
        Self::open(storage)
    }
}

#[test]
fn test_encode_vvv() {
    use super::RawColumn;

    let data = [
        b"hello".to_vec(),
        b"hello".to_vec(),
        b"hello".to_vec(),
        b"goodbye".to_vec(),
    ];
    let c = VVV::from(data.as_slice());
    let rc = RawColumn::from(data.as_slice());
    assert_eq!(rc.read_bytes().unwrap().as_slice(), &data);

    let mut encoded: Vec<u8> = Vec::new();
    let chunks: Vec<(Vec<u8>, u64)> = c
        .clone()
        .map(|chunk| {
            let chunk = chunk.unwrap();
            (chunk.value, chunk.range.end - chunk.range.start)
        })
        .collect();
    <VVV as IsRawColumn>::encode(&mut encoded, chunks.as_slice()).unwrap();

    let storage = Storage::from(encoded.clone());
    let c2 = VVV::open(storage.clone()).unwrap();
    assert_eq!(
        c2.map(|x| x.unwrap()).collect::<Vec<_>>(),
        c.map(|x| x.unwrap()).collect::<Vec<_>>()
    );
    let rc2 = RawColumn::decode(encoded).unwrap();
    assert_eq!(rc2.read_bytes().unwrap().as_slice(), &data);

    let mut f = tempfile::tempfile().unwrap();
    <VVV as IsRawColumn>::encode(&mut f, chunks.as_slice()).unwrap();
    let rc = RawColumn::try_from(f).unwrap();
    assert_eq!(rc.read_bytes().unwrap().as_slice(), &data);
}

#[test]
fn test_encode_fvv() {
    use super::RawColumn;

    let data = [
        b"hello".to_vec(),
        b"hello".to_vec(),
        b"hello".to_vec(),
        b"goodb".to_vec(),
    ];
    let c = FVV::from(data.as_slice());
    let rc = RawColumn::from(data.as_slice());
    assert_eq!(rc.read_bytes().unwrap().as_slice(), &data);

    let mut encoded: Vec<u8> = Vec::new();
    let chunks: Vec<(Vec<u8>, u64)> = c
        .clone()
        .map(|chunk| {
            let chunk = chunk.unwrap();
            (chunk.value, chunk.range.end - chunk.range.start)
        })
        .collect();
    <FVV as IsRawColumn>::encode(&mut encoded, chunks.as_slice()).unwrap();

    let storage = Storage::from(encoded.clone());
    let c2 = FVV::open(storage.clone()).unwrap();
    assert_eq!(
        c2.map(|x| x.unwrap()).collect::<Vec<_>>(),
        c.map(|x| x.unwrap()).collect::<Vec<_>>()
    );
    let rc2 = RawColumn::decode(encoded).unwrap();
    assert_eq!(rc2.read_bytes().unwrap().as_slice(), &data);

    let mut f = tempfile::tempfile().unwrap();
    <FVV as IsRawColumn>::encode(&mut f, chunks.as_slice()).unwrap();
    let rc = RawColumn::try_from(f).unwrap();
    assert_eq!(rc.read_bytes().unwrap().as_slice(), &data);
}
