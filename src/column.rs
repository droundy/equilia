use crate::encoding::{read_u64, write_u64};

/// A raw column
pub struct RawColumn {
    /// FIXME should support mmap or File backing store also.
    bytes: Vec<u8>,
    inner: RawColumnInner,
}

fn run_length_encode<T: PartialEq + Clone>(elems: &[T]) -> Vec<(T, usize)> {
    let mut out = Vec::new();
    if let Some(mut previous) = elems.first() {
        let mut count = 0;
        for v in elems.iter() {
            if v == previous {
                count += 1;
            } else {
                out.push((previous.clone(), count));
                count = 0;
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
        let bytes = BoolColumn::encode(&run_length_encode(bools));
        let boolcolumn = BoolColumn {
            last: bools.first().copied().unwrap_or(false),
            offset: Offset(1),
        };
        RawColumn {
            bytes,
            inner: RawColumnInner::Bool(boolcolumn),
        }
    }
}

impl RawColumn {
    /// This isn't what we'll really want to use, but might be useful for testing?
    pub fn read_bools(&self) -> Result<Vec<bool>, ()> {
        match &self.inner {
            RawColumnInner::Bool(b) => {
                let mut out = Vec::new();
                let mut b = b.clone();
                while let Some((b, count, _)) = b.next(&self.bytes)? {
                    for _ in 0..count {
                        out.push(*b);
                    }
                }
                Ok(out)
            }
        }
    }
}

pub(crate) enum RawColumnInner {
    Bool(BoolColumn),
}

#[derive(Clone, Copy)]
pub(crate) struct BoolColumn {
    last: bool,
    offset: Offset,
}

/// A specific format for a [`RawColumn`].
///
/// Note that this type doubles as a kind of iterator, but a weird one where the
/// values are borrowed from the iterator not the data itself.
pub(crate) trait IsRawColumn<T>: Sized + Clone + Copy {
    /// Create a column from a set of values and run lengths
    ///
    /// Eventually we'll want to be able to write to a file instead
    /// of an in-memory buffer.
    ///
    /// Implementations may assume that two sequential T will not be equal.
    fn encode(input: &[(T, usize)]) -> Vec<u8>;

    /// Decode a column into this type and and an offset to the first element
    ///
    /// This does not read all the values, just the header.
    fn decode(buf: &[u8]) -> Result<(Self, Offset), ()>;

    /// Read the next chunk of identical elements, returning offset of next element
    fn next<'a, 'b>(&'a mut self, buf: &'b [u8]) -> Result<Option<(&'a T, usize, Offset)>, ()>;

    /// Read the next chunk of identical elements, returning offset of next element
    fn skip(&mut self, elem: &T, offset: Offset);
}

/// An offset into a Column
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Offset(usize);

impl IsRawColumn<bool> for BoolColumn {
    fn encode(input: &[(bool, usize)]) -> Vec<u8> {
        let mut bytes = Vec::new();
        if input.is_empty() {
            return bytes;
        }
        bytes.push(input[0].0 as u8);
        for &(_, count) in input {
            write_u64(&mut bytes, count as u64);
        }
        bytes
    }

    fn decode(buf: &[u8]) -> Result<(Self, Offset), ()> {
        let last = buf.first().copied().unwrap_or(0) == 1;
        Ok((
            BoolColumn {
                last,
                offset: Offset(1),
            },
            Offset(1),
        ))
    }

    fn next<'a, 'b>(&'a mut self, buf: &'b [u8]) -> Result<Option<(&'a bool, usize, Offset)>, ()> {
        if buf.len() <= self.offset.0 {
            return Ok(None);
        }
        self.last = !self.last;
        let (count, newbuf) = read_u64(&buf[self.offset.0..])?;
        self.offset.0 += buf.len() - newbuf.len();
        Ok(Some((&self.last, count as usize, self.offset)))
    }

    fn skip(&mut self, elem: &bool, offset: Offset) {
        self.last = !*elem;
        self.offset = offset;
    }
}
