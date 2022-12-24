use std::ops::Range;

use crate::encoding::{read_u64, write_u64};

/// A raw column
pub struct RawColumn {
    /// FIXME should support mmap or File backing store also.
    bytes: Vec<u8>,
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
    /// This isn't what we'll really want to use, but might be useful for
    /// testing?
    ///
    /// It also illustrates how some common logic can be abstracted away into a
    /// helper function like the `column_to_vec` below.
    pub fn read_bools(&self) -> Result<Vec<bool>, ()> {
        match &self.inner {
            RawColumnInner::Bool(b) => column_to_vec(b, &self.bytes),
        }
    }
}

fn column_to_vec<C: IsRawColumn>(column: &C, buf: &[u8]) -> Result<Vec<C::Element>, ()> {
    let mut out = Vec::new();
    column.for_each(buf, |v, range| {
        for _ in range {
            out.push(v.clone());
        }
    })?;
    Ok(out)
}

pub(crate) enum RawColumnInner {
    Bool(BoolColumn),
}

#[derive(Clone, Copy)]
pub(crate) struct BoolColumn {
    last: bool,
    offset: Offset,
}

/// An error reading
pub struct Error;
/// A chunk of identical values.
pub struct Chunk<T> {
    value: T,
    count: u64,
    offset: Offset,
}

/// A specific format for a [`RawColumn`].
///
/// Note that this type doubles as a kind of iterator, but a weird one where the
/// values are borrowed from the iterator not the data itself.
pub(crate) trait IsRawColumn:
    Sized + Clone + Copy + Iterator<Item = Result<Chunk<Self::Element>, Error>>
{
    type Element: Clone;
    /// Create a column from a set of values and run lengths
    ///
    /// Eventually we'll want to be able to write to a file instead
    /// of an in-memory buffer.
    ///
    /// Implementations may assume that two sequential T will not be equal.
    fn encode(input: &[(Self::Element, u64)]) -> Vec<u8>;

    /// Decode a column into this type and and an offset to the first element
    ///
    /// This does not read all the values, just the header.
    fn decode(buf: &[u8]) -> Result<(Self, Offset), ()>;

    /// Read the next chunk of identical elements, returning offset of next element
    fn next_deprecated<'a, 'b>(
        &'a mut self,
        buf: &'b [u8],
    ) -> Result<Option<(&'a Self::Element, u64, Offset)>, ()>;

    /// Read the next chunk of identical elements, returning offset of next element
    fn skip(&mut self, elem: &Self::Element, offset: Offset);

    /// Do something for each value in the column.
    fn for_each(
        &self,
        buf: &[u8],
        mut f: impl FnMut(&Self::Element, Range<u64>),
    ) -> Result<(), ()> {
        let mut iter = self.clone();
        let mut i = 0;
        while let Some((v, count, _)) = iter.next_deprecated(buf)? {
            f(v, i..i + count as u64);
            i += count as u64;
        }
        Ok(())
    }
}

/// An offset into a Column
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Offset(usize);

impl Iterator for BoolColumn {
    type Item = Result<Chunk<bool>, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl IsRawColumn for BoolColumn {
    type Element = bool;
    fn encode(input: &[(bool, u64)]) -> Vec<u8> {
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

    fn next_deprecated<'a, 'b>(
        &'a mut self,
        buf: &'b [u8],
    ) -> Result<Option<(&'a bool, u64, Offset)>, ()> {
        if buf.len() <= self.offset.0 {
            return Ok(None);
        }
        self.last = !self.last;
        let (count, newbuf) = read_u64(&buf[self.offset.0..])?;
        self.offset.0 += buf.len() - newbuf.len();
        Ok(Some((&self.last, count, self.offset)))
    }

    fn skip(&mut self, elem: &bool, offset: Offset) {
        self.last = !*elem;
        self.offset = offset;
    }
}
