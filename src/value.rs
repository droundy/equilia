/// The type of data actually stored in a column.
///
/// This is in distinction from a logical [`Kind`], which might
/// perform some transformation on the raw type, such as a
/// `DateTime` that might be stored as a `RawKind::U64` of
/// seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RawKind {
    /// A 64-bit integer
    U64,
    /// A boolean value
    Bool,
    /// A sequence of bytes
    Bytes,
    /// A sequence of bytes with fixed length
    FixedBytes(usize),
}

/// A value that could exist in a column
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RawValue {
    /// A `u64` value
    U64(u64),
    /// A boolean value
    Bool(bool),
    /// A bytes value
    Bytes(Vec<u8>),
    /// A bytes value with fixed length
    FixedBytes(Vec<u8>),
}

impl RawValue {
    /// The `RawKind` of this value
    pub fn kind(&self) -> RawKind {
        match self {
            RawValue::Bool(_) => RawKind::Bool,
            RawValue::U64(_) => RawKind::U64,
            RawValue::Bytes(_) => RawKind::Bytes,
            RawValue::FixedBytes(b) => RawKind::FixedBytes(b.len()),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut v = vec![];
        match self {
            RawValue::U64(number) => {
                v.push(0);
                v.extend(number.to_be_bytes());
            }
            RawValue::Bool(b) => {
                v.push(1);
                v.push(*b as u8);
            }
            RawValue::Bytes(bytes) => {
                v.push(2);
                v.push(bytes.len().try_into().unwrap());
                v.extend(bytes);
            }
            RawValue::FixedBytes(bytes) => {
                v.push(3);
                v.push(bytes.len().try_into().unwrap());
                v.extend(bytes);
            }
        }

        v
    }

    pub fn decode(data: &[u8]) -> Result<(Self, &[u8]), std::io::Error> {
        if data.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "no data",
            ));
        }

        match data[0] {
            0 => Ok((
                Self::U64(u64::from_be_bytes(data[1..].try_into().unwrap())),
                &[],
            )),
            1 => Ok((Self::Bool(data[1] != 0), &[])),
            2 => {
                let len = data[1] as usize;
                let bytes = data[2..2 + len].to_vec();
                Ok((Self::Bytes(bytes), &data[2 + len..]))
            }
            3 => {
                let len = data[1] as usize;
                let bytes = data[2..2 + len].to_vec();
                Ok((Self::FixedBytes(bytes), &data[2 + len..]))
            }
            _ => unreachable!(),
        }
    }
}

impl std::fmt::Display for RawValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RawValue::Bool(b) => write!(f, "{b:?}"),
            RawValue::U64(n) => write!(f, "{n}"),
            RawValue::FixedBytes(x) | RawValue::Bytes(x) => {
                if let Ok(s) = std::str::from_utf8(x) {
                    write!(f, "'{s}'")
                } else {
                    write!(f, "{x:?}")
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::RawValue;

    #[test]
    fn encode_bool() {
        {
            let value = RawValue::Bool(false);
            let output = value.encode();
            let expected = vec![1, 0];
            assert_eq!(expected, output);
        }
        {
            let value = RawValue::Bool(true);
            let output = value.encode();
            let expected = vec![1, 1];
            assert_eq!(expected, output);
        }
    }

    #[test]
    fn decode_bool() {
        {
            let data = vec![1, 0];
            let output = RawValue::decode(&data).unwrap();
            let expected = RawValue::Bool(false);
            assert_eq!(expected, output.0);
        }
        {
            let data = vec![1, 1];
            let output = RawValue::decode(&data).unwrap();
            let expected = RawValue::Bool(true);
            assert_eq!(expected, output.0);
        }
    }

    #[test]
    fn encode_u64() {
        let value = RawValue::U64(999_999_999);
        let output = value.encode();
        let expected = vec![0, 0, 0, 0, 0, 59, 154, 201, 255];
        assert_eq!(expected, output);
    }

    #[test]
    fn decode_u64() {
        let data = vec![0, 0, 0, 0, 0, 59, 154, 201, 255];
        let output = RawValue::decode(&data).unwrap();
        let expected = RawValue::U64(999_999_999);
        assert_eq!(expected, output.0);
    }

    #[test]
    fn encode_bytes() {
        let value = RawValue::Bytes(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]);
        let output = value.encode();
        let expected = vec![2, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0];
        assert_eq!(expected, output);
    }

    #[test]
    fn decode_bytes() {
        {
            let data = vec![2, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0];
            let output = RawValue::decode(&data).unwrap();
            let expected = RawValue::Bytes(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]);
            assert_eq!(expected, output.0);
        }
        {
            let data = vec![2, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 9, 9, 9];
            let output = RawValue::decode(&data).unwrap();
            let expected = (
                RawValue::Bytes(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
                &data[12..],
            );
            assert_eq!(expected, output);
        }
    }

    #[test]
    fn encode_fixedbytes() {
        let value = RawValue::FixedBytes(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]);
        let output = value.encode();
        let expected = vec![3, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0];
        assert_eq!(expected, output);
    }

    #[test]
    fn decode_fixedbytes() {
        {
            let data = vec![3, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0];
            let output = RawValue::decode(&data).unwrap();
            let expected = RawValue::FixedBytes(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]);
            assert_eq!(expected, output.0);
        }
        {
            let data = vec![3, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 9, 9, 9];
            let output = RawValue::decode(&data).unwrap();
            let expected = (
                RawValue::FixedBytes(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 0]),
                &data[12..],
            );
            assert_eq!(expected, output);
        }
    }
}
