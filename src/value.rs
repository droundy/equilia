use crate::{
    lens::{LensId, RawValues},
    Lens, LensError,
};

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
}

impl std::fmt::Display for RawKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RawKind::Bool => f.write_str("bool"),
            RawKind::Bytes => f.write_str("bytes"),
            RawKind::U64 => f.write_str("u64"),
        }
    }
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
}

impl TryFrom<RawValues> for RawValue {
    type Error = LensError;
    fn try_from(value: RawValues) -> Result<Self, Self::Error> {
        let badvalue = Err(LensError::InvalidKinds {
            expected: "a serialized RawValue".to_string(),
            found: value.clone(),
            context: Vec::new(),
        });
        if let [RawValue::Bytes(b)] = value.0.as_slice() {
            match b.first() {
                None => badvalue,
                Some(0) => {
                    if b.len() != 2 || b[1] > 1 {
                        badvalue
                    } else {
                        Ok(RawValue::Bool(b[1] == 1))
                    }
                }
                Some(1) => {
                    if b.len() != 9 {
                        badvalue
                    } else {
                        Ok(RawValue::U64(u64::from_be_bytes(
                            b[1..9].try_into().unwrap(),
                        )))
                    }
                }
                Some(2) => Ok(RawValue::Bytes(b[1..].to_vec())),
                Some(_) => badvalue,
            }
        } else {
            badvalue
        }
    }
}
impl From<RawValue> for RawValues {
    fn from(v: RawValue) -> Self {
        let bytes = match v {
            RawValue::Bool(b) => vec![0, b as u8],
            RawValue::U64(v) => {
                let mut bytes = Vec::with_capacity(9);
                bytes.push(1);
                bytes.extend(v.to_be_bytes());
                bytes
            }
            RawValue::Bytes(b) => {
                let mut bytes = Vec::with_capacity(9);
                bytes.push(2);
                bytes.extend(b);
                bytes
            }
        };
        RawValues(vec![RawValue::Bytes(bytes)])
    }
}

impl Lens for RawValue {
    const RAW_KINDS: &'static [RawKind] = &[RawKind::Bytes];

    const LENS_ID: crate::lens::LensId = LensId(*b"rawvalue________");

    const EXPECTED: &'static str = "a serialized RawValue";

    const NAMES: &'static [&'static str] = &["value"];
}

impl RawValue {
    /// The `RawKind` of this value
    pub fn kind(&self) -> RawKind {
        match self {
            RawValue::Bool(_) => RawKind::Bool,
            RawValue::U64(_) => RawKind::U64,
            RawValue::Bytes(_) => RawKind::Bytes,
        }
    }

    pub(crate) fn assert_bool(&self) -> bool {
        if let RawValue::Bool(b) = self {
            *b
        } else {
            panic!("Found {} rather than bool", self.kind());
        }
    }

    pub(crate) fn assert_u64(&self) -> u64 {
        if let RawValue::U64(v) = self {
            *v
        } else {
            panic!("Found {} rather than u64", self.kind());
        }
    }

    pub(crate) fn assert_bytes(&self) -> Vec<u8> {
        if let RawValue::Bytes(v) = self {
            v.clone()
        } else {
            panic!("Found {} rather than bytes", self.kind());
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
            _ => unreachable!(),
        }
    }
}

impl std::fmt::Display for RawValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RawValue::Bool(b) => write!(f, "{b:?}"),
            RawValue::U64(n) => write!(f, "{n}"),
            RawValue::Bytes(x) => {
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
}
