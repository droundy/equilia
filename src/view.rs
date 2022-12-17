use crate::value::{RawKind, RawValue};

/// A vec of values
pub struct RawValues(pub Vec<RawValue>);

/// A conversion error
pub enum LensError {
    /// The kinds of columns were invalid
    InvalidKinds {
        /// A human-friendly description of the format of this type.
        expected: String,
    },
}

/// A way of looking at a table or modifying it, a kind of pseudocolumn.
pub trait Lens: Into<RawValues> + TryFrom<RawValues, Error = LensError> {
    /// The kinds of raw columns involved
    const RAW_KINDS: &'static [RawKind];
    /// A stable unique identifier for this type.
    const UUID: [u8; 16];
    /// The expected kind error message;
    const EXPECTED: &'static str;
}

impl Lens for u64 {
    const RAW_KINDS: &'static [RawKind] = &[RawKind::U64];
    const UUID: [u8; 16] = *b"just a u64 only!";
    const EXPECTED: &'static str = "u64";
}

impl From<u64> for RawValues {
    fn from(v: u64) -> Self {
        RawValues(vec![RawValue::U64(v)])
    }
}
impl TryFrom<RawValues> for u64 {
    type Error = LensError;
    fn try_from(value: RawValues) -> Result<Self, Self::Error> {
        match value.0.as_slice() {
            &[RawValue::U64(v)] => Ok(v),
            _ => Err(LensError::InvalidKinds {
                expected: Self::EXPECTED.to_string(),
            }),
        }
    }
}

impl Lens for std::time::SystemTime {
    const RAW_KINDS: &'static [RawKind] = &[RawKind::U64, RawKind::U64];
    const UUID: [u8; 16] = *b"time::SystemTime";
    const EXPECTED: &'static str = "seconds: u64, nanos: u64";
}

impl From<std::time::SystemTime> for RawValues {
    fn from(t: std::time::SystemTime) -> Self {
        let d = t.duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap();
        RawValues(vec![
            RawValue::U64(d.as_secs()),
            RawValue::U64(d.subsec_nanos() as u64),
        ])
    }
}

impl TryFrom<RawValues> for std::time::SystemTime {
    type Error = LensError;
    fn try_from(value: RawValues) -> Result<Self, Self::Error> {
        use std::time::{Duration, SystemTime};
        match value.0.as_slice() {
            &[RawValue::U64(secs), RawValue::U64(nanos)] => Ok(SystemTime::UNIX_EPOCH
                + Duration::from_secs(secs)
                + Duration::from_nanos(nanos)),
            _ => Err(LensError::InvalidKinds {
                expected: Self::EXPECTED.to_string(),
            }),
        }
    }
}
