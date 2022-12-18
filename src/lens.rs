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

macro_rules! define_lens_id {
    ($tname:ident, $lensid:expr) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $tname(pub(crate) [u8; 16]);

        impl $tname {
            pub fn new() -> Self {
                Self(rand::random())
            }
            #[allow(dead_code)]
            pub(crate) const fn const_new(b: &[u8; 16]) -> Self {
                Self(*b)
            }
        }

        impl Lens for $tname {
            const RAW_KINDS: &'static [RawKind] = &[RawKind::U64];
            const LENS_ID: LensId = LensId(*$lensid);
            const EXPECTED: &'static str = "u64";
        }
        impl From<$tname> for RawValues {
            fn from(id: $tname) -> Self {
                RawValues(vec![RawValue::FixedBytes(id.0.to_vec())])
            }
        }
        impl TryFrom<RawValues> for $tname {
            type Error = LensError;
            fn try_from(v: RawValues) -> Result<Self, LensError> {
                match &v.0.as_slice() {
                    &[RawValue::FixedBytes(b)] => {
                        if let Ok(b) = b.as_slice().try_into() {
                            Ok(Self(b))
                        } else {
                            Err(LensError::InvalidKinds {
                                expected: Self::EXPECTED.to_string(),
                            })
                        }
                    }
                    _ => Err(LensError::InvalidKinds {
                        expected: Self::EXPECTED.to_string(),
                    }),
                }
            }
        }
    };
}

define_lens_id! {ColumnId, b"__column_id_____"}
define_lens_id! {TableId, b"__table_id______"}

/// A compound aggregation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LensId([u8; 16]);

/// A way of looking at a table or modifying it, a kind of pseudocolumn.
pub trait Lens: Into<RawValues> + TryFrom<RawValues, Error = LensError> {
    /// The kinds of raw columns involved
    const RAW_KINDS: &'static [RawKind];
    /// A stable unique identifier for this type.
    const LENS_ID: LensId;
    /// The expected kind error message;
    const EXPECTED: &'static str;
}

impl Lens for u64 {
    const RAW_KINDS: &'static [RawKind] = &[RawKind::U64];
    const LENS_ID: LensId = LensId(*b"just a u64 only!");
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
    const LENS_ID: LensId = LensId(*b"time::SystemTime");
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
