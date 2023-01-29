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
    /// The values of columns were invalid
    InvalidValue {
        /// The particular invalid value
        value: String,
    },
}

macro_rules! define_lens_id {
    ($tname:ident, $lensid:expr) => {
        #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
            const EXPECTED: &'static str = "[u8;16]";
            const NAMES: &'static [&'static str] = &[""];
        }
        impl From<$tname> for RawValues {
            fn from(id: $tname) -> Self {
                RawValues(vec![RawValue::Bytes(id.0.to_vec())])
            }
        }
        impl TryFrom<RawValues> for $tname {
            type Error = LensError;
            fn try_from(v: RawValues) -> Result<Self, LensError> {
                match &v.0.as_slice() {
                    &[RawValue::Bytes(b)] => {
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

        impl std::fmt::Display for $tname {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                if let Ok(s) = std::str::from_utf8(&self.0) {
                    if s.chars().any(char::is_whitespace) {
                        write!(f, "'{s}'")
                    } else {
                        write!(f, "{}", s.trim_end_matches('_'))
                    }
                } else {
                    for c in self.0.iter() {
                        write!(f, "{:x}", c)?;
                    }
                    Ok(())
                }
            }
        }

        impl $tname {
            /// Show this id as a filename
            pub fn as_filename(&self) -> std::path::PathBuf {
                let mut s = String::with_capacity(32);
                use std::fmt::Write;
                for c in self.0.iter() {
                    write!(&mut s, "{:x}", c).unwrap();
                }
                s.into()
            }
        }
        impl std::fmt::Debug for $tname {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                if let Ok(s) = std::str::from_utf8(&self.0) {
                    write!(f, "{}('{s}')", stringify!($tname))
                } else {
                    write!(f, "{}({:?})", stringify!($tname), self.0)
                }
            }
        }
    };
}

define_lens_id! {ColumnId, b"__ColumnId______"}
define_lens_id! {TableId, b"__TableId_______"}
define_lens_id! {LensId, b"__LensId________"}

/// A way of looking at a table or modifying it, a kind of pseudocolumn.
pub trait Lens: Into<RawValues> + TryFrom<RawValues, Error = LensError> {
    /// The kinds of raw columns involved
    const RAW_KINDS: &'static [RawKind];
    /// A stable unique identifier for this type.
    const LENS_ID: LensId;
    /// The expected kind error message;
    const EXPECTED: &'static str;
    /// Names
    const NAMES: &'static [&'static str];
}

impl Lens for u64 {
    const RAW_KINDS: &'static [RawKind] = &[RawKind::U64];
    const LENS_ID: LensId = LensId(*b"u64_____________");
    const EXPECTED: &'static str = "u64";
    const NAMES: &'static [&'static str] = &[""];
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
    const NAMES: &'static [&'static str] = &["seconds", "subsecond_nanos"];
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

impl Lens for String {
    const RAW_KINDS: &'static [RawKind] = &[RawKind::Bytes];
    const LENS_ID: LensId = LensId(*b"String__________");
    const EXPECTED: &'static str = "utf8 bytes";
    const NAMES: &'static [&'static str] = &[""];
}

impl From<String> for RawValues {
    fn from(v: String) -> Self {
        RawValues(vec![RawValue::Bytes(v.as_bytes().to_vec())])
    }
}

impl TryFrom<RawValues> for String {
    type Error = LensError;
    fn try_from(value: RawValues) -> Result<Self, Self::Error> {
        match value.0.as_slice() {
            [RawValue::Bytes(b)] => {
                String::from_utf8(b.clone()).map_err(|e| LensError::InvalidValue {
                    value: format!("{e}"),
                })
            }
            _ => Err(LensError::InvalidKinds {
                expected: Self::EXPECTED.to_string(),
            }),
        }
    }
}

impl Lens for bool {
    const RAW_KINDS: &'static [RawKind] = &[RawKind::Bool];
    const LENS_ID: LensId = LensId(*b"bool____________");
    const EXPECTED: &'static str = "bool";
    const NAMES: &'static [&'static str] = &[""];
}

impl From<bool> for RawValues {
    fn from(v: bool) -> Self {
        RawValues(vec![RawValue::Bool(v)])
    }
}

impl TryFrom<RawValues> for bool {
    type Error = LensError;
    fn try_from(value: RawValues) -> Result<Self, Self::Error> {
        match value.0.as_slice() {
            &[RawValue::Bool(b)] => Ok(b),
            _ => Err(LensError::InvalidKinds {
                expected: Self::EXPECTED.to_string(),
            }),
        }
    }
}
