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

/// The type of a column.
///
/// This is a logical type, which will be stored as one or more [`RawKind`]
/// columns.  We use the name "kind" for types for the convenience of not using
/// a reserved keyword.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Kind {
    /// A 64-bit integer
    U64,
    /// A boolean value
    Bool,
    /// A sequence of bytes
    Bytes,
    /// A utf8-encoded string
    String,
    /// A date and time with 1-second resolution
    DateTime,
    /// A uuid
    Uuid,
    /// A boolean column that if true means the value is deleted
    Deleted,
    /// A DateTime column that indicates when a row should be deleted
    TTL,
    /// A column uuid
    Column,
    /// A table uuid
    Table,
}

/// A column id
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnId([u8; 16]);

impl std::fmt::Display for ColumnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Ok(s) = std::str::from_utf8(&self.0) {
            write!(f, "Column('{s}')")
        } else {
            write!(f, "Column({:?})", self.0)
        }
    }
}

impl From<&[u8; 16]> for ColumnId {
    fn from(bytes: &[u8; 16]) -> Self {
        ColumnId(*bytes)
    }
}

/// A column id
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableId([u8; 16]);

impl std::fmt::Display for TableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Ok(s) = std::str::from_utf8(&self.0) {
            write!(f, "Table('{s}')")
        } else {
            write!(f, "Table({:?})", self.0)
        }
    }
}

impl From<&[u8; 16]> for TableId {
    fn from(bytes: &[u8; 16]) -> Self {
        TableId(*bytes)
    }
}

/// A logical value that has a Kind.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Value {
    /// A `u64` value
    U64(u64),
    /// A boolean value
    Bool(bool),
    /// A bytes value
    Bytes(Vec<u8>),
    /// A bytes value with fixed length
    FixedBytes(Vec<u8>),
    /// A column Uuid
    Column(ColumnId),
}

impl Value {
    /// The `RawKind` of this value
    pub fn kind(&self) -> RawKind {
        match self {
            Value::Bool(_) => RawKind::Bool,
            Value::U64(_) => RawKind::U64,
            Value::Bytes(_) => RawKind::Bytes,
            Value::FixedBytes(b) => RawKind::FixedBytes(b.len()),
            Value::Column(b) => RawKind::FixedBytes(b.0.len()),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Bool(b) => write!(f, "{b:?}"),
            Value::U64(n) => write!(f, "{n}"),
            Value::FixedBytes(x) | Value::Bytes(x) => {
                if let Ok(s) = std::str::from_utf8(x) {
                    write!(f, "'{s}'")
                } else {
                    write!(f, "{x:?}")
                }
            }
            Value::Column(x) => write!(f, "{x}"),
        }
    }
}
