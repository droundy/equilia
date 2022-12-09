#![deny(missing_docs)]
//! A nice columnar data store.

use internment::Intern;

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

impl RawValue {
    /// The `RawKind` of this value
    pub fn kind(&self) -> RawKind {
        match self {
            RawValue::Bool(_) => RawKind::Bool,
            RawValue::U64(_) => RawKind::U64,
            RawValue::Bytes(_) => RawKind::Bytes,
        }
    }
}

/// The type of a column.
///
/// This is a logical type, which will be stored as one or
/// more [`RawKind`] columns.
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
    /// The most significant value of a split column
    Split {
        /// The type of this column
        kind: Intern<Kind>,
        /// The number represented by this column
        units: u64,
        /// The units of the next larger column
        modulo: u64,
    },
}

/// A column schema
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnSchema {
    name: Intern<str>,
    kind: Kind,
}

/// A table schema
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableSchema {
    name: Intern<str>,
    primary: Vec<ColumnSchema>,
}

impl std::fmt::Display for TableSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Table {}", self.name)?;
        for c in self.primary.iter() {
            writeln!(f, "{}: {:?}", c.name, c.kind)?;
        }
        Ok(())
    }
}

#[test]
fn format_table() {
    let table = TableSchema {
        name: Intern::from("my-table"),
        primary: vec![
            ColumnSchema {
                name: Intern::from("date"),
                kind: Kind::DateTime,
            },
            ColumnSchema {
                name: Intern::from("name"),
                kind: Kind::String,
            },
        ],
    };
    let expected = expect_test::expect![[r#"
        Table my-table
        date: DateTime
        name: String
    "#]];
    expected.assert_eq(table.to_string().as_str());
}
