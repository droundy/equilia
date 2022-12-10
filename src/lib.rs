#![deny(missing_docs)]
//! A nice columnar data store.

use internment::Intern;
use std::collections::BTreeSet;

mod value;

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

/// A "raw" row, as it will be sorted and stored.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RawRow {
    values: Vec<RawValue>,
}

impl FromIterator<RawValue> for RawRow {
    fn from_iter<T: IntoIterator<Item = RawValue>>(iter: T) -> Self {
        RawRow {
            values: iter.into_iter().collect(),
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
    /// A portion of a split column
    Split {
        /// The type of this column
        kind: Intern<Kind>,
        /// The number represented by this column
        units: u64,
        /// The units of the next larger column
        modulo: u64,
    },
}

/// A logical value that has a Kind.
pub enum Value {
    /// A `u64` value
    U64(u64),
    /// A boolean value
    Bool(bool),
    /// A bytes value
    Bytes(Vec<u8>),
    /// A bytes value with fixed length
    FixedBytes(Vec<u8>),
}

impl Value {
    /// The `RawKind` of this value
    pub fn kind(&self) -> RawKind {
        match self {
            Value::Bool(_) => RawKind::Bool,
            Value::U64(_) => RawKind::U64,
            Value::Bytes(_) => RawKind::Bytes,
            Value::FixedBytes(b) => RawKind::FixedBytes(b.len()),
        }
    }
}

/// A column schema
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnSchema {
    name: Intern<str>,
    kind: Kind,
    default: RawValue, // Should be a Value but I'm short on time to create that type.
}

/// A kind of column to aggregate
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AggregatingSchema {
    /// One or more columns, we pick the max of a pair
    Max(Vec<ColumnSchema>),
    /// One or more columns, we pick the min of a pair
    Min(Vec<ColumnSchema>),
    /// Summing
    Sum([ColumnSchema; 1]),
}

/// A table schema
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableSchema {
    name: Intern<str>,
    primary: Vec<ColumnSchema>,
    aggregation: BTreeSet<AggregatingSchema>,
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

impl std::fmt::Display for ColumnSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {:?} DEFAULT {}", self.name, self.kind, self.default)
    }
}

impl AggregatingSchema {
    fn columns(&self) -> impl Iterator<Item = &ColumnSchema> {
        match self {
            AggregatingSchema::Max(v) => v.iter(),
            AggregatingSchema::Min(v) => v.iter(),
            AggregatingSchema::Sum(c) => c.iter(),
        }
    }
}

impl TableSchema {
    /// Iterate over the columns in the schema
    pub fn columns(&self) -> impl Iterator<Item = &ColumnSchema> {
        self.primary
            .iter()
            .chain(self.aggregation.iter().flat_map(|a| a.columns()))
    }
}

fn column_list(
    keyword: &str,
    v: &[ColumnSchema],
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    if let Some(c) = v.first() {
        write!(f, "    {keyword} ( {}", c.name)?;
        for c in v[1..].iter() {
            write!(f, ", {}", c.name)?;
        }
        writeln!(f, " ),")
    } else {
        Ok(())
    }
}

impl std::fmt::Display for TableSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "CREATE TABLE {} {{", self.name)?;
        for c in self.columns() {
            writeln!(f, "    {c},")?;
        }
        column_list("PRIMARY KEY", &self.primary, f)?;
        for a in self.aggregation.iter() {
            match a {
                AggregatingSchema::Max(v) => column_list("MAX", v, f)?,
                AggregatingSchema::Min(v) => column_list("MIN", v, f)?,
                AggregatingSchema::Sum(c) => column_list("SUM", c, f)?,
            }
        }
        writeln!(f, "}};")
    }
}

/// The schema of the tables of tables
pub fn table_schema_schema() -> TableSchema {
    TableSchema {
        name: Intern::from("__table_schemas__"),
        primary: vec![
            ColumnSchema {
                name: Intern::from("table_id"),
                kind: Kind::Uuid,
                default: RawValue::FixedBytes(vec![0; 16]),
            },
            ColumnSchema {
                name: Intern::from("column_id"),
                kind: Kind::Uuid,
                default: RawValue::FixedBytes(vec![0; 16]),
            },
        ],
        aggregation: [
            AggregatingSchema::Max(vec![
                ColumnSchema {
                    name: Intern::from("modified"),
                    kind: Kind::DateTime,
                    default: RawValue::U64(0),
                },
                ColumnSchema {
                    name: Intern::from("name"),
                    kind: Kind::String,
                    default: RawValue::Bytes(Vec::new()),
                },
                ColumnSchema {
                    name: Intern::from("deleted"),
                    kind: Kind::Deleted,
                    default: RawValue::Bool(false),
                },
            ]),
            AggregatingSchema::Min(vec![ColumnSchema {
                name: Intern::from("created"),
                kind: Kind::DateTime,
                default: RawValue::U64(0),
            }]),
        ]
        .into_iter()
        .collect(),
    }
}

/// The schema of the tables of tables
pub fn db_schema_schema() -> TableSchema {
    TableSchema {
        name: Intern::from("__tables__"),
        primary: vec![ColumnSchema {
            name: Intern::from("table_id"),
            kind: Kind::Uuid,
            default: RawValue::U64(0),
        }],
        aggregation: [
            AggregatingSchema::Max(vec![
                ColumnSchema {
                    name: Intern::from("modified"),
                    kind: Kind::DateTime,
                    default: RawValue::U64(0),
                },
                ColumnSchema {
                    name: Intern::from("name"),
                    kind: Kind::String,
                    default: RawValue::Bytes(Vec::new()),
                },
                ColumnSchema {
                    name: Intern::from("deleted"),
                    kind: Kind::Deleted,
                    default: RawValue::Bool(false),
                },
            ]),
            AggregatingSchema::Min(vec![ColumnSchema {
                name: Intern::from("created"),
                kind: Kind::DateTime,
                default: RawValue::U64(0),
            }]),
        ]
        .into_iter()
        .collect(),
    }
}

#[test]
fn format_db_tables() {
    let expected = expect_test::expect![[r#"
        CREATE TABLE __table_schemas__ {
            table_id Uuid DEFAULT '                ',
            column_id Uuid DEFAULT '                ',
            modified DateTime DEFAULT 0,
            name String DEFAULT '',
            deleted Deleted DEFAULT false,
            created DateTime DEFAULT 0,
            PRIMARY KEY ( table_id, column_id ),
            MAX ( modified, name, deleted ),
            MIN ( created ),
        };
    "#]];
    expected.assert_eq(table_schema_schema().to_string().as_str());

    let expected = expect_test::expect![[r#"
        CREATE TABLE __tables__ {
            table_id Uuid DEFAULT 0,
            modified DateTime DEFAULT 0,
            name String DEFAULT '',
            deleted Deleted DEFAULT false,
            created DateTime DEFAULT 0,
            PRIMARY KEY ( table_id ),
            MAX ( modified, name, deleted ),
            MIN ( created ),
        };
    "#]];
    expected.assert_eq(db_schema_schema().to_string().as_str());
}
