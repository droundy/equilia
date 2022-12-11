#![deny(missing_docs)]
//! A nice columnar data store.

use std::collections::BTreeSet;

mod column;
mod encoding;
mod value;

pub use column::RawColumn;
use value::{ColumnId, RawValue, TableId};
pub use value::{Kind, Value};

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

/// A column schema
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnSchema {
    id: ColumnId,
    default: Value,
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
    id: TableId,
    primary: Vec<ColumnSchema>,
    aggregation: BTreeSet<AggregatingSchema>,
}

impl std::fmt::Display for ColumnSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {:?} DEFAULT {}",
            self.id,
            self.default.kind(),
            self.default
        )
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
        write!(f, "    {keyword} ( {}", c.id)?;
        for c in v[1..].iter() {
            write!(f, ", {}", c.id)?;
        }
        writeln!(f, " ),")
    } else {
        Ok(())
    }
}

impl std::fmt::Display for TableSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "CREATE TABLE {} {{", self.id)?;
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
        id: TableId::from(b"__table_schemas_"),
        primary: vec![
            ColumnSchema {
                id: ColumnId::from(b"table_id--tables"),
                default: Value::Column(ColumnId::from(b"TABLE--NOT-EXIST")),
            },
            ColumnSchema {
                id: ColumnId::from(b"column_id-tables"),
                default: Value::Column(ColumnId::from(b"COLUMN-NOT-EXIST")),
            },
        ],
        aggregation: [
            AggregatingSchema::Max(vec![
                ColumnSchema {
                    id: ColumnId::from(b"modified-column!"),
                    default: Value::U64(0), // FIXME datetime
                },
                ColumnSchema {
                    id: ColumnId::from(b"name-of-column!!"),
                    default: Value::Bytes(Vec::new()),
                },
                ColumnSchema {
                    id: ColumnId::from(b"column-isdeleted"),
                    default: Value::Bool(false),
                },
            ]),
            AggregatingSchema::Min(vec![ColumnSchema {
                id: ColumnId::from(b"columnwascreated"),
                default: Value::U64(0),
            }]),
        ]
        .into_iter()
        .collect(),
    }
}

/// The schema of the tables of tables
pub fn db_schema_schema() -> TableSchema {
    TableSchema {
        id: TableId::from(b"__tables_in_db__"),
        primary: vec![ColumnSchema {
            id: ColumnId::from(b"table_id-in-db!!"),
            default: Value::U64(0),
        }],
        aggregation: [
            AggregatingSchema::Max(vec![
                ColumnSchema {
                    id: ColumnId::from(b"MODIFIED-TABLE.."),
                    default: Value::U64(0),
                },
                ColumnSchema {
                    id: ColumnId::from(b"name-of-table..."),
                    default: Value::Bytes(Vec::new()),
                },
                ColumnSchema {
                    id: ColumnId::from(b"table-is-deleted"),
                    default: Value::Bool(false),
                },
            ]),
            AggregatingSchema::Min(vec![ColumnSchema {
                id: ColumnId::from(b"table-wascreated"),
                default: Value::U64(0),
            }]),
        ]
        .into_iter()
        .collect(),
    }
}

#[test]
fn format_db_tables() {
    let expected = expect_test::expect![[r#"
        CREATE TABLE Table('__table_schemas_') {
            Column('table_id--tables') FixedBytes(16) DEFAULT Column('TABLE--NOT-EXIST'),
            Column('column_id-tables') FixedBytes(16) DEFAULT Column('COLUMN-NOT-EXIST'),
            Column('modified-column!') U64 DEFAULT 0,
            Column('name-of-column!!') Bytes DEFAULT '',
            Column('column-isdeleted') Bool DEFAULT false,
            Column('columnwascreated') U64 DEFAULT 0,
            PRIMARY KEY ( Column('table_id--tables'), Column('column_id-tables') ),
            MAX ( Column('modified-column!'), Column('name-of-column!!'), Column('column-isdeleted') ),
            MIN ( Column('columnwascreated') ),
        };
    "#]];
    expected.assert_eq(table_schema_schema().to_string().as_str());

    let expected = expect_test::expect![[r#"
        CREATE TABLE Table('__tables_in_db__') {
            Column('table_id-in-db!!') U64 DEFAULT 0,
            Column('MODIFIED-TABLE..') U64 DEFAULT 0,
            Column('name-of-table...') Bytes DEFAULT '',
            Column('table-is-deleted') Bool DEFAULT false,
            Column('table-wascreated') U64 DEFAULT 0,
            PRIMARY KEY ( Column('table_id-in-db!!') ),
            MAX ( Column('MODIFIED-TABLE..'), Column('name-of-table...'), Column('table-is-deleted') ),
            MIN ( Column('table-wascreated') ),
        };
    "#]];
    expected.assert_eq(db_schema_schema().to_string().as_str());
}
