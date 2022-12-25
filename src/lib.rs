#![deny(missing_docs)]
//! A nice columnar data store.

use std::collections::BTreeSet;

pub mod column;
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
    comment: Option<String>,
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
                comment: Some("The table this column is in.".into()),
            },
            ColumnSchema {
                id: ColumnId::from(b"column_id-tables"),
                default: Value::Column(ColumnId::from(b"COLUMN-NOT-EXIST")),
                comment: Some("The id of the column.".into()),
            },
            ColumnSchema {
                id: ColumnId::from(b"column-sortorder"),
                default: Value::U64(0),
                comment: Some("The sort order where the column shows up.".into()),
            },
            ColumnSchema {
                id: ColumnId::from(b"column-aggregate"),
                default: Value::U64(0),
                comment: Some("0: primary, 1: max, 2: min, 3: sum.".into()),
            },
        ],
        aggregation: [
            AggregatingSchema::Max(vec![
                ColumnSchema {
                    id: ColumnId::from(b"modified-column!"),
                    default: Value::U64(0), // FIXME datetime
                    comment: Some("The time this column was modified.".into()),
                },
                ColumnSchema {
                    id: ColumnId::from(b"name-of-column!!"),
                    default: Value::Bytes(Vec::new()),
                    comment: Some("The name of the column.".into()),
                },
                ColumnSchema {
                    id: ColumnId::from(b"column-isdeleted"),
                    default: Value::Bool(false),
                    comment: Some("Whether this column has been deleted.".into()),
                },
                ColumnSchema {
                    id: ColumnId::from(b"column-comment.."),
                    default: Value::Bytes(Vec::new()),
                    comment: Some("A human-friendly description of this column.".into()),
                },
            ]),
            AggregatingSchema::Min(vec![ColumnSchema {
                id: ColumnId::from(b"columnwascreated"),
                default: Value::U64(0),
                comment: Some("The time this column was created.".into()),
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
            comment: Some("The id of this table".into()),
        }],
        aggregation: [
            AggregatingSchema::Max(vec![
                ColumnSchema {
                    id: ColumnId::from(b"MODIFIED-TABLE.."),
                    default: Value::U64(0),
                    comment: Some("When the table was last modified.".into()),
                },
                ColumnSchema {
                    id: ColumnId::from(b"name-of-table..."),
                    default: Value::Bytes(Vec::new()),
                    comment: Some("The name of the table.".into()),
                },
                ColumnSchema {
                    id: ColumnId::from(b"table-is-deleted"),
                    default: Value::Bool(false),
                    comment: Some("Whether this table has been deleted.".into()),
                },
            ]),
            AggregatingSchema::Min(vec![ColumnSchema {
                id: ColumnId::from(b"table-wascreated"),
                default: Value::U64(0),
                comment: Some("When this table was created.".into()),
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
            `table_id--tables` FixedBytes(16) DEFAULT `TABLE--NOT-EXIST`,
            `column_id-tables` FixedBytes(16) DEFAULT `COLUMN-NOT-EXIST`,
            `column-sortorder` U64 DEFAULT 0,
            `column-aggregate` U64 DEFAULT 0,
            `modified-column!` U64 DEFAULT 0,
            `name-of-column!!` Bytes DEFAULT '',
            `column-isdeleted` Bool DEFAULT false,
            `column-comment..` Bytes DEFAULT '',
            `columnwascreated` U64 DEFAULT 0,
            PRIMARY KEY ( `table_id--tables`, `column_id-tables`, `column-sortorder`, `column-aggregate` ),
            MAX ( `modified-column!`, `name-of-column!!`, `column-isdeleted`, `column-comment..` ),
            MIN ( `columnwascreated` ),
        };
    "#]];
    expected.assert_eq(table_schema_schema().to_string().as_str());

    let expected = expect_test::expect![[r#"
        CREATE TABLE Table('__tables_in_db__') {
            `table_id-in-db!!` U64 DEFAULT 0,
            `MODIFIED-TABLE..` U64 DEFAULT 0,
            `name-of-table...` Bytes DEFAULT '',
            `table-is-deleted` Bool DEFAULT false,
            `table-wascreated` U64 DEFAULT 0,
            PRIMARY KEY ( `table_id-in-db!!` ),
            MAX ( `MODIFIED-TABLE..`, `name-of-table...`, `table-is-deleted` ),
            MIN ( `table-wascreated` ),
        };
    "#]];
    expected.assert_eq(db_schema_schema().to_string().as_str());
}
