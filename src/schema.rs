use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::column::encoding::StorageError;
use crate::lens::{ColumnId, Lens, LensId, RawValues, TableId};
use crate::table::IsRow;
use crate::value::{RawKind, RawValue};
use crate::{LensError, Table, TableBuilder};

/// A kind of column to aggregate
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u64)]
pub enum Aggregation {
    None = 0,
    Min = 1,
    Max = 2,
    Sum = 3,
}
impl Lens for Aggregation {
    const RAW_KINDS: &'static [crate::value::RawKind] = u64::RAW_KINDS;
    const EXPECTED: &'static str = "An integer indicating which aggregation";
    const LENS_ID: LensId = LensId(*b"__Aggregation___");
    const NAMES: &'static [&'static str] = &[""];
}
impl From<Aggregation> for RawValues {
    fn from(a: Aggregation) -> Self {
        (a as u64).into()
    }
}
impl TryFrom<RawValues> for Aggregation {
    type Error = LensError;
    fn try_from(value: RawValues) -> Result<Self, Self::Error> {
        let v = u64::try_from(value)?;
        if v == Aggregation::None as u64 {
            Ok(Aggregation::None)
        } else if v == Aggregation::Max as u64 {
            Ok(Aggregation::Max)
        } else if v == Aggregation::Min as u64 {
            Ok(Aggregation::Min)
        } else if v == Aggregation::Sum as u64 {
            Ok(Aggregation::Sum)
        } else {
            Err(LensError::InvalidValue {
                value: format!("Unexpected: {v}"),
            })
        }
    }
}

/// A schema for a column
pub struct ColumnSchema<T> {
    default: T,
    name: &'static str,
    id: ColumnId,
}

/// A kind of column to aggregate
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RawColumnSchema {
    default: RawValue,
    name: &'static str,
    id: ColumnId,
    fieldname: &'static str,
    lens: LensId,
}
impl RawColumnSchema {
    fn display_name(&self) -> String {
        if self.fieldname.is_empty() {
            self.name.to_owned()
        } else {
            format!("{}.{}", self.name, self.fieldname,)
        }
    }
    pub(crate) fn file_name(&self) -> PathBuf {
        self.id.as_filename()
    }

    pub(crate) fn kind(&self) -> RawKind {
        self.default.kind()
    }
}
impl std::fmt::Display for RawColumnSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {:?} DEFAULT {} LENS {}",
            self.display_name(),
            self.default.kind(),
            self.default,
            self.lens,
        )
    }
}
/// A compound aggregation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AggregationId([u8; 16]);
/// A kind of column to aggregate
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AggregatingSchema {
    /// One or more columns, we pick the max of a pair
    Max {
        columns: OrderedRawColumns,
        id: AggregationId,
    },
    /// One or more columns, we pick the min of a pair
    Min {
        columns: OrderedRawColumns,
        id: AggregationId,
    },
    /// Summing
    Sum(OrderedRawColumns),
}

impl AggregatingSchema {
    fn columns(&self) -> impl Iterator<Item = &(u64, RawColumnSchema)> {
        match self {
            AggregatingSchema::Max { columns, .. } => columns.iter(),
            AggregatingSchema::Min { columns, .. } => columns.iter(),
            AggregatingSchema::Sum(columns) => columns.iter(),
        }
    }
}

type OrderedRawColumns = BTreeSet<(u64, RawColumnSchema)>;

/// The schema of a table
pub struct TableSchema {
    name: &'static str,
    pub(crate) id: TableId,
    primary: OrderedRawColumns, // must all have AggregationNone
    aggregations: BTreeSet<AggregatingSchema>,
}

impl TableSchema {
    /// Create a new empty table
    pub fn new(name: &'static str) -> Self {
        TableSchema {
            name,
            id: TableId::new(),
            primary: BTreeSet::new(),
            aggregations: BTreeSet::new(),
        }
    }

    /// Add columns to the primary key
    pub fn add_primary(&mut self, columns: impl Iterator<Item = RawColumnSchema>) {
        let first_order = if let Some(o) = self.primary.iter().next_back() {
            o.0 + 1
        } else {
            0
        };
        for (o, c) in columns.enumerate() {
            self.primary.insert((first_order + o as u64, c));
        }
    }

    /// Add max aggregating column group
    pub fn add_max(&mut self, columns: impl Iterator<Item = RawColumnSchema>) {
        self.aggregations.insert(AggregatingSchema::Max {
            columns: columns.enumerate().map(|(o, c)| (o as u64, c)).collect(),
            id: AggregationId(rand::random()),
        });
    }

    /// Add min aggregating column group
    pub fn add_min(&mut self, columns: impl Iterator<Item = RawColumnSchema>) {
        self.aggregations.insert(AggregatingSchema::Min {
            columns: columns.enumerate().map(|(o, c)| (o as u64, c)).collect(),
            id: AggregationId(rand::random()),
        });
    }

    /// Add summing columns
    pub fn add_sum(&mut self, columns: impl Iterator<Item = RawColumnSchema>) {
        for c in columns {
            self.aggregations
                .insert(AggregatingSchema::Sum([(0, c)].into_iter().collect()));
        }
    }

    /// All the columns
    pub(crate) fn columns(&self) -> impl Iterator<Item = &(u64, RawColumnSchema)> {
        self.primary
            .iter()
            .chain(self.aggregations.iter().flat_map(|a| a.columns()))
    }

    /// The number of columns
    pub fn num_columns(&self) -> usize {
        self.primary.len() + self.aggregations.len()
    }

    fn to_table_rows(&self) -> Vec<TableSchemaRow> {
        let table = self.id;
        let mut out = Vec::new();
        for (order, c) in self.primary.iter() {
            out.push(TableSchemaRow {
                table,
                column: c.id,
                order: *order,
                aggregate: Aggregation::None,
                modified: std::time::SystemTime::now(),
                column_name: c.name.to_string(),
            })
        }
        out
    }

    fn to_db_row(&self) -> DbSchemaRow {
        DbSchemaRow {
            table: self.id,
            created: std::time::SystemTime::now(),
            modified: std::time::SystemTime::now(),
            table_name: self.name.to_string(),
            is_deleted: false,
        }
    }

    /// Create an empty builder for a table.
    pub fn build(self) -> TableBuilder {
        TableBuilder::new(Arc::new(self))
    }
}

impl std::fmt::Display for TableSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "CREATE TABLE {} ID {} {{", self.name, self.id)?;
        for (_, c) in self.columns() {
            writeln!(f, "    {c},")?;
        }
        column_list("PRIMARY KEY", &self.primary, f)?;
        for a in self.aggregations.iter() {
            match a {
                AggregatingSchema::Max { columns, .. } => column_list("MAX", columns, f)?,
                AggregatingSchema::Min { columns, .. } => column_list("MIN", columns, f)?,
                AggregatingSchema::Sum(columns) => column_list("SUM", columns, f)?,
            }
        }
        writeln!(f, "}};")
    }
}
fn column_list(
    keyword: &str,
    v: &OrderedRawColumns,
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    let mut columns = v.iter().map(|x| &x.1);
    if let Some(c) = columns.next() {
        write!(f, "    {keyword} ( {}", c.display_name())?;
        for c in columns {
            write!(f, ", {}", c.display_name())?;
        }
        writeln!(f, " ),")
    } else {
        Ok(())
    }
}

impl<T: Lens + Default + Clone> ColumnSchema<T> {
    /// Create a new column with default given by [`Default`].
    pub fn new(name: &'static str) -> ColumnSchema<T> {
        ColumnSchema {
            default: T::default(),
            name,
            id: ColumnId::new(),
        }
    }
}
impl<T: Lens + Clone> ColumnSchema<T> {
    /// Create a new column with the specified default
    pub fn with_default(name: &'static str, default: T) -> ColumnSchema<T> {
        ColumnSchema {
            default,
            name,
            id: ColumnId::new(),
        }
    }

    fn with_id(self, id: ColumnId) -> Self {
        ColumnSchema { id, ..self }
    }

    /// Iterate over the raw columns corresponding to this one.
    pub fn raw(&self) -> impl Iterator<Item = RawColumnSchema> {
        let vs: RawValues = self.default.clone().into();
        let id = self.id;
        let name = self.name;
        vs.0.into_iter()
            .enumerate()
            .map(move |(idx, default)| RawColumnSchema {
                name,
                default,
                id,
                fieldname: T::NAMES[idx],
                lens: T::LENS_ID,
            })
    }
}

pub(crate) struct TableSchemaRow {
    table: TableId,
    column: ColumnId,
    order: u64,
    aggregate: Aggregation,
    modified: std::time::SystemTime,
    column_name: String,
}

impl IsRow for TableSchemaRow {
    const TABLE_ID: TableId = TableId::const_new(b"__table_schemas_");
    fn to_raw(self) -> Vec<RawValue> {
        let mut out = Vec::with_capacity(7);
        out.extend(RawValues::from(self.table).0);
        out.extend(RawValues::from(self.column).0);
        out.extend(RawValues::from(self.order).0);
        out.extend(RawValues::from(self.aggregate).0);
        out.extend(RawValues::from(self.modified).0);
        out.extend(RawValues::from(self.column_name).0);
        assert_eq!(out.len(), 7);
        out
    }
    fn from_raw(values: Vec<RawValue>) -> Result<Self, LensError> {
        let mut values = values.into_iter();
        let table = RawValues(vec![values.next().unwrap()]).try_into()?;
        let column = RawValues(vec![values.next().unwrap(), values.next().unwrap()]).try_into()?;
        let order = RawValues(vec![values.next().unwrap(), values.next().unwrap()]).try_into()?;
        let aggregate =
            RawValues(vec![values.next().unwrap(), values.next().unwrap()]).try_into()?;
        let modified =
            RawValues(vec![values.next().unwrap(), values.next().unwrap()]).try_into()?;
        let column_name = RawValues(vec![values.next().unwrap()]).try_into()?;
        Ok(TableSchemaRow {
            table,
            column,
            order,
            aggregate,
            modified,
            column_name,
        })
    }
}

/// This is he schema for the table that holds schemas of tables
pub fn table_schema_schema() -> TableSchema {
    let mut table = TableSchema::new("columns");
    table.id = TableSchemaRow::TABLE_ID;
    table.add_primary(
        ColumnSchema::with_default("table", TableId::const_new(b"TABLE--NOT-EXIST"))
            .with_id(ColumnId::const_new(b"table_id--tables"))
            .raw(),
    );
    table.add_primary(
        ColumnSchema::with_default("column", ColumnId::const_new(b"COLUMN-NOT-EXIST"))
            .with_id(ColumnId::const_new(b"column_id-tables"))
            .raw(),
    );
    table.add_primary(
        ColumnSchema::with_default("order", 0u64)
            .with_id(ColumnId::const_new(b"column-sortorder"))
            .raw(),
    );
    table.add_primary(
        ColumnSchema::with_default("aggregate", Aggregation::None)
            .with_id(ColumnId::const_new(b"column-aggregate"))
            .raw(),
    );
    table.add_max(
        ColumnSchema::with_default("modified", std::time::SystemTime::UNIX_EPOCH)
            .with_id(ColumnId::const_new(b"modified-column!"))
            .raw()
            .chain(
                ColumnSchema::with_default("column_name", String::default())
                    .with_id(ColumnId::const_new(b"name-of-column!!"))
                    .raw(),
            ),
    );
    table
}

pub(crate) struct DbSchemaRow {
    table: TableId,
    created: std::time::SystemTime,
    modified: std::time::SystemTime,
    table_name: String,
    is_deleted: bool,
}

impl IsRow for DbSchemaRow {
    const TABLE_ID: TableId = TableId::const_new(b"__db_schema_____");
    fn to_raw(self) -> Vec<RawValue> {
        let mut out = Vec::with_capacity(7);
        out.extend(RawValues::from(self.table).0);
        out.extend(RawValues::from(self.created).0);
        out.extend(RawValues::from(self.modified).0);
        out.extend(RawValues::from(self.table_name).0);
        out.extend(RawValues::from(self.is_deleted).0);
        assert_eq!(out.len(), 7);
        out
    }
    fn from_raw(values: Vec<RawValue>) -> Result<Self, LensError> {
        let mut values = values.into_iter();
        let table = RawValues(vec![values.next().unwrap()]).try_into()?;
        let created = RawValues(vec![values.next().unwrap(), values.next().unwrap()]).try_into()?;
        let modified =
            RawValues(vec![values.next().unwrap(), values.next().unwrap()]).try_into()?;
        let table_name = RawValues(vec![values.next().unwrap()]).try_into()?;
        let is_deleted = RawValues(vec![values.next().unwrap()]).try_into()?;
        Ok(DbSchemaRow {
            table,
            created,
            modified,
            table_name,
            is_deleted,
        })
    }
}

pub fn save_db_schema(
    tables: Vec<TableSchema>,
    directory: impl AsRef<Path>,
) -> Result<(), StorageError> {
    let mut table_table = TableBuilder::new(Arc::new(table_schema_schema()));
    let mut db_table = TableBuilder::new(Arc::new(db_schema_schema()));
    for t in tables {
        for row in t.to_table_rows() {
            table_table.insert_row(row).unwrap();
        }
        db_table.insert_row(t.to_db_row()).unwrap();
    }
    table_table.save(directory.as_ref())?;
    db_table.save(directory)
}
pub fn load_db_schema(directory: impl AsRef<Path>) -> Result<Vec<TableSchema>, StorageError> {
    let mut out = Vec::new();
    let db_schema = Arc::new(db_schema_schema());
    let db_table = Table::read(directory.as_ref(), db_schema)?;
    Ok(out)
}

/// This is the schema for the table that holds the schema of the db itself
///
/// In other words, this table holds the set of tables.
pub fn db_schema_schema() -> TableSchema {
    let mut table = TableSchema::new("tables");
    table.id = DbSchemaRow::TABLE_ID;
    table.add_primary(
        ColumnSchema::with_default("table", TableId::const_new(b"TABLE--NOT-EXIST"))
            .with_id(ColumnId::const_new(b"table_id--tables"))
            .raw(),
    );
    table.add_primary(
        ColumnSchema::with_default("created", std::time::SystemTime::UNIX_EPOCH)
            .with_id(ColumnId::const_new(b"__table_created!"))
            .raw(),
    );
    table.add_max(
        ColumnSchema::with_default("modified", std::time::SystemTime::UNIX_EPOCH)
            .with_id(ColumnId::const_new(b"modified-table!!"))
            .raw()
            .chain(
                ColumnSchema::with_default("table_name", String::default())
                    .with_id(ColumnId::const_new(b"name-of-table!!!"))
                    .raw(),
            )
            .chain(
                ColumnSchema::with_default("is_deleted", false)
                    .with_id(ColumnId::const_new(b"deleted-table!!!"))
                    .raw(),
            ),
    );
    table
}

#[test]
fn format_db_tables() {
    let expected = expect_test::expect![[r#"
        CREATE TABLE columns ID __table_schemas {
            table Bytes DEFAULT 'TABLE--NOT-EXIST' LENS __TableId,
            column Bytes DEFAULT 'COLUMN-NOT-EXIST' LENS __ColumnId,
            order U64 DEFAULT 0 LENS u64,
            aggregate U64 DEFAULT 0 LENS __Aggregation,
            modified.seconds U64 DEFAULT 0 LENS time::SystemTime,
            modified.subsecond_nanos U64 DEFAULT 0 LENS time::SystemTime,
            column_name Bytes DEFAULT '' LENS String,
            PRIMARY KEY ( table, column, order, aggregate ),
            MAX ( modified.seconds, modified.subsecond_nanos, column_name ),
        };
    "#]];
    expected.assert_eq(table_schema_schema().to_string().as_str());

    let expected = expect_test::expect![[r#"
        CREATE TABLE tables ID __db_schema {
            table Bytes DEFAULT 'TABLE--NOT-EXIST' LENS __TableId,
            created.seconds U64 DEFAULT 0 LENS time::SystemTime,
            created.subsecond_nanos U64 DEFAULT 0 LENS time::SystemTime,
            modified.seconds U64 DEFAULT 0 LENS time::SystemTime,
            modified.subsecond_nanos U64 DEFAULT 0 LENS time::SystemTime,
            table_name Bytes DEFAULT '' LENS String,
            is_deleted Bool DEFAULT false LENS bool,
            PRIMARY KEY ( table, created.seconds, created.subsecond_nanos ),
            MAX ( modified.seconds, modified.subsecond_nanos, table_name, is_deleted ),
        };
    "#]];
    expected.assert_eq(db_schema_schema().to_string().as_str());
}
