use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::column::encoding::StorageError;
use crate::lens::{ColumnId, Lens, LensId, RawValues, TableId};
use crate::table::IsRow;
use crate::value::{RawKind, RawValue};
use crate::{Context, Error, LensError, RawColumn, Table, TableBuilder};

/// A kind of column to aggregate
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u64)]
pub enum Aggregation {
    Min([u8; 15]),
    Max([u8; 15]),
    Sum,
}
impl Lens for Option<Aggregation> {
    const RAW_KINDS: &'static [crate::value::RawKind] = LensId::RAW_KINDS;
    const EXPECTED: &'static str = "Bytes indicating which aggregation and with what id";
    const LENS_ID: LensId = LensId(*b"__Aggregation___");
    const NAMES: &'static [&'static str] = &[""];
}
impl From<Option<Aggregation>> for RawValues {
    fn from(a: Option<Aggregation>) -> Self {
        let bytes = match a {
            None => vec![0; 16],
            Some(Aggregation::Min(bytes)) => {
                let mut b = Vec::with_capacity(16);
                b.push(1);
                b.extend(bytes);
                b
            }
            Some(Aggregation::Max(bytes)) => {
                let mut b = Vec::with_capacity(16);
                b.push(2);
                b.extend(bytes);
                b
            }
            Some(Aggregation::Sum) => vec![3; 16],
        };
        RawValues(vec![RawValue::Bytes(bytes)])
    }
}
impl TryFrom<RawValues> for Option<Aggregation> {
    type Error = LensError;
    fn try_from(value: RawValues) -> Result<Self, Self::Error> {
        match LensId::try_from(value)?.0 {
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] => Ok(None),
            [1, id @ ..] => Ok(Some(Aggregation::Min(id))),
            [2, id @ ..] => Ok(Some(Aggregation::Max(id))),
            [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3] => Ok(Some(Aggregation::Sum)),
            v => Err(LensError::InvalidValue {
                value: format!("Unexpected: {v:?}"),
                context: Vec::new(),
            }),
        }
    }
}

/// A schema for a column
pub struct ColumnSchema<T> {
    default: T,
    name: String,
    id: ColumnId,
}

/// The schema of a raw column, including the `LensId` metadata
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RawColumnSchema {
    pub(crate) order: u64,
    id: ColumnId,
    default: RawValue,
    name: String,
    lens: LensId,
}

/// A row of the table schema
///
/// This stores both the RawColumnSchema information (which describes the column
/// itself and how to read it) and where it fits into the TableSchema.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TableSchemaRow {
    /// The id of the table this belongs to
    table: TableId,
    /// The id of this column
    column: ColumnId,
    /// The order in which this column appears, either in the primary key, or a max/min aggregation.
    order: u64,
    /// Is the key primary, or an aggregation, and which kind if it is an aggregation?
    aggregate: Option<Aggregation>,
    /// The default value of the column
    default: RawValue,
    /// When was this column's name modified?
    modified: std::time::SystemTime,
    /// The user-visible name of the column
    column_name: String,
    /// The id of the lens for viewing the column
    lens: LensId,
}

impl RawColumnSchema {
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
            self.name,
            self.default.kind(),
            self.default,
            self.lens,
        )
    }
}

/// The schema of a table
#[derive(Clone)]
pub struct TableSchema {
    name: String,
    pub(crate) id: TableId,
    primary: BTreeSet<RawColumnSchema>, // must all have Aggregation = None
    aggregations: BTreeMap<Aggregation, BTreeSet<RawColumnSchema>>,
}

impl TableSchema {
    /// Create a new empty table
    pub fn new(name: &'static str) -> Self {
        TableSchema {
            name: name.to_string(),
            id: TableId::new(),
            primary: BTreeSet::new(),
            aggregations: BTreeMap::new(),
        }
    }

    /// Add columns to the primary key
    pub fn add_primary(&mut self, columns: impl Iterator<Item = RawColumnSchema>) {
        let first_order = if let Some(o) = self.primary.iter().next_back() {
            o.order + 1
        } else {
            0
        };
        for (o, mut c) in columns.enumerate() {
            if c.order == 0 {
                c.order = first_order + o as u64;
            }
            self.primary.insert(c);
        }
    }

    /// Add max aggregating column group
    pub fn add_max(&mut self, columns: impl Iterator<Item = RawColumnSchema>) {
        self.aggregations.insert(
            Aggregation::Max(rand::random()),
            columns
                .enumerate()
                .map(|(o, mut c)| {
                    if c.order == 0 {
                        c.order = o as u64;
                    }
                    c
                })
                .collect(),
        );
    }

    /// Add min aggregating column group
    pub fn add_min(&mut self, columns: impl Iterator<Item = RawColumnSchema>) {
        self.aggregations.insert(
            Aggregation::Min(rand::random()),
            columns
                .enumerate()
                .map(|(o, mut c)| {
                    if c.order == 0 {
                        c.order = o as u64;
                    }
                    c
                })
                .collect(),
        );
    }

    /// Add summing columns
    pub fn add_sum(&mut self, columns: impl Iterator<Item = RawColumnSchema>) {
        self.aggregations.insert(
            Aggregation::Sum,
            columns
                .enumerate()
                .map(|(o, mut c)| {
                    if c.order == 0 {
                        c.order = o as u64;
                    }
                    c
                })
                .collect(),
        );
    }

    /// All the columns
    pub(crate) fn columns(&self) -> impl Iterator<Item = &RawColumnSchema> {
        self.primary
            .iter()
            .chain(self.aggregations.iter().flat_map(|a| a.1.iter()))
    }

    /// The number of columns
    pub fn num_columns(&self) -> usize {
        self.primary.len()
            + self
                .aggregations
                .iter()
                .map(|(_, c)| c.len())
                .sum::<usize>()
    }

    fn to_table_rows(&self) -> Vec<TableSchemaRow> {
        let table = self.id;
        let mut out = Vec::new();
        for c in self.primary.iter() {
            out.push(TableSchemaRow {
                table,
                column: c.id,
                lens: c.lens,
                order: c.order,
                aggregate: None,
                modified: std::time::SystemTime::now(),
                column_name: c.name.to_string(),
                default: c.default.clone(),
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
        for c in self.columns() {
            writeln!(f, "    {c},")?;
        }
        column_list("PRIMARY KEY", &self.primary, f)?;
        for (a, columns) in self.aggregations.iter() {
            match a {
                Aggregation::Max(_) => column_list("MAX", columns, f)?,
                Aggregation::Min(_) => column_list("MIN", columns, f)?,
                Aggregation::Sum => column_list("SUM", columns, f)?,
            }
        }
        writeln!(f, "}};")
    }
}
fn column_list(
    keyword: &str,
    v: &BTreeSet<RawColumnSchema>,
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    let mut columns = v.iter();
    if let Some(c) = columns.next() {
        write!(f, "    {keyword} ( {}", c.name)?;
        for c in columns {
            write!(f, ", {}", c.name)?;
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
            name: name.to_string(),
            id: ColumnId::new(),
        }
    }
}
impl<T: Lens + Clone> ColumnSchema<T> {
    /// Create a new column with the specified default
    pub fn with_default(name: &'static str, default: T) -> ColumnSchema<T> {
        ColumnSchema {
            default,
            name: name.to_string(),
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
        let name = self.name.clone();
        vs.0.into_iter()
            .enumerate()
            .map(move |(idx, default)| RawColumnSchema {
                order: 0,
                name: format!("{}.{}", name, T::NAMES[idx]),
                default,
                id,
                lens: T::LENS_ID,
            })
    }
}

impl IsRow for TableSchemaRow {
    const TABLE_ID: TableId = TableId::const_new(b"__table_schemas_");
    fn to_raw(self) -> Vec<RawValue> {
        let mut out = Vec::with_capacity(9);
        out.extend(RawValues::from(self.table).0);
        println!("table is {:?}", RawValues::from(self.table).0);
        println!("column is {:?}", RawValues::from(self.column).0);
        out.extend(RawValues::from(self.column).0);
        out.extend(RawValues::from(self.order).0);
        out.extend(RawValues::from(self.lens).0);
        out.extend(RawValues::from(self.default).0);
        out.extend(RawValues::from(self.aggregate).0);
        out.extend(RawValues::from(self.modified).0);
        out.extend(RawValues::from(self.column_name).0);
        assert_eq!(out.len(), 9);
        out
    }
    fn from_raw(columns: Vec<RawColumn>) -> Result<Vec<Self>, Error> {
        let mut columns = columns.into_iter();
        let table = columns.next().unwrap().read_values().context("table id")?;
        let length = table.len();
        let mut table = table.into_iter();
        let mut column = columns
            .next()
            .unwrap()
            .read_values()
            .context("column id")?
            .into_iter();
        let mut order = columns
            .next()
            .unwrap()
            .read_u64()
            .context("order")?
            .into_iter();
        let mut default = columns
            .next()
            .unwrap()
            .read_values()
            .context("default")?
            .into_iter();
        let mut aggregate = columns
            .next()
            .unwrap()
            .read_values()
            .context("aggregation")?
            .into_iter();
        let mut modified_1 = columns
            .next()
            .unwrap()
            .read_values()
            .context("modified seconds")?
            .into_iter();
        let mut modified_2 = columns
            .next()
            .unwrap()
            .read_values()
            .context("modified subsec")?
            .into_iter();
        let mut column_name = columns
            .next()
            .unwrap()
            .read_values()
            .context("column name")?
            .into_iter();
        let mut lens = columns
            .next()
            .unwrap()
            .read_values()
            .context("lens id")?
            .into_iter();
        let mut out = Vec::with_capacity(length);
        for _ in 0..length {
            out.push(TableSchemaRow {
                table: RawValues(vec![table.next().unwrap()])
                    .try_into()
                    .context("converting table id")?,
                column: RawValues(vec![column.next().unwrap()])
                    .try_into()
                    .context("converting column id")?,
                order: order.next().unwrap(),
                lens: RawValues(vec![lens.next().unwrap()])
                    .try_into()
                    .context("converting lens id")?,
                aggregate: RawValues(vec![aggregate.next().unwrap()]).try_into()?,
                modified: RawValues(vec![modified_1.next().unwrap(), modified_2.next().unwrap()])
                    .try_into()?,
                column_name: RawValues(vec![column_name.next().unwrap()]).try_into()?,
                default: RawValues(vec![default.next().unwrap()]).try_into()?,
            });
        }
        Ok(out)
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
        ColumnSchema::with_default("lens", bool::LENS_ID)
            .with_id(ColumnId::const_new(b"column-lens_____"))
            .raw(),
    );
    table.add_primary(
        ColumnSchema::with_default("default", RawValue::Bool(false))
            .with_id(ColumnId::const_new(b"column-default__"))
            .raw(),
    );
    table.add_primary(
        ColumnSchema::with_default("aggregate", None)
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
    fn from_raw(columns: Vec<RawColumn>) -> Result<Vec<Self>, Error> {
        let mut columns = columns.into_iter();
        let table = columns.next().unwrap().read_values()?;
        let length = table.len();
        let mut table = table.into_iter();
        let mut created_1 = columns.next().unwrap().read_values()?.into_iter();
        let mut created_2 = columns.next().unwrap().read_values()?.into_iter();
        let mut modified_1 = columns.next().unwrap().read_values()?.into_iter();
        let mut modified_2 = columns.next().unwrap().read_values()?.into_iter();
        let mut table_name = columns.next().unwrap().read_values()?.into_iter();
        let mut is_deleted = columns.next().unwrap().read_bools()?.into_iter();
        let mut out = Vec::with_capacity(table.len());
        for _ in 0..length {
            out.push(DbSchemaRow {
                table: RawValues(vec![table.next().unwrap()]).try_into()?,
                created: RawValues(vec![created_1.next().unwrap(), created_2.next().unwrap()])
                    .try_into()?,
                modified: RawValues(vec![modified_1.next().unwrap(), modified_2.next().unwrap()])
                    .try_into()?,
                table_name: RawValues(vec![table_name.next().unwrap()]).try_into()?,
                is_deleted: is_deleted.next().unwrap(),
            });
        }
        Ok(out)

        // let mut values = values.into_iter();
        // let table = RawValues(vec![values.next().unwrap()]).try_into()?;
        // let created = RawValues(vec![values.next().unwrap(), values.next().unwrap()]).try_into()?;
        // let modified =
        //     RawValues(vec![values.next().unwrap(), values.next().unwrap()]).try_into()?;
        // let table_name = RawValues(vec![values.next().unwrap()]).try_into()?;
        // let is_deleted = RawValues(vec![values.next().unwrap()]).try_into()?;
        // Ok(DbSchemaRow {
        //     table,
        //     created,
        //     modified,
        //     table_name,
        //     is_deleted,
        // })
    }
}

/// Saves the database schema to the requested directory.
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

/// Reads the dtatabase schema from the requested directory
pub fn load_db_schema(directory: impl AsRef<Path>) -> Result<Vec<TableSchema>, Error> {
    let mut out = Vec::new();
    let db_schema = Arc::new(db_schema_schema());
    let db_table = Table::read(directory.as_ref(), db_schema).context("read tables")?;
    let table_schema = Arc::new(table_schema_schema());
    let table_table = Table::read(directory, table_schema).context("read columns")?;
    println!("I have read the table table");
    let mut table_rows: Vec<TableSchemaRow> = table_table.to_rows().context("columns to rows")?;
    table_rows.sort();
    let mut table_columns: HashMap<TableId, Vec<TableSchemaRow>> = HashMap::new();
    for tr in table_rows.into_iter() {
        table_columns.entry(tr.table).or_default().push(tr);
    }
    for db_row in db_table
        .to_rows::<DbSchemaRow>()
        .context("tables to rows")?
        .into_iter()
    {
        let name = db_row.table_name;
        let id = db_row.table;
        let mut primary = BTreeSet::new();
        let mut aggregations: BTreeMap<Aggregation, BTreeSet<RawColumnSchema>> = BTreeMap::new();
        for tr in table_columns.remove(&id).unwrap_or_default().into_iter() {
            let c = RawColumnSchema {
                order: tr.order,
                name: tr.column_name,
                id: tr.column,
                default: tr.default,
                lens: tr.lens,
            };
            match tr.aggregate {
                None => {
                    primary.insert(c);
                }
                Some(agg) => {
                    aggregations.entry(agg).or_default().insert(c);
                }
            }
        }

        out.push(TableSchema {
            name,
            id,
            primary,
            aggregations,
        })
    }
    Ok(out)
}

#[test]
fn save_and_load_schema() {
    let dir = tempfile::tempdir().unwrap();
    let table_schema = table_schema_schema();
    let db_schema = db_schema_schema();
    println!("\nsaving schema\n");
    save_db_schema(vec![table_schema.clone(), db_schema.clone()], dir.as_ref()).unwrap();
    println!("\nloading schema\n");
    let schemas = load_db_schema(dir).unwrap();
    println!("\nI have loaded the shcemas!\n");
    assert!(schemas.iter().any(|schema| schema.id == table_schema.id));
    assert!(schemas.iter().any(|schema| schema.id == db_schema.id));
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
            table. Bytes DEFAULT 'TABLE--NOT-EXIST' LENS __TableId,
            column. Bytes DEFAULT 'COLUMN-NOT-EXIST' LENS __ColumnId,
            order. U64 DEFAULT 0 LENS u64,
            lens. Bytes DEFAULT 'bool____________' LENS __LensId,
            default. U64 DEFAULT 0 LENS u64,
            aggregate. Bytes DEFAULT '                ' LENS __Aggregation,
            modified.seconds U64 DEFAULT 0 LENS time::SystemTime,
            modified.subsecond_nanos U64 DEFAULT 0 LENS time::SystemTime,
            column_name. Bytes DEFAULT '' LENS String,
            PRIMARY KEY ( table., column., order., lens., default., aggregate. ),
            MAX ( modified.seconds, modified.subsecond_nanos, column_name. ),
        };
    "#]];
    expected.assert_eq(table_schema_schema().to_string().as_str());

    let expected = expect_test::expect![[r#"
        CREATE TABLE tables ID __db_schema {
            table. Bytes DEFAULT 'TABLE--NOT-EXIST' LENS __TableId,
            created.seconds U64 DEFAULT 0 LENS time::SystemTime,
            created.subsecond_nanos U64 DEFAULT 0 LENS time::SystemTime,
            modified.seconds U64 DEFAULT 0 LENS time::SystemTime,
            modified.subsecond_nanos U64 DEFAULT 0 LENS time::SystemTime,
            table_name. Bytes DEFAULT '' LENS String,
            is_deleted. Bool DEFAULT false LENS bool,
            PRIMARY KEY ( table., created.seconds, created.subsecond_nanos ),
            MAX ( modified.seconds, modified.subsecond_nanos, table_name., is_deleted. ),
        };
    "#]];
    expected.assert_eq(db_schema_schema().to_string().as_str());
}
