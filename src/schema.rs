use std::collections::BTreeSet;

use crate::lens::{ColumnId, Lens, LensId, RawValues, TableId};
use crate::value::RawValue;
use crate::LensError;

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
    const LENS_ID: LensId = LensId(*b"AggregationKind.");
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
    suborder: u64,
    lens: LensId,
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
    id: TableId,
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
    fn columns(&self) -> impl Iterator<Item = &(u64, RawColumnSchema)> {
        self.primary
            .iter()
            .chain(self.aggregations.iter().flat_map(|a| a.columns()))
    }
}

impl std::fmt::Display for TableSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "CREATE TABLE {} ID {} {{", self.name, self.id)?;
        for (_, c) in self.columns() {
            writeln!(f, "    {c},")?;
        }
        // column_list("PRIMARY KEY", &self.primary, f)?;
        // for a in self.aggregation.iter() {
        //     match a {
        //         AggregatingSchema::Max(v) => column_list("MAX", v, f)?,
        //         AggregatingSchema::Min(v) => column_list("MIN", v, f)?,
        //         AggregatingSchema::Sum(c) => column_list("SUM", c, f)?,
        //     }
        // }
        // writeln!(f, "}};")
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
            .map(move |(suborder, default)| RawColumnSchema {
                name,
                default,
                id,
                suborder: suborder as u64,
                lens: T::LENS_ID,
            })
    }
}

/// This is he schema for the table that holds schemas of tables
pub fn table_schema_schema() -> TableSchema {
    let mut table = TableSchema::new("tables");
    table.id = TableId::const_new(b"__table_schemas_");
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
    table
}

#[test]
fn format_db_tables() {
    let expected = expect_test::expect![[r#"
        CREATE TABLE tables ID '__table_schemas_' {
            table FixedBytes(16) DEFAULT 'TABLE--NOT-EXIST' LENS '__table_id______',
            column FixedBytes(16) DEFAULT 'COLUMN-NOT-EXIST' LENS '__column_id_____',
            order U64 DEFAULT 0 LENS 'just a u64 only!',
            aggregate U64 DEFAULT 0 LENS 'AggregationKind.',
    "#]];
    expected.assert_eq(table_schema_schema().to_string().as_str());

    // let expected = expect_test::expect![[r#"
    //     CREATE TABLE Table('__tables_in_db__') {
    //         `table_id-in-db!!` U64 DEFAULT 0,
    //         `MODIFIED-TABLE..` U64 DEFAULT 0,
    //         `name-of-table...` Bytes DEFAULT '',
    //         `table-is-deleted` Bool DEFAULT false,
    //         `table-wascreated` U64 DEFAULT 0,
    //         PRIMARY KEY ( `table_id-in-db!!` ),
    //         MAX ( `MODIFIED-TABLE..`, `name-of-table...`, `table-is-deleted` ),
    //         MIN ( `table-wascreated` ),
    //     };
    // "#]];
    // expected.assert_eq(db_schema_schema().to_string().as_str());
}
