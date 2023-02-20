use std::path::Path;
use std::sync::Arc;

use crate::column::encoding::StorageError;
use crate::column::RawColumn;
use crate::lens::TableId;
use crate::schema::TableSchema;
use crate::value::{RawKind, RawValue};
use crate::Error;

/// An invalid column error
#[derive(Debug, thiserror::Error)]
pub enum InvalidColumn {
    #[error("Wrong kind: column {column} wanted {wanted} not {found}")]
    WrongKind {
        table: String,
        column: String,
        column_number: u64,
        found: RawKind,
        wanted: RawKind,
    },
    #[error("Wrong number of raw columns: {found} should be {wanted}")]
    WrongNumber { found: usize, wanted: usize },
}

/// A table with values in it
pub struct Table {
    columns: Vec<RawColumn>,
}

impl Table {
    /// Read from disk
    pub fn read(
        directory: impl AsRef<Path>,
        schema: Arc<TableSchema>,
    ) -> Result<Self, StorageError> {
        let directory: &Path = directory.as_ref();
        let mut columns = Vec::new();
        for schema in schema.columns() {
            let path = directory.join(schema.file_name());
            println!("reading file {path:?} for {schema}");
            columns.push(RawColumn::open(path)?);
        }
        println!("Finished reading columns for table {schema}");
        Ok(Table { columns })
    }

    /// Extract rows
    pub fn to_rows<R: IsRow>(&self) -> Result<Vec<R>, Error> {
        R::from_raw(self.columns.clone())
    }
}

/// A type that could represent a row of a table
pub trait IsRow: Sized {
    const TABLE_ID: TableId;
    fn to_raw(self) -> Vec<RawValue>;
    fn from_raw(values: Vec<RawColumn>) -> Result<Vec<Self>, Error>;
}

/// A not-yet-sorted table
pub struct TableBuilder {
    schema: Arc<TableSchema>,
    rows: Vec<Vec<RawValue>>,
}

impl TableBuilder {
    /// Create an empty builder for a table.
    pub fn new(schema: Arc<TableSchema>) -> Self {
        TableBuilder {
            schema,
            rows: Vec::new(),
        }
    }

    /// Add a row
    pub fn insert_raw_row(&mut self, row: Vec<RawValue>) -> Result<(), InvalidColumn> {
        if row.len() != self.schema.num_columns() {
            return Err(InvalidColumn::WrongNumber {
                found: row.len(),
                wanted: self.schema.num_columns(),
            });
        }
        // row.reverse();
        for (c, v) in self.schema.columns().zip(row.iter()) {
            println!(
                "{:2} column: {}:   wants {} got {}",
                c.order,
                c,
                c.kind(),
                v.kind()
            );
        }
        for (c, v) in self.schema.columns().zip(row.iter()) {
            if c.kind() != v.kind() {
                return Err(InvalidColumn::WrongKind {
                    table: format!("{}", self.schema),
                    column: format!("{}", c),
                    column_number: c.order,
                    found: v.kind(),
                    wanted: c.kind(),
                });
            }
        }
        self.rows.push(row);
        Ok(())
    }

    /// Insert a row that is specific to this table schema.
    pub fn insert_row<R: IsRow>(&mut self, row: R) -> Result<(), InvalidColumn> {
        assert_eq!(R::TABLE_ID, self.schema.id);
        self.insert_raw_row(row.to_raw())
    }

    /// Create the table
    pub fn table(mut self) -> Table {
        self.rows.sort_unstable();
        let mut columns = Vec::new();
        for (idx, c) in self.schema.columns().enumerate() {
            match c.kind() {
                RawKind::Bool => {
                    let mut vals = Vec::new();
                    for r in self.rows.iter() {
                        vals.push(r[idx].assert_bool())
                    }
                    columns.push(RawColumn::from(vals.as_slice()));
                }
                RawKind::U64 => {
                    let mut vals = Vec::new();
                    for r in self.rows.iter() {
                        vals.push(r[idx].assert_u64())
                    }
                    columns.push(RawColumn::from(vals.as_slice()));
                }
                RawKind::Bytes => {
                    let mut vals = Vec::new();
                    for r in self.rows.iter() {
                        vals.push(r[idx].assert_bytes())
                    }
                    columns.push(RawColumn::from(vals.as_slice()));
                }
            }
        }

        Table { columns }
    }

    /// Create the table on disk
    pub fn save(mut self, directory: impl AsRef<Path>) -> Result<(), StorageError> {
        let directory: &Path = directory.as_ref();
        std::fs::create_dir_all(directory)?;
        self.rows.sort_unstable();
        for (idx, schema) in self.schema.columns().enumerate() {
            let filename = directory.join(schema.file_name());
            let mut f = std::fs::File::create(filename)?;
            match schema.kind() {
                RawKind::Bool => {
                    let mut vals = Vec::new();
                    for r in self.rows.iter() {
                        vals.push(r[idx].assert_bool())
                    }
                    RawColumn::write_bools(&mut f, vals.as_slice())?;
                }
                RawKind::U64 => {
                    let mut vals = Vec::new();
                    for r in self.rows.iter() {
                        vals.push(r[idx].assert_u64())
                    }
                    RawColumn::write_u64(&mut f, vals.as_slice())?;
                }
                RawKind::Bytes => {
                    let mut vals = Vec::new();
                    for r in self.rows.iter() {
                        vals.push(r[idx].assert_bytes())
                    }
                    RawColumn::write_bytes(&mut f, vals.as_slice())?;
                }
            }
        }
        Ok(())
    }
}
