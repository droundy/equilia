use std::path::Path;
use std::sync::Arc;

use crate::column::encoding::StorageError;
use crate::column::RawColumn;
use crate::schema::TableSchema;
use crate::value::{RawKind, RawValue};
use thiserror::Error;

/// An invalid column error
#[derive(Debug, Error)]
pub enum InvalidColumn {
    #[error("Wrong kind: column {column} wanted {wanted} not {found}")]
    WrongKind {
        column: u64,
        found: RawKind,
        wanted: RawKind,
    },
    #[error("Wrong number of raw columns: {found} should be {wanted}")]
    WrongNumber { found: usize, wanted: usize },
}

/// A table with values in it
pub struct Table {
    schema: Arc<TableSchema>,
    columns: Vec<RawColumn>,
}

// impl Table {
//     /// Save to disk
//     pub fn save(self, directory: impl AsRef<Path>) -> Result<(), StorageError> {
//         let directory: &Path = directory.as_ref();
//         std::fs::create_dir_all(directory)?;
//         for (schema, column) in self.schema.columns().map(|(_, c)| c).zip(self.columns) {
//             let filename = directory.join(schema.file_name());
//             std::fs::write(filename, contents);
//         }
//         Ok(())
//     }
// }

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
    pub fn insert_row(&mut self, mut row: Vec<RawValue>) -> Result<(), InvalidColumn> {
        if row.len() != self.schema.num_columns() {
            return Err(InvalidColumn::WrongNumber {
                found: row.len(),
                wanted: self.schema.num_columns(),
            });
        }
        row.reverse();
        for ((column, c), v) in self.schema.columns().zip(row.iter()) {
            if c.kind() != v.kind() {
                return Err(InvalidColumn::WrongKind {
                    column: *column,
                    found: v.kind(),
                    wanted: c.kind(),
                });
            }
        }
        self.rows.push(row);
        Ok(())
    }

    /// Create the table
    pub fn table(mut self) -> Table {
        self.rows.sort_unstable();
        let mut columns = Vec::new();
        for (idx, c) in self.schema.columns().map(|(_, c)| c).enumerate() {
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

        Table {
            schema: self.schema,
            columns,
        }
    }

    /// Create the table on disk
    pub fn save(mut self, directory: impl AsRef<Path>) -> Result<(), StorageError> {
        let directory: &Path = directory.as_ref();
        std::fs::create_dir_all(directory)?;
        self.rows.sort_unstable();
        for (idx, schema) in self.schema.columns().map(|(_, c)| c).enumerate() {
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
