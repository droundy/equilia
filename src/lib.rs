#![deny(missing_docs)]
//! A nice columnar data store.

use thiserror::Error;

pub mod column;
mod lens;
mod parser;
mod schema;
mod table;
mod value;

use column::encoding::StorageError;
pub use column::RawColumn;
pub use lens::{Context, Lens, LensError};
pub use schema::{
    columns_schema, load_db_schema, save_db_schema, tables_schema, ColumnSchema, RawColumnSchema,
    TableSchema,
};
pub use table::{Table, TableBuilder};
use value::RawValue;

/// An error of any sort
#[derive(Debug, Error)]
pub enum Error {
    /// An IO error
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    /// Lens trouble
    #[error("Type error: {0}")]
    Lens(#[from] LensError),
}

impl Context for Error {
    fn context<S: ToString>(self, context: S) -> Self {
        match self {
            Error::Lens(e) => Error::Lens(e.context(context)),
            Error::Storage(e) => Error::Storage(e.context(context)),
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
