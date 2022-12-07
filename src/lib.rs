#![deny(missing_docs)]
//! A nice columnar data store.

use internment::Intern;

/// The type of data actually stored in a column.
///
/// This is in distinction from a logical [`Type`], which might
/// perform some transformation on the raw type, such as a
/// `DateTime` that might be stored as a `RawType::U64` of
/// seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RawType {
    /// A 64-bit integer
    U64,
    /// A boolean value
    Bool,
    /// A sequence of bytes
    Bytes,
}

/// The type of a column.
///
/// This is a logical type, which will be stored as one or
/// more [`RawType`] columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Type {
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
        ty: Intern<Type>,
        /// The number represented by this column
        units: u64,
        /// The units of the next larger column
        modulo: u64,
    },
}

/// A column schema
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Column {
    name: Intern<str>,
    ty: Type,
}

/// A table schema
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Table {
    name: Intern<str>,
    primary: Vec<Column>,
}

impl std::fmt::Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Table {}", self.name)?;
        for c in self.primary.iter() {
            writeln!(f, "{:20} {:?}", c.name, c.ty)?;
        }
        Ok(())
    }
}

#[test]
fn format_table() {
    let table = Table {
        name: Intern::from("my-table"),
        primary: vec![
            Column {
                name: Intern::from("date"),
                ty: Type::DateTime,
            },
            Column {
                name: Intern::from("date"),
                ty: Type::DateTime,
            },
        ],
    };
    let expected = expect_test::expect![[r#"
        Table my-table
        date                 DateTime
        date                 DateTime
    "#]];
    expected.assert_eq(table.to_string().as_str());
}
