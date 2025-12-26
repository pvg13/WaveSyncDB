
use diesel::{Connection, backend::Backend, r2d2::{ConnectionManager, PooledConnection, R2D2Connection}, sqlite::Sqlite};
use sqlparser::dialect::Dialect;

pub enum DialectType {
    PostgreSQL,
    MySQL,
    SQLite,
    Unknown,
}

impl DialectType {
    pub(crate) fn from_url(url: &str) -> Self {
        if url.contains("postgres") {
            DialectType::PostgreSQL
        } else if url.contains("mysql") {
            DialectType::MySQL
        } else if url.contains("sqlite") {
            DialectType::SQLite
        } else {
            DialectType::Unknown
        }
    }

    pub fn build(&self) -> Box<dyn Dialect> {
        match self {
            DialectType::PostgreSQL => Box::new(sqlparser::dialect::PostgreSqlDialect {}),
            DialectType::MySQL => Box::new(sqlparser::dialect::MySqlDialect {}),
            DialectType::SQLite => Box::new(sqlparser::dialect::SQLiteDialect {}),
            DialectType::Unknown => Box::new(sqlparser::dialect::GenericDialect {}),
        }
    }
}

// // 2. Define the bridge trait
// pub trait HasDialectType {
//     fn dialect_type() -> DialectType;
// }

// // 3. Conditional Implementations based on Features
// #[cfg(feature = "postgres")]
// impl HasDialectType for diesel::pg::Pg {
//     fn dialect_type() -> DialectType { DialectType::PostgreSQL }
// }

// #[cfg(feature = "mysql")]
// impl HasDialectType for diesel::mysql::Mysql {
//     fn dialect_type() -> DialectType { DialectType::MySQL }
// }

// #[cfg(feature = "sqlite")]
// impl HasDialectType for diesel::sqlite::Sqlite {
//     fn dialect_type() -> DialectType { DialectType::SQLite }
// }

// impl<T: HasDialectType + R2D2Connection + 'static> HasDialectType for PooledConnection<ConnectionManager<T>> {
//     fn dialect_type() -> DialectType { T::dialect_type() }
// }

// // 4. The generic conversion
// impl<T> From<&T> for DialectType 
// where 
//     T: Connection,
//     T::Backend: HasDialectType 
// {
//     fn from(_conn: &T) -> Self {
//         T::Backend::dialect_type()
//     }
// }