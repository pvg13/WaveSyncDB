use std::marker::PhantomData;

use sqlparser::dialect::Dialect;

pub(crate) enum DialectType {
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

    pub(crate) fn build(&self) -> Box<dyn Dialect> {
        match self {
            DialectType::PostgreSQL => Box::new(sqlparser::dialect::PostgreSqlDialect {}),
            DialectType::MySQL => Box::new(sqlparser::dialect::MySqlDialect {}),
            DialectType::SQLite => Box::new(sqlparser::dialect::SQLiteDialect {}),
            DialectType::Unknown => Box::new(sqlparser::dialect::GenericDialect {}),
        }
    }
}