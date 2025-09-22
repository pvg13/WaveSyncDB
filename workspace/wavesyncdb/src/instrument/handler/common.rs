// Cargo.toml
// sqlparser = "0.47"   // or the latest compatible

use std::{collections::BTreeSet, marker::PhantomData};

use diesel::{backend::Backend, connection::DebugQuery, debug_query, query_builder::QueryFragment, AggregateExpressionMethods, IntoSql};
use sqlparser::{
    ast::*,
    dialect::{self, dialect_from_str, Dialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect},
    parser::Parser,
};

use crate::instrument::dialects::DialectType;

// Replaces bind parameters ('?')
// Example: 'INSERT INTO `tasks` (`title`, `description`, `completed`) VALUES (?, ?, ?) -- binds: ["Sample Task", "This is a sample task", false]' -> 'INSERT INTO `tasks` (`title`, `description`, `completed`) VALUES ('Sample Task', 'This is a sample task', false)'
fn replace_binds(query: &str) -> String {
    let mut result = String::new();
    let mut bind_iter = query.split("-- binds:").nth(1).unwrap_or("").trim().trim_start_matches('[').trim_end_matches(']').split(", ").map(|s| s.trim_matches('"').to_string());
    let mut in_bind_section = false;
    let mut chars = query.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '?' && !in_bind_section {
            if let Some(bind) = bind_iter.next() {
                result.push_str(&format!("'{}'", bind));
            } else {
                result.push(c);
            }
        } else {
            result.push(c);
            if c == '-' && chars.peek() == Some(&'-') {
                in_bind_section = true;
            }
        }
    }

    result
}

pub fn handle_query(query: &dyn DebugQuery, dialect: &DialectType) {
    let query_string = replace_binds(&query.to_string());
    log::debug!("Query string: {}", query_string);
    log::debug!("Using dialect: {:?}", query);
    
    let dialect_impl: Box<dyn Dialect> = dialect.build();
    match Parser::parse_sql(&*dialect_impl, &query_string) {
        Ok(statements) => {
            for statement in statements {
                match statement {
                    Statement::Insert(insert) => {
                        log::info!("Insert into table: {}", insert);
                        
                    }
                    Statement::Update {
                        table: table_name,
                        assignments,
                        selection,
                        ..
                    } => {
                        log::info!("Update table: {} with {:?} and {:?}", table_name, assignments, selection);
                    }
                    Statement::Delete(delete) => {
                        log::info!("Delete from table: {}", delete);
                    }
                    _ => {
                        log::info!("Other statement: {:?}", statement);
                    }
                }
            }
        }
        Err(e) => {
            log::error!("Failed to parse SQL query: {}", e);
        }
    }
    
}

pub fn extract_type(query: &dyn DebugQuery, dialect: &DialectType) {
    let query_string = query.to_string();
    log::debug!("Query string for type extraction: {}", query_string);
    // Further parsing logic can be added here
    let dialect_impl: Box<dyn Dialect> = dialect.build();
    match Parser::parse_sql(&*dialect_impl, &query_string) {
        Ok(statements) => {
            for statement in statements {
                if let Statement::Insert(insert) = statement {
                    log::debug!("Extracted table name: {}", insert.table);
                }
            }
        }
        Err(e) => {
            log::error!("Failed to parse SQL query: {}", e);
        }
    }

}