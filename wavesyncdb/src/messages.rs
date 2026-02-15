use core::error;
use std::error::Error;

use libp2p::PeerId;
use sqlparser::ast::Table;

use crate::{CrudError, DATABASE};

type TableName = String;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum OpValue {
    Integer(i64),
    Text(String),
    Boolean(bool),
    Float(f64),
    Null,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum Operation {
    Create {
        table: TableName,
        fields: Vec<(String, OpValue)>,
    },
    Update {
        table: TableName,
        pk: i32,
        fields: Vec<(String, OpValue)>,
    },
    Delete {
        table: TableName,
        pk: i32,
    },
}

pub struct SyncMessage {
    pub sequence_id: u64, // For ordering
    pub origin_peer: PeerId,
    pub operation: Operation,
    pub timestamp: i64, // Last-Write-Wins resolution
}

impl Operation {
    pub async fn execute(&self) -> Result<(), crate::CrudError> {
        let db = DATABASE.get().ok_or(CrudError::DbNotInitialized)?;

        match self {
            Operation::Create { table, fields } => {
                let names: Vec<&str> = fields.iter().map(|(n, _)| n.as_str()).collect();
                let placeholders = (1..=fields.len())
                    .map(|i| format!("${}", i))
                    .collect::<Vec<_>>()
                    .join(", ");

                // We use INSERT OR IGNORE to handle duplicate gossip messages gracefully
                let sql = format!(
                    "INSERT OR IGNORE INTO {} ({}) VALUES ({})",
                    table,
                    names.join(", "),
                    placeholders
                );

                let mut query = sqlx::query(&sql);
                for (_, val) in fields {
                    query = self.bind_op_value(query, val);
                }
                query.execute(db).await?;
            }

            Operation::Update { table, pk, fields } => {
                let set_clause = fields.iter().enumerate()
                    .map(|(i, (name, _))| format!("{} = ${}", name, i + 1))
                    .collect::<Vec<_>>()
                    .join(", ");

                // Primary key is the last parameter
                let pk_index = fields.len() + 1;
                let sql = format!("UPDATE {} SET {} WHERE id = ${}", table, set_clause, pk_index);

                let mut query = sqlx::query(&sql);
                for (_, val) in fields {
                    query = self.bind_op_value(query, val);
                }
                query = query.bind(pk); // Bind the i32 primary key
                
                query.execute(db).await?;
            }

            Operation::Delete { table, pk } => {
                let sql = format!("DELETE FROM {} WHERE id = $1", table);
                sqlx::query(&sql)
                    .bind(pk)
                    .execute(db)
                    .await?;
            }
        }
        Ok(())
    }

    /// Helper to bridge our OpValue enum to sqlx binding
    fn bind_op_value<'q>(
        &self, 
        mut query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>, 
        value: &'q OpValue
    ) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
        match value {
            OpValue::Integer(i) => query.bind(i),
            OpValue::Text(s) => query.bind(s),
            OpValue::Boolean(b) => query.bind(b),
            OpValue::Float(f) => query.bind(f),
            OpValue::Null => query.bind(None::<i32>),
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[tokio::test]
//     async fn test_create_operation() {

//         let op = Operation::Create {
//             table: "users".to_string(),
//             fields: vec![
//                 ("id".to_string(), "INTEGER PRIMARY KEY".to_string()),
//                 ("name".to_string(), "TEXT".to_string()),
//                 ("email".to_string(), "TEXT".to_string()),
//             ],
//         };
//         // op.execute().await.unwrap();

//         );
//         // op.execute().await.unwrap();
//     }

//     #[tokio::test]
//     async fn test_update_operation() {
//         let op = Operation::Update(
//             "users".to_string(),
//             vec![
//                 ("name".to_string(), "Bob".to_string()),
//                 ("email".to_string(), "bob@example.com".to_string()),
//             ],
//         );
//         // op.execute().await.unwrap();
//     }

//     #[tokio::test]
//     async fn test_delete_operation() {
//         let op = Operation::Delete("users".to_string(), 1);
//         // op.execute().await.unwrap();
//     }
// }
