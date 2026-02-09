use core::error;

use sqlparser::ast::Table;

use crate::DATABASE;

type TableName = String;
type UpdateFields = Vec<(String, String)>;
type CreateFields = Vec<(String, String)>;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum Operation {
    Create(TableName, CreateFields),
    Update(TableName, UpdateFields),
    Delete(TableName, i32),
}

impl Operation {
    pub async fn execute(&self) -> Result<(), impl error::Error> {
        match self {
            Operation::Create(table_name, fields) => {
                let field_defs: Vec<String> = fields
                    .iter()
                    .map(|(name, typ)| format!("{} {}", name, typ))
                    .collect();
                let query = format!(
                    "CREATE TABLE IF NOT EXISTS {} ({})",
                    table_name,
                    field_defs.join(", ")
                );
                sqlx::query(&query).execute(DATABASE.get().unwrap()).await?;

                // Insert the actual data into the table
                let field_names: Vec<String> = fields.iter().map(|(name, _)| name.clone()).collect();
                let placeholders: Vec<String> = (0..fields.len()).map(|i| format!("${}", i + 1)).collect();
                let insert_query = format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    table_name,
                    field_names.join(", "),
                    placeholders.join(", ")
                );
                let mut query = sqlx::query(&insert_query);
                for (_, value) in fields {
                    query = query.bind(value);
                }
                query.execute(DATABASE.get().unwrap()).await?;
            },
            Operation::Update(table_name, fields) => {
                let set_clauses: Vec<String> = fields
                    .iter()
                    .map(|(name, value)| format!("{} = '{}'", name, value))
                    .collect();
                let query = format!(
                    "UPDATE {} SET {}",
                    table_name,
                    set_clauses.join(", ")
                );
                sqlx::query(&query).execute(DATABASE.get().unwrap()).await?;
            },
            Operation::Delete(table_name, id) => {
                let query = format!(
                    "DELETE FROM {} WHERE id = {}",
                    table_name,
                    id
                );
                sqlx::query(&query).execute(DATABASE.get().unwrap()).await?;
            },
        }
        Ok::<(), sqlx::Error>(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_operation() {

        let op = Operation::Create(
            "users".to_string(),
            vec![
                ("id".to_string(), "INTEGER PRIMARY KEY".to_string()),
                ("name".to_string(), "TEXT".to_string()),
                ("email".to_string(), "TEXT".to_string()),
            ],
        );
        // op.execute().await.unwrap();
    }

    #[tokio::test]
    async fn test_update_operation() {
        let op = Operation::Update(
            "users".to_string(),
            vec![
                ("name".to_string(), "Bob".to_string()),
                ("email".to_string(), "bob@example.com".to_string()),
            ],
        );
        // op.execute().await.unwrap();
    }

    #[tokio::test]
    async fn test_delete_operation() {
        let op = Operation::Delete("users".to_string(), 1);
        // op.execute().await.unwrap();
    }
}
