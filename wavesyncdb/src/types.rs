
#[derive(Debug, Clone)]
pub struct DbQuery {
    sql: String,
}

impl DbQuery {
    pub fn new(sql: String) -> Self {
        DbQuery { sql }
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }
}