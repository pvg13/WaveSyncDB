
#[derive(Debug, Clone)]
pub struct DbQuery {
    sql: String,
    parameters: Vec<String>,
    topic: String
}

impl DbQuery {
    pub fn new(sql: String, parameters: Vec<String>, topic: String) -> Self {
        DbQuery { sql, parameters, topic }
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub fn parameters(&self) -> &Vec<String> {
        &self.parameters
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }
}