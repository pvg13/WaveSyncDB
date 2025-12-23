#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlQueryKind {
    Insert,
    Update,
    Delete,
    Select,
    Create,
    Alter,
    Drop,
    Other,
}

/// Very simple classifier: look at the first keyword in the SQL part of a Diesel DebugQuery string.
///
/// It strips leading whitespace and the `-- binds:` trailer.
/// It is *case-insensitive*.
pub fn classify_sql_query(debug_query: &str) -> SqlQueryKind {
    // Cut off the binds part if present
    let sql = debug_query
        .split_once("-- binds:")
        .map(|(a, _)| a)
        .unwrap_or(debug_query);

    // Trim whitespace and lowercase for comparison
    let sql_trimmed = sql.trim_start();
    let first_word = sql_trimmed
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();

    match first_word.as_str() {
        "insert" => SqlQueryKind::Insert,
        "update" => SqlQueryKind::Update,
        "delete" => SqlQueryKind::Delete,
        "select" => SqlQueryKind::Select,
        "create" => SqlQueryKind::Create,
        "alter" => SqlQueryKind::Alter,
        "drop" => SqlQueryKind::Drop,
        _ => SqlQueryKind::Other,
    }
}

#[cfg(test)]
mod classify_tests {
    use super::*;

    #[test]
    fn test_insert() {
        let dbg = r#"INSERT INTO `tasks` (...) VALUES (?) -- binds: [1]"#;
        assert_eq!(classify_sql_query(dbg), SqlQueryKind::Insert);
    }

    #[test]
    fn test_update() {
        let dbg = r#"   UPDATE tasks SET x = ? -- binds: [1]"#;
        assert_eq!(classify_sql_query(dbg), SqlQueryKind::Update);
    }

    #[test]
    fn test_delete() {
        let dbg = r#"DELETE FROM tasks WHERE id = ? -- binds: [42]"#;
        assert_eq!(classify_sql_query(dbg), SqlQueryKind::Delete);
    }

    #[test]
    fn test_select() {
        let dbg = r#"SELECT * FROM users -- binds: []"#;
        assert_eq!(classify_sql_query(dbg), SqlQueryKind::Select);
    }

    #[test]
    fn test_other() {
        let dbg = r#"PRAGMA table_info(tasks)"#;
        assert_eq!(classify_sql_query(dbg), SqlQueryKind::Other);
    }
}
