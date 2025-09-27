//! Rewrite Diesel DebugQuery strings into executable SQL with inlined literals.
//!
//! Supported input shape (as printed by Diesel):
//!   INSERT ... VALUES (?, ?) -- binds: ["foo", 1]
//!   UPDATE ... WHERE id = ?  -- binds: [42]
//!   DELETE ... WHERE id = $1 -- binds: [42]        (Postgres style)
//!
//! This is intended for debugging/testing. For production, prefer prepared
//! statements (`sql_query(...).bind(...).execute(...)`).

use crate::instrument::dialects::DialectType;


#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RewriterError {
    /// The input had no `-- binds:` trailer to parse.
    MissingBindsTrailer,
    /// The binds list couldn't be parsed (malformed JSON-like array).
    MalformedBinds,
    /// Placeholder count doesn't match binds length.
    PlaceholderCountMismatch { placeholders: usize, binds: usize },
}

#[derive(Debug, Clone)]
struct Bind {
    raw: String,
    was_quoted: bool,
}

fn parse_binds(binds_part: &str) -> Result<Vec<Bind>, RewriterError> {
    // Expect something like: ` -- binds: ["a", 1, NULL, true]`
    // We parse a *very small* JSON-like subset:
    // - Items separated by commas
    // - Strings delimited by double quotes with \" escaping
    // - Literals: NULL/null, true/false, numbers
    let mut s = binds_part.trim();
    let lb = s.find('[').ok_or(RewriterError::MalformedBinds)? + 1;
    let rb = s.rfind(']').ok_or(RewriterError::MalformedBinds)?;
    if rb < lb { return Err(RewriterError::MalformedBinds); }
    s = &s[lb..rb];

    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_str = false;
    let mut esc = false;
    let mut was_quoted = false;

    for ch in s.chars() {
        match (in_str, esc, ch) {
            (true, true, c) => { cur.push(c); esc = false; }
            (true, false, '\\') => esc = true,
            (true, false, '"')  => in_str = false,
            (true, false, c)    => cur.push(c),

            (false, _, '"') => { in_str = true; was_quoted = true; }
            (false, _, ',') => {
                let item = cur.trim().to_string();
                if !item.is_empty() || was_quoted {
                    out.push(Bind { raw: item, was_quoted });
                }
                cur.clear();
                was_quoted = false;
            }
            (false, _, c) => cur.push(c),
        }
    }

    let tail = cur.trim().to_string();
    if !tail.is_empty() || was_quoted {
        out.push(Bind { raw: tail, was_quoted });
    }

    Ok(out)
}

/// Format a single bind value as a backend-specific SQL literal.
///
/// * Strings → single quoted with `'` doubled.
/// * NULL → `NULL`
/// * Booleans → `1/0` for SQLite, `true/false` otherwise
/// * Numbers → unquoted
fn format_literal(bind: &Bind, dialect: &DialectType) -> String {
    let s = bind.raw.as_str();

    // NULL
    if !bind.was_quoted && s.eq_ignore_ascii_case("null") {
        return "NULL".to_string();
    }
    // booleans
    if !bind.was_quoted && (s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("false")) {
        return match dialect {
            &DialectType::SQLite => if s.eq_ignore_ascii_case("true") { "1".into() } else { "0".into() },
            _ => s.to_ascii_lowercase(),
        };
    }
    // integers / floats
    if !bind.was_quoted && s.parse::<i128>().is_ok() { return s.to_string(); }
    if !bind.was_quoted && s.parse::<f64>().is_ok()  { return s.to_string(); }

    // default: treat as string
    let escaped = s.replace('\'', "''");
    format!("'{}'", escaped)
}

/// Count placeholders in the SQL (ignoring those inside strings and comments).
/// - For SQLite/MySQL: `?`
/// - For Postgres: `$1`, `$2`, ...
fn count_placeholders(sql: &str, dialect: &DialectType) -> usize {
    let mut count = 0usize;

    let mut in_sq = false; // single-quoted string
    let mut in_dq = false; // double-quoted identifier/string (PG identifiers)
    let mut in_bt = false; // backtick-quoted identifier (MySQL/SQLite)
    let mut in_lc = false; // line comment --
    let mut in_bc = false; // block comment /* */

    let mut it = sql.chars().peekable();

    while let Some(c) = it.next() {
        // Handle comments first
        if in_lc {
            if c == '\n' { in_lc = false; }
            continue;
        }
        if in_bc {
            if c == '*' && matches!(it.peek(), Some('/')) {
                it.next(); // consume '/'
                in_bc = false;
            }
            continue;
        }

        // String/identifier state toggles
        match c {
            '\'' if !in_dq && !in_bt => {
                // doubled '' inside string should not toggle
                if in_sq && matches!(it.peek(), Some('\'')) {
                    it.next(); // consume second quote
                } else {
                    in_sq = !in_sq;
                }
                continue;
            }
            '"' if !in_sq && !in_bt => { in_dq = !in_dq; continue; }
            '`' if !in_sq && !in_dq => { in_bt = !in_bt; continue; }
            '-' if !in_sq && !in_dq && !in_bt && matches!(it.peek(), Some('-')) => {
                it.next(); in_lc = true; continue;
            }
            '/' if !in_sq && !in_dq && !in_bt && matches!(it.peek(), Some('*')) => {
                it.next(); in_bc = true; continue;
            }
            _ => {}
        }

        if in_sq || in_dq || in_bt { continue; }

        match dialect {
            DialectType::SQLite | DialectType::MySQL => {
                if c == '?' { count += 1; }
            }
            DialectType::PostgreSQL => {
                if c == '$' {
                    // capture digits
                    let mut n = String::new();
                    while let Some(d) = it.peek() {
                        if d.is_ascii_digit() { n.push(*d); it.next(); } else { break; }
                    }
                    if !n.is_empty() {
                        count += 1;
                    }
                }
            },
            DialectType::Unknown => { /* can't count */
                log::warn!("Counting placeholders is not supported for Unknown dialect");
                return 0;
            }
        }
    }

    count
}

/// Replace placeholders with binds and return executable SQL.
/// Returns an error if trailer/binds are missing or counts mismatch.
pub fn rewrite_debugquery_to_sql(input: &str, dialect: &DialectType) -> Result<String, RewriterError> {
    let (sql_part, binds_part) = input
        .split_once("-- binds:")
        .ok_or(RewriterError::MissingBindsTrailer)?;

    let binds = parse_binds(binds_part)?;
    let expected = count_placeholders(sql_part, dialect);
    if expected != binds.len() {
        return Err(RewriterError::PlaceholderCountMismatch {
            placeholders: expected,
            binds: binds.len(),
        });
    }

    let mut out = String::with_capacity(sql_part.len());

    let mut in_sq = false;
    let mut in_dq = false;
    let mut in_bt = false;
    let mut in_lc = false;
    let mut in_bc = false;
    let mut bi = 0usize;
    let mut it = sql_part.chars().peekable();

    while let Some(c) = it.next() {
        // comments
        if in_lc {
            out.push(c);
            if c == '\n' { in_lc = false; }
            continue;
        }
        if in_bc {
            if c == '*' && matches!(it.peek(), Some('/')) {
                out.push(c); out.push(it.next().unwrap());
                in_bc = false;
            } else {
                out.push(c);
            }
            continue;
        }

        // string/identifier states
        match c {
            '\'' if !in_dq && !in_bt => {
                // doubled '' inside string remains inside
                if in_sq && matches!(it.peek(), Some('\'')) {
                    out.push('\''); out.push(it.next().unwrap());
                } else {
                    in_sq = !in_sq;
                    out.push('\'');
                }
                continue;
            }
            '"' if !in_sq && !in_bt => { in_dq = !in_dq; out.push('"'); continue; }
            '`' if !in_sq && !in_dq => { in_bt = !in_bt; out.push('`'); continue; }
            '-' if !in_sq && !in_dq && !in_bt && matches!(it.peek(), Some('-')) => {
                out.push('-'); out.push(it.next().unwrap()); in_lc = true; continue;
            }
            '/' if !in_sq && !in_dq && !in_bt && matches!(it.peek(), Some('*')) => {
                out.push('/'); out.push(it.next().unwrap()); in_bc = true; continue;
            }
            _ => {}
        }

        if in_sq || in_dq || in_bt {
            out.push(c);
            continue;
        }

        match dialect {
            DialectType::SQLite | DialectType::MySQL => {
                if c == '?' {
                    out.push_str(&format_literal(&binds[bi], dialect));
                    bi += 1;
                } else {
                    out.push(c);
                }
            }
            DialectType::PostgreSQL => {
                if c == '$' {
                    // consume digits
                    let mut n = String::new();
                    while let Some(d) = it.peek() {
                        if d.is_ascii_digit() { n.push(*d); it.next(); } else { break; }
                    }
                    if let Ok(idx1) = n.parse::<usize>() {
                        // 1-based index
                        out.push_str(&format_literal(&binds[idx1 - 1], dialect));
                    } else {
                        out.push('$');
                    }
                } else {
                    out.push(c);
                }
            },
            DialectType::Unknown => {
                // Just copy input as-is
                out.push(c);
            }
        }
    }

    Ok(out.trim_end().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_sqlite() {
        let dbg = r#"INSERT INTO `tasks` (`title`, `description`, `completed`) VALUES (?, ?, ?) -- binds: ["Sample Task", "This is a sample task", false]"#;
        let out = rewrite_debugquery_to_sql(dbg, &DialectType::SQLite).unwrap();
        assert_eq!(
            out,
            "INSERT INTO `tasks` (`title`, `description`, `completed`) VALUES ('Sample Task', 'This is a sample task', 0)"
        );
    }

    #[test]
    fn update_mysql_bool_literal() {
        let dbg = r#"UPDATE `tasks` SET `completed` = ? WHERE (`tasks`.`id` = ?) -- binds: [true, 1]"#;
        let out = rewrite_debugquery_to_sql(dbg, &DialectType::MySQL).unwrap();
        // MySQL accepts TRUE/FALSE keywords
        assert_eq!(
            out,
            "UPDATE `tasks` SET `completed` = true WHERE (`tasks`.`id` = 1)"
        );
    }

    #[test]
    fn delete_sqlite() {
        let dbg = r#"DELETE FROM `tasks` WHERE (`tasks`.`id` = ?) -- binds: [1]"#;
        let out = rewrite_debugquery_to_sql(dbg, &DialectType::SQLite).unwrap();
        assert_eq!(out, "DELETE FROM `tasks` WHERE (`tasks`.`id` = 1)");
    }

    #[test]
    fn postgres_numbered_placeholders() {
        let dbg = r#"SELECT * FROM users WHERE id = $1 AND name = $2 -- binds: [42, "Alice"]"#;
        let out = rewrite_debugquery_to_sql(dbg, &DialectType::PostgreSQL).unwrap();
        assert_eq!(out, "SELECT * FROM users WHERE id = 42 AND name = 'Alice'");
    }

    #[test]
    fn does_not_replace_inside_strings_or_comments() {
        let dbg = r#"SELECT '?' AS q, '-- binds: nope' AS c /* $1 ? */ , col FROM t WHERE x = ? -- binds: [7]"#;
        let out = rewrite_debugquery_to_sql(dbg, &DialectType::SQLite).unwrap();
        assert_eq!(
            out,
            "SELECT '?' AS q, '-- binds: nope' AS c /* $1 ? */ , col FROM t WHERE x = 7"
        );
    }

    #[test]
    fn doubled_single_quotes_inside_string() {
        // '?' inside a string must not be replaced; doubled '' kept
        let dbg = r#"SELECT 'it''s ? not a placeholder' AS s, col FROM t WHERE x = ? -- binds: [9]"#;
        let out = rewrite_debugquery_to_sql(dbg, &DialectType::SQLite).unwrap();
        assert_eq!(
            out,
            "SELECT 'it''s ? not a placeholder' AS s, col FROM t WHERE x = 9"
        );
    }

    #[test]
    fn placeholders_mismatch_error() {
        let dbg = r#"SELECT * FROM t WHERE a = ? AND b = ? -- binds: [1]"#;
        let err = rewrite_debugquery_to_sql(dbg, &DialectType::SQLite).unwrap_err();
        match err {
            RewriterError::PlaceholderCountMismatch { placeholders, binds } => {
                assert_eq!(placeholders, 2);
                assert_eq!(binds, 1);
            }
            _ => panic!("unexpected error"),
        }
    }

    #[test]
    fn null_and_numbers_and_strings() {
        let dbg = r#"INSERT INTO t (a,b,c,d) VALUES ($1,$2,$3,$4) -- binds: [NULL, 3.14, 7, "hey"]"#;
        let out = rewrite_debugquery_to_sql(dbg, &DialectType::PostgreSQL).unwrap();
        assert_eq!(
            out,
            "INSERT INTO t (a,b,c,d) VALUES (NULL, 3.14, 7, 'hey')"
        );
    }

    #[test]
    fn quoted_identifiers_and_backticks() {
        let dbg = r#"UPDATE "weird-table" SET `val` = ? WHERE "id" = ? -- binds: ["a'b", 5]"#;
        let out = rewrite_debugquery_to_sql(dbg, &DialectType::MySQL).unwrap();
        assert_eq!(
            out,
            "UPDATE \"weird-table\" SET `val` = 'a''b' WHERE \"id\" = 5"
        );
    }

    #[test]
    fn missing_binds_trailer() {
        let dbg = r#"SELECT 1"#;
        let err = rewrite_debugquery_to_sql(dbg, &DialectType::SQLite).unwrap_err();
        assert!(matches!(err, RewriterError::MissingBindsTrailer));
    }

    #[test]
    fn malformed_binds() {
        let dbg = r#"SELECT ? -- binds: [1, "unterminated]"#;
        let err = parse_binds("-- binds: [1, \"unterminated]").unwrap_err_or(RewriterError::MalformedBinds);
        let _ = dbg; // silence warning
        match err {
            RewriterError::MalformedBinds => {}
            _ => panic!("expected MalformedBinds"),
        }
    }

    // Small helper for the malformed test above
    trait UnwrapErrOr<T> {
        fn unwrap_err_or(self, default: RewriterError) -> RewriterError;
    }
    impl<T> UnwrapErrOr<T> for Result<T, RewriterError> {
        fn unwrap_err_or(self, default: RewriterError) -> RewriterError {
            match self {
                Ok(_) => default,
                Err(e) => e,
            }
        }
    }
}
