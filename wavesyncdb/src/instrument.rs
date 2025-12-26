pub mod dialects;
mod types;
mod util;

use diesel::connection::Instrumentation;
use diesel::connection::InstrumentationEvent;

use crate::instrument::dialects::DialectType;
use crate::instrument::types::{SqlQueryKind, classify_sql_query};
use crate::instrument::util::rewrite_debugquery_to_sql;

pub struct WaveSyncInstrument {
    // You can add fields here if needed for state tracking
    dialect: DialectType,
    topic: String,
    tx: tokio::sync::mpsc::Sender<crate::types::DbQuery>,
}

impl WaveSyncInstrument {
    pub fn new(
        tx: tokio::sync::mpsc::Sender<crate::types::DbQuery>,
        topic: impl Into<String>,
        dialect: DialectType,
    ) -> Self {
        WaveSyncInstrument {
            dialect,
            topic: topic.into(),
            tx,
        }
    }
}

impl Instrumentation for WaveSyncInstrument {
    fn on_connection_event(&mut self, event: diesel::connection::InstrumentationEvent<'_>) {
        match event {
            InstrumentationEvent::FinishEstablishConnection { url, error, .. } => {
                log::debug!("Finished establishing connection to: {}", url);
                if let Some(err) = error {
                    log::error!("Error establishing connection: {}", err);
                }
                let dialect_type = dialects::DialectType::from_url(url);
                self.dialect = dialect_type;
            }
            InstrumentationEvent::FinishQuery { query, .. } => {
                // Here you can parse the query and handle it accordingly
                // handle_query(query, &self.dialect);

                let query_kind = classify_sql_query(&query.to_string());

                let query_string =
                    rewrite_debugquery_to_sql(&query.to_string(), &self.dialect).unwrap();

                let db_query = crate::types::DbQuery::new(
                    query_string.to_string(),
                    vec![],
                    self.topic.clone(),
                );

                match query_kind {
                    SqlQueryKind::Select => {
                        log::debug!("SELECT query executed: {}", query); 
                    }
                    SqlQueryKind::Insert => {
                        log::debug!("INSERT query executed: {}", query);
                        self.tx.try_send(db_query).unwrap_or_else(|e| {
                            log::error!("Failed to send query to sync engine: {}", e);
                        });
                    }
                    SqlQueryKind::Update => {
                        log::debug!("UPDATE query executed: {}", query);
                        self.tx.try_send(db_query).unwrap_or_else(|e| {
                            log::error!("Failed to send query to sync engine: {}", e);
                        });
                    }
                    SqlQueryKind::Delete => {
                        log::debug!("DELETE query executed: {}", query);
                        self.tx.try_send(db_query).unwrap_or_else(|e| {
                            log::error!("Failed to send query to sync engine: {}", e);
                        });
                    }
                    _ => {
                        log::debug!("Other query executed: {}", query);
                    }
                }
            }
            InstrumentationEvent::CommitTransaction { depth, .. } => {
                log::debug!("Committed transaction at depth: {}", depth);
            }
            _ => {
                /* Handle other events if necessary */
                log::debug!("Unhandled database event: {:?}", event);
            }
        }
    }
}
