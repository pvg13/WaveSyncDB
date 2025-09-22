mod handler;
mod dialects;
use std::sync::Arc;

use diesel::prelude::*;
use diesel::connection::Instrumentation;
use diesel::connection::InstrumentationEvent;
use sqlparser::dialect::Dialect;

use crate::instrument::dialects::DialectType;
use crate::instrument::handler::common::handle_query;


pub struct WaveSyncInstrument {
    // You can add fields here if needed for state tracking
    dialect: DialectType,
}

impl WaveSyncInstrument {
    pub fn new() -> Self {
        WaveSyncInstrument {
            dialect: DialectType::Unknown,
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
                log::debug!("Finished executing query: {}", query);
                // Here you can parse the query and handle it accordingly
                handle_query(query, &self.dialect);

            },
            InstrumentationEvent::CommitTransaction { depth, .. } => {
                log::debug!("Committed transaction at depth: {}", depth);
            },
            _ => { /* Handle other events if necessary */
                log::debug!("Unhandled database event: {:?}", event);
            }
        }
    }
}



