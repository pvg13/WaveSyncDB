use diesel::prelude::*;
use diesel::connection::Instrumentation;
use diesel::connection::InstrumentationEvent;

pub struct WaveSyncInstrument {}

impl Instrumentation for WaveSyncInstrument {
    fn on_connection_event(&mut self, event: diesel::connection::InstrumentationEvent<'_>) {
        match event {
            InstrumentationEvent::FinishQuery { query, .. } => {
                log::debug!("Finished executing SQL: {}", query);
                let query = query.to_string();
                match query.trim_start().to_uppercase().as_str() {
                    q if q.starts_with("SELECT") => {
                        log::debug!("WaveSync: Executed a SELECT query.");
                    },
                    q if q.starts_with("INSERT") => {
                        log::debug!("WaveSync: Executed an INSERT query.");
                    },
                    q if q.starts_with("UPDATE") => {
                        log::debug!("WaveSync: Executed an UPDATE query.");
                    },
                    q if q.starts_with("DELETE") => {
                        log::debug!("WaveSync: Executed a DELETE query.");
                    },
                    _ => {
                        log::debug!("WaveSync: Executed a different type of query.");
                    }
                }
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



