use std::sync::{Arc, Mutex};

use crate::db::WaveSyncFfi;
use crate::error::WaveSyncError;

/// Initialize Android logcat logging once.
pub(crate) fn init_logging() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        #[cfg(target_os = "android")]
        {
            android_logger::init_once(
                android_logger::Config::default()
                    .with_max_level(log::LevelFilter::Info)
                    .with_tag("wavesyncdb"),
            );
        }
        #[cfg(not(target_os = "android"))]
        {
            // env_logger is optional; only init if available
        }
    });
}

#[derive(uniffi::Object)]
pub struct WaveSyncFfiBuilder {
    inner: Mutex<Option<wavesyncdb::WaveSyncDbBuilder>>,
}

#[uniffi::export(async_runtime = "tokio")]
impl WaveSyncFfiBuilder {
    #[uniffi::constructor]
    pub fn new(db_url: String, topic: String) -> Arc<Self> {
        init_logging();
        Arc::new(Self {
            inner: Mutex::new(Some(wavesyncdb::WaveSyncDbBuilder::new(&db_url, &topic))),
        })
    }

    pub fn with_passphrase(self: Arc<Self>, passphrase: String) -> Arc<Self> {
        self.mutate(|b| b.with_passphrase(&passphrase));
        self
    }

    pub fn with_relay_server(self: Arc<Self>, addr: String) -> Arc<Self> {
        self.mutate(|b| b.with_relay_server(&addr));
        self
    }

    pub fn with_rendezvous_server(self: Arc<Self>, addr: String) -> Arc<Self> {
        self.mutate(|b| b.with_rendezvous_server(&addr));
        self
    }

    pub fn with_bootstrap_peer(self: Arc<Self>, addr: String) -> Arc<Self> {
        self.mutate(|b| b.with_bootstrap_peer(&addr));
        self
    }

    pub fn with_ipv6(self: Arc<Self>, enabled: bool) -> Arc<Self> {
        self.mutate(|b| b.with_ipv6(enabled));
        self
    }

    pub fn with_sync_interval(self: Arc<Self>, seconds: u64) -> Arc<Self> {
        self.mutate(|b| b.with_sync_interval(std::time::Duration::from_secs(seconds)));
        self
    }

    pub fn with_keep_alive_interval(self: Arc<Self>, seconds: u64) -> Arc<Self> {
        self.mutate(|b| b.with_keep_alive_interval(std::time::Duration::from_secs(seconds)));
        self
    }

    pub fn with_push_token(self: Arc<Self>, platform: String, token: String) -> Arc<Self> {
        self.mutate(|b| b.with_push_token(&platform, &token));
        self
    }

    pub fn managed_relay(self: Arc<Self>, addr: String, api_key: String) -> Arc<Self> {
        self.mutate(|b| b.managed_relay(&addr, &api_key));
        self
    }

    pub async fn build(self: Arc<Self>) -> Result<Arc<WaveSyncFfi>, WaveSyncError> {
        let builder = self
            .inner
            .lock()
            .unwrap()
            .take()
            .ok_or(WaveSyncError::NotInitialized)?;

        let db = builder.build().await?;
        let rt = tokio::runtime::Handle::current();

        Ok(Arc::new(WaveSyncFfi::new(db, rt)))
    }
}

impl WaveSyncFfiBuilder {
    /// Helper to apply a mutation to the inner builder.
    fn mutate<F>(&self, f: F)
    where
        F: FnOnce(wavesyncdb::WaveSyncDbBuilder) -> wavesyncdb::WaveSyncDbBuilder,
    {
        let mut guard = self.inner.lock().unwrap();
        if let Some(builder) = guard.take() {
            *guard = Some(f(builder));
        }
    }
}
