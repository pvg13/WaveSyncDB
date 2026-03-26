// Usage:
// cargo run -p wavesyncdb --example managed_relay_test -- \
//   --relay-addr /ip4/127.0.0.1/tcp/4001/p2p/12D3Koo... \
//   --api-key wsc_live_xxx

use std::time::Duration;

use wavesyncdb::{NetworkEvent, RelayStatus, WaveSyncDbBuilder};

#[tokio::main]
async fn main() {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();

    let args: Vec<String> = std::env::args().collect();

    let relay_addr = args
        .iter()
        .position(|a| a == "--relay-addr")
        .and_then(|i| args.get(i + 1))
        .expect("missing --relay-addr");

    let api_key = args
        .iter()
        .position(|a| a == "--api-key")
        .and_then(|i| args.get(i + 1))
        .expect("missing --api-key");

    let db = WaveSyncDbBuilder::new("sqlite::memory:", "managed-relay-test")
        .managed_relay(relay_addr, api_key)
        .build()
        .await
        .expect("failed to build WaveSyncDb");

    let mut rx = db.network_event_rx();

    let result = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            match rx.recv().await {
                Ok(NetworkEvent::RelayStatusChanged(RelayStatus::Connected)) => {
                    log::info!("Managed relay connected — auth accepted");
                    return true;
                }
                Ok(NetworkEvent::EngineFailed { reason }) => {
                    log::error!("Engine failed: {reason}");
                    return false;
                }
                Ok(event) => {
                    log::debug!("Event: {event:?}");
                }
                Err(e) => {
                    log::error!("Event channel error: {e}");
                    return false;
                }
            }
        }
    })
    .await;

    match result {
        Ok(true) => {
            log::info!("SUCCESS");
            std::process::exit(0);
        }
        Ok(false) => {
            log::error!("FAILED");
            std::process::exit(1);
        }
        Err(_) => {
            log::error!("TIMEOUT — no auth result within 15s");
            std::process::exit(1);
        }
    }
}
