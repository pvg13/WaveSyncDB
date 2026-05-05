# FAQ & troubleshooting

## When should I *not* use WaveSyncDB?

- **Public, multi-tenant fan-out.** If your data is meant to reach untrusted strangers, this is the wrong tool — every authenticated peer can read and write everything in the topic.
- **Authoritative central state.** If you need a single source of truth that arbitrates writes (financial transactions, inventory levels), a traditional client-server database with a server-side write API is what you want.
- **Heavy write throughput** (>1000 writes/s sustained). The protocol is fine for collaborative apps but not optimized for telemetry-style ingest. Use Kafka.
- **Adversarial peers.** WaveSyncDB has no per-row ACLs. Anyone with the passphrase can write anything.

## Does it work offline?

Yes. The local SQLite write commits before any network step. The change waits in the shadow table until a peer reconnects and asks for it. There is no "online required" mode.

## What happens if I lose the passphrase?

The mesh keeps working for everyone who still has it. A peer that loses the passphrase can:

- Keep using its local SQLite (the data is still there, unencrypted at rest).
- Re-join only by getting the passphrase back. There is no recovery mechanism.

## Can I rotate the passphrase?

Only by coordinating across every peer simultaneously — the new passphrase changes the topic hash, so old and new peers don't see each other. The recommended sequence is:

1. Deploy a new app version that supports the new passphrase.
2. Roll out to every peer.
3. After a confirmation period, drop the old passphrase support.

There is no graceful in-place rotation protocol.

## How do I migrate from raw SeaORM?

Replace `DatabaseConnection` with `WaveSyncDb`:

```diff
- let db: DatabaseConnection = Database::connect("sqlite:./app.db").await?;
+ let db = WaveSyncDbBuilder::new("sqlite:./app.db?mode=rwc", "my-topic")
+     .build()
+     .await?;
+ db.get_schema_registry(module_path!().split("::").next().unwrap()).sync().await?;
```

Then add `#[derive(SyncEntity)]` to each entity you want synced. Existing rows get picked up the next time a peer asks for catch-up — they're already in the entity table, so the registry just builds the shadow rows lazily on first write.

## Why aren't my changes appearing on the other peer?

Walk through the layers in order:

1. **Did the local write commit?** Read it back from the same connection — if SeaORM doesn't see it, the write didn't happen.
2. **Did the engine emit a `SyncChangeset`?** Subscribe to the change broadcast: `db.change_rx().recv().await`. If you don't see a notification, the SQL parser didn't recognize the statement (e.g., a raw `execute()` of a multi-statement script). Check your write pattern.
3. **Are the peers connected?** Check `db.network_status().connected_peers`. Empty list means peer discovery hasn't completed.
4. **Same topic + passphrase?** A topic mismatch silently rejects all messages — check both sides.
5. **HMAC failing?** Enable `RUST_LOG=wavesyncdb=debug` and look for "HMAC verification failed". Almost always a passphrase typo.
6. **Schema registered on both sides?** A peer that hasn't called `.sync()` yet rejects inbound changes for unknown tables.

## Why is sync slow on cellular?

The slow path is **circuit-relay-based**: cellular CGNAT defeats DCUtR hole-punching, so traffic stays on the relay. Typical numbers:

- Circuit-relay round trip: 100–250 ms.
- DCUtR direct round trip: 20–60 ms.

If your relay is on a different continent from your users, latency adds up. Move the relay closer.

## Why is FCM background sync sometimes slow?

FCM delivery itself is the bottleneck. Google's "high priority data message" SLA is 5 s but in practice 95th-percentile delivery on Android is often 1.5–3 s, and iOS APNs is similar. Once the push lands, our `background_sync` typically finishes in <2 s on a warm cache, so the visible latency is dominated by Google/Apple infrastructure.

## How big can the database get?

There's no hard limit — SQLite handles tens of GB. The shadow tables grow proportionally to the number of column-writes (not rows), so a write-heavy table grows faster than its read counterpart.

For now there's no shadow-table compaction. If your dataset's write rate is high enough that shadow size becomes a problem, open an issue on GitHub — the data structure was designed to support log compaction; it just hasn't been implemented yet.

## Can I use it without Rust on the other side?

Today, no — the protocol is documented (see [Sync protocol](/docs/sync-protocol)) but the only client implementation is in Rust. A Swift / Kotlin / TypeScript client would need to:

1. Parse the JSON wire format.
2. Implement libp2p's request-response + Noise + QUIC stack (or use a libp2p port for that language).
3. Compute HMAC-BLAKE3 the same way.
4. Manage shadow tables in its local store.

This is non-trivial. Practically, the supported way to use WaveSyncDB on iOS/Android is to compile the Rust crate via FFI and call into it from Swift/Kotlin. The `wavesyncdb_ffi` crate exposes a C ABI for that.

## Can two peers use different SQLite versions?

Yes — the wire format doesn't depend on SQLite version. You can have one peer on SQLite 3.39 and another on 3.45 without issue. WaveSyncDB only requires a SQLite that supports `INSERT OR REPLACE` and indexed views, both standard since 3.0.

## How do I run an integration test against a real network?

The repository's integration tests under `wavesyncdb/tests/` use file-backed temp SQLite databases and process-local peers. They run with `--test-threads=1` because mDNS discovery is process-wide and parallel tests cross-discover. To test against a real LAN, set up two distinct binaries on two machines and use the example apps as a starting point.

## What's the relationship to gossipsub / pub-sub?

Earlier WaveSyncDB versions used libp2p **gossipsub** for fan-out. The current version uses libp2p **request-response** instead, because:

- gossipsub fan-out is best-effort with no per-message acknowledgement, so we needed a separate retry loop on top.
- request-response gives per-message acks for free, simplifying the protocol.
- gossipsub adds 30+ KiB per peer of meshing state; request-response adds none.

The trade-off is that very large topics (~100s of peers) would scale better with gossipsub. WaveSyncDB targets small groups, so this isn't a concern.

## Is the relay a single point of failure?

It's a single point of *coordination*, but data integrity doesn't depend on it. If the relay goes down:

- LAN peers (mDNS) keep syncing normally.
- WAN peers that already have direct connections keep syncing.
- New WAN peers can't discover or reach others until the relay is back.
- No data is lost. As soon as the relay returns, catch-up replays everything.

For higher availability, you can run multiple relays — list them all in the `with_relay_server` calls (the engine connects to each); peers will then keep working as long as at least one relay is up.

## Does it support encryption at rest?

Not directly — that's a SQLite concern. Use SQLCipher or filesystem-level encryption if you need it. WaveSyncDB doesn't intercept SQLite writes at the filesystem layer, so SQLCipher integrates transparently.

## Is the project production-ready?

It is in production for the project's primary use case (a small-group collaboration app). The core sync engine, conflict resolution, and Dioxus integration are stable. The relay + push-notification integration is stable but newer; if you deploy at significant scale, expect to find rough edges and please open issues. License + commercial-support agreements are available — see the [License](/docs/introduction#licensing) section.
