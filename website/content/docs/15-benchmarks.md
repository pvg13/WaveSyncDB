# Benchmarks

Real numbers, captured from a single machine running both peers in process. We avoid the temptation to compare against other databases — every cross-product comparison invites tuning-fairness arguments. Instead, we measure WaveSyncDB against itself: the cost of the wrapper, the speed of live sync, the throughput of catch-up, and the time to converge under contention.

If you want to reproduce or rerun on your hardware, every binary lives in `examples/bench/` and runs with one `cargo run` invocation each. See [How to reproduce](#how-to-reproduce) at the bottom.

## Hardware and software

| Item | Value |
|---|---|
| CPU | 13th Gen Intel® Core™ i7-13700KF (24 logical cores) |
| Memory | 32 GB |
| OS | Linux 6.19 (CachyOS) |
| Rust | 1.92.0 |
| WaveSyncDB | commit `06232c2` + write-path optimization series (single-tx bookkeeping, batched ON CONFLICT shadow upsert, MAX-derived db_version, inlined dispatch) |
| Build | `cargo run --release` |
| Storage | local NVMe, SQLite WAL mode (default) |

## 1. Local write overhead

The cost of routing writes through `WaveSyncDb` instead of a raw SeaORM `DatabaseConnection`. Same SQLite database layout, same `task` entity, same number of ops. Only the connection wrapper differs. No peers configured — this is a pure local-write measurement.

| Operation | Raw SeaORM | WaveSyncDB | Ratio | Per-write overhead |
|---|---:|---:|---:|---:|
| `INSERT` | **22 393 ops/s** | **9 690 ops/s** | 0.43× | ~58 µs |
| `UPDATE` | **28 931 ops/s** | **11 002 ops/s** | 0.38× | ~56 µs |
| `DELETE` | **28 907 ops/s** | **11 728 ops/s** | 0.41× | ~51 µs |

5 000 ops per measurement. The bookkeeping that adds the overhead — shadow tables, per-column Lamport clocks, the broadcast send — is collapsed into a single SQLite transaction with one batched `ON CONFLICT DO UPDATE … RETURNING` for all changed columns at once. The `db_version` counter recovers from `MAX(shadow.db_version)` on startup, so it doesn't need a per-write `_wavesync_meta` write. Net: **two fsyncs per write** (the entity insert + the shadow tx), regardless of column count.

> **Reading these numbers**: 10 000 writes/s comfortably covers any user-facing CRUD app. WaveSyncDB is not built for telemetry-style ingest — if you need 100k writes/s, use Kafka.

These numbers are post-optimization. An earlier version of WaveSyncDB ran 7.5× slower than raw SeaORM on INSERT (3 700 ops/s); the [write-path rework](https://github.com/pvg13/WaveSyncDB/blob/main/wavesyncdb/src/connection.rs) closed most of that gap.

## 2. Peer-to-peer write latency (LAN)

Two `WaveSyncDb` instances on the same machine, mDNS discovery enabled, separate SQLite files. Peer B subscribes to `change_rx`; we time from `task.insert(&peer_a).await` returning to the matching `ChangeNotification` resolving on Peer B.

| Metric | Latency |
|---|---:|
| Mean | **0.43 ms** |
| p50 | **0.42 ms** |
| p95 | **0.57 ms** |
| p99 | **0.77 ms** |
| max | **1.31 ms** |

100 sequential inserts. The path measured is full end-to-end: parse SQL → shadow table → libp2p `request-response::Push` over loopback → HMAC verify on B → apply transaction → broadcast. Sub-millisecond p50 for live collaboration on the same host.

Real WAN measurements add the round-trip from the network plus the relay if circuit-relayed. Expect ~50–100 ms p50 over the public internet, dominated by the relay round trip.

## 3. Catch-up after offline period

Peer A writes 1 000 rows to its local SQLite. Then Peer B starts cold, mDNS discovers A, and we time the version-vector catch-up until Peer B's local table contains all 1 000 rows.

| Phase | Wall-clock |
|---|---:|
| Peer A writes 1 000 rows (no Peer B yet) | 0.11 s |
| Peer B engine startup | 0.001 s |
| Catch-up (mDNS discover + VV exchange + apply) | **0.17 s** |
| **Total time to converged state** | **0.17 s** |
| **Throughput during catch-up** | **5 933 rows / s** |

A peer that's been offline for any length of time, holding any number of accumulated changes, fully reconciles its state in a single round trip. The 1 000 rows here are arbitrary — the round-trip cost is constant, only the changeset payload size grows.

## 4. Concurrent same-column conflict resolution

Two peers, both updating `tasks.title` on the same row, 250 updates each, in parallel. We time both phases and verify both peers converge to the same final value (correctness assertion).

| Metric | Value |
|---|---:|
| Concurrent writes (250 × 2 = 500 total) | 0.07 s |
| Convergence after writes stop | 0.05 s |
| **Total wall-clock** | **0.12 s** |
| **Total throughput** | **4 030 updates / s** |
| Both peers' final `title` agreed | ✅ identical |

The deterministic `(col_version → value_bytes → site_id)` ordering means no human-visible "merge resolution" is needed — peers reach the same final state regardless of message arrival order.

## How to reproduce

```bash
git clone https://github.com/pvg13/WaveSyncDB
cd WaveSyncDB

# Build once (release mode is required for representative numbers)
cargo build -p wavesyncdb-bench --release

# Run any combination
cargo run -p wavesyncdb-bench --release --bin bench_overhead
cargo run -p wavesyncdb-bench --release --bin bench_lan_sync
cargo run -p wavesyncdb-bench --release --bin bench_catchup
cargo run -p wavesyncdb-bench --release --bin bench_conflict
```

Each binary prints a human-readable summary plus a single `BENCH_RESULT: { ... }` JSON line at the end so a CI dashboard can ingest results without scraping the human output. Source: [`examples/bench/`](https://github.com/pvg13/WaveSyncDB/tree/main/examples/bench).

## Caveats and what this does NOT measure

- **Single machine only**. The LAN and catch-up benches use loopback. Real LAN adds switching latency (~0.5 ms p50); cellular via the relay adds 50–200 ms.
- **No relay in the loop**. WAN benches require a real relay deployment; deferred to a follow-up.
- **No mobile.** All numbers are from x86_64 Linux. Android / iOS will be slower mainly because of slower disk and the FCM wake-up step.
- **No memory measurements**. Shadow table size scales with column-writes; for typical CRUD workloads the storage overhead is 5–15× the entity tables, which SQLite handles without breaking a sweat.
- **Steady state**, not p99.99 tail. The benches don't run long enough to capture once-an-hour stalls (GC-style collection? OS page cache eviction?). For sustained-tail measurement, run continuously and capture a histogram over hours.
- **Engine startup excluded** from the latency benches but included in catch-up. Cold-start latency (process spawn → engine ready) is ~150–250 ms on x86; we exclude it from steady-state benches because typical apps start once and stay running.

## Where to go from here

- [Sync protocol](/docs/sync-protocol) — what the bytes on the wire actually look like.
- [Conflict resolution](/docs/conflict-resolution) — why convergence is correct, not just empirically observed.
- [FAQ & troubleshooting](/docs/faq) — common bottlenecks and how to investigate them.
