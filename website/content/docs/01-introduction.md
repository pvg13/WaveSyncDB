# Introduction

**WaveSyncDB** is a peer-to-peer sync layer for SeaORM applications. It lets you build offline-first apps where every device has a full local copy of the data and changes replicate automatically when peers are connected.

You keep writing standard SeaORM code — `ActiveModel::insert(&db)`, queries, the usual idioms — and WaveSyncDB handles replication transparently. When two peers edit the same data concurrently, per-column conflict resolution ensures they converge to the same state without manual intervention.

## What you get

- **Drop-in connection wrapper** — `WaveSyncDb` implements SeaORM's `ConnectionTrait`. Replace your `DatabaseConnection` and existing code keeps working.
- **Per-column conflict resolution** — concurrent edits to different columns both survive. Same-column conflicts resolve deterministically; every peer reaches the same final state.
- **Local-first** — writes commit to local SQLite immediately. The UI never blocks on the network. Sync runs in the background.
- **P2P networking** — mDNS for same-network discovery, circuit relay for WAN, DCUtR hole-punching when possible.
- **Mobile push wake-up** — silent FCM/APNs notifications wake sleeping phones so they catch up within seconds.
- **Group authentication** — a shared passphrase derives the topic and signs every message. Unauthenticated peers are silently dropped.
- **Cross-platform** — desktop, Android, iOS, and browser (wasm32) from one codebase.

> **License**: WaveSyncDB is dual-licensed. **AGPL-3.0-or-later** for open-source projects; a separate **commercial license** for proprietary or SaaS use. See [Licensing](#licensing) below.

## When to use it

WaveSyncDB is designed for:

- Offline-first desktop or mobile apps where each device should have the full dataset.
- **Small groups** — your own devices, a family, a team of collaborators (not thousands of unrelated clients).
- Apps where you want to avoid running a central API just to keep clients in sync.
- Projects already using SeaORM and SQLite.

It is **not** a good fit for:

- Multi-tenant SaaS with per-row access control (every authenticated peer can read/write everything).
- High-throughput ingest (>1000 writes/s sustained).
- Scenarios requiring a single authoritative central database.

## What's next

- [Quickstart](/docs/quickstart) — get a running app in under five minutes.
- [Architecture](/docs/architecture) — how writes propagate end to end.
- [Conflict resolution](/docs/conflict-resolution) — why per-column CRDTs converge.

## Licensing

WaveSyncDB is **dual-licensed**:

- **AGPL-3.0-or-later** — free for use in AGPL-compatible open-source projects. If your derivative work is itself released under AGPL, no further action is required. AGPL extends copyleft to network use: running a modified WaveSyncDB (or its relay) as a hosted service obliges you to offer the source to remote users.
- **Commercial license** — required if you are building proprietary, closed-source, or SaaS software and do not want to release your application source under the AGPL — including modifications to a network-facing relay or backend.

For commercial licensing, contact **pablo13vazquez@gmail.com**. Pricing is negotiated per agreement based on use, scale, and support level.

The full text lives in `LICENSE`, `LICENSE-AGPL`, and `LICENSE-COMMERCIAL` in the repository.
