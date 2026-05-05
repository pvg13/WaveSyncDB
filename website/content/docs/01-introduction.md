# Introduction

**WaveSyncDB** is a transparent peer-to-peer sync layer for SeaORM applications.

You write your app the way you always have — `#[derive(DeriveEntityModel)]` entities, `ActiveModel::insert(&db)`, the usual SeaORM idioms — and WaveSyncDB makes every write replicate to every peer without changing a line of business logic. Conflicts are resolved automatically using per-column Lamport clocks, so concurrent edits to different columns of the same row both survive and every peer converges to the same final state.

## What you get

- A drop-in **`WaveSyncDb`** that implements SeaORM's `ConnectionTrait`. Swap your `DatabaseConnection` for it and continue writing normal SeaORM code.

> **License**: WaveSyncDB is dual-licensed. **AGPL-3.0-or-later** is free for use in AGPL-compatible open-source projects. A separate **commercial license** is available for proprietary, closed-source, or SaaS products. See [Licensing](#licensing) at the bottom of this page.

- **Per-column conflict resolution** — concurrent updates to different columns survive; same-column conflicts resolve deterministically using `(col_version → value bytes → site_id)` total ordering.
- **Local-first by design** — every write hits SQLite first, so the UI never blocks on the network. Sync is opportunistic in the background.
- **P2P over libp2p** — mDNS for the LAN, AutoNAT + circuit relay for WAN, DCUtR hole-punching for direct connections when both sides are reachable.
- **First-class mobile** — a relay server can fan out silent FCM and APNs pushes that wake sleeping phones so they catch up within seconds of a desktop write.
- **Group authentication** — a shared passphrase derives the topic and signs every message via HMAC-BLAKE3. Anything unauthenticated is silently dropped.

## When to use it

WaveSyncDB is a good fit when:

- You want offline-first behaviour for a desktop or mobile app.
- The data is **owned by a small group** of users (collaborators, family, your own devices) rather than being public/multi-tenant.
- You'd rather avoid running a central API tier just to keep a few clients in sync.
- You're already happy with SeaORM and SQLite.

It is not a good fit for fan-out to thousands of unrelated clients, for adversarial multi-tenant scenarios, or for cases where you need a single authoritative central database.

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
