# RELEASING.md — Release Policy

> When a release is warranted, how versions bump, and the compatibility state
> of every axis that can silently break a fleet. Update the matrix in §4 as
> part of every release. This file is force-added (`git add -f RELEASING.md`)
> because the repo gitignores `**/*.md`.

---

## 1. When to release

A release is warranted when **any** of these fires:

- **Milestone complete.** A phase or issue-cluster lands and the full suite is
  green. The milestone's content decides the bump size (§2), not the other way
  around.
- **Consumer pull.** A consuming app (Mediterranea) or the deployed relay
  needs something already merged to dev. Cut whatever is green, even
  mid-milestone.
- **Severity override.** A data-loss or security fix **forces** a release as
  soon as it is verified — it never waits for a batch.
- **Staleness prompt.** dev sitting more than ~4 weeks or ~50 commits ahead of
  the last tag is not itself a trigger, but it is a prompt to ask whether a
  milestone is actually done.

Never release: with a red or unverified-flaky suite, with unmerged feature
branches you intend to include, or on calendar pressure alone.

## 2. Version rules

All crates use pre-1.0 Cargo semver: `0.X.Y` — **X breaking, Y compatible**
(Cargo's resolver already treats 0.x minor bumps as breaking, so the numbers
mean what tooling thinks they mean). Crates version **independently**: bump
only what changed.

### wavesyncdb (embedded library)

**MINOR (`0.X`)** when any compatibility axis breaks:

| Axis | Example | Failure mode if fleets mix |
|---|---|---|
| Wire protocol | snapshot 3.0.0 → 4.0.0 | peers on different versions silently never sync |
| Key/topic derivation | Argon2id cutover, `wavesync2-` prefix | different key/topic ⇒ peers silently never pair |
| On-disk format | shadow schema, capture tables, meta-key semantics | old binary on new DB corrupts state or stalls sync |
| Public API | signatures, trait contracts, new variants in exhaustive-matched enums (`NetworkEvent`) | downstream compile breaks |
| Behavior contracts | `register_table` as sole shadow creator, WAL on managed connections | documented invariants consumers build on |

**PATCH (`0.x.Y`)** for everything else: compatible fixes, additive API,
internal changes.

**Mandatory banner:** a MINOR that touches the wire-protocol or derivation
axes must carry **"upgrade all peers together"** at the top of its notes —
mixed fleets do not error, they silently never pair/sync.

### wavesync_relay (deployed service)

**MINOR** for new or removed client-facing protocol handlers, or
ops-breaking changes (env-var renames, metric renames, storage migrations).
**PATCH** otherwise.

**Standing invariant — relay backward tolerance:** a new relay serves old
clients, and clients degrade gracefully (never break) against an old relay
(e.g. mailbox `UnsupportedProtocols` ⇒ reconcile-only delivery). A change
that would violate this needs explicit fleet coordination in the notes, not
just a bump.

### wavesyncdb_derive (proc-macro)

Moves **only** when its generated-code contract changes. **MINOR** if
generated code starts requiring new library items (bump the library's derive
dep floor in the same release); **PATCH** for additive emissions and fixes.

## 3. Release procedure

1. Full check suite green: `cargo fmt --check`; clippy `-D warnings` (default
   **and** `--features dioxus,push-sync`); lib + relay + derive tests; all
   integration suites `--test-threads=1`; `notifications`;
   `integration_dioxus --features dioxus`. Re-run known QUIC-timeout flakes in
   isolation before accepting them as flakes.
2. Bump only the crates that changed, plus dep refs (root workspace dep,
   `wavesyncdb`'s derive dep floor). Rebuild to refresh `Cargo.lock`.
3. Write notes **breaking-first**, then highlights. The notes live in the
   `chore(release): vX.Y.Z` commit body.
4. Annotated tag `vX.Y.Z` (one-line summary + the same notes).
5. Push: `git push origin dev --tags`.
6. GitHub Release from the tag: `gh release create vX.Y.Z --verify-tag
   --title "vX.Y.Z — <one-liner>" --notes-from-tag` (or paste the commit
   body).
7. **Deploy order:** relay first (Dokploy), then all clients together
   (Mediterranea rev bump — protocol/derivation changes make stragglers
   silently unpairable), then the web bundle rebuild.
8. Update the compatibility matrix (§4) and, if any rule here changed, the
   CLAUDE.md release section.

## 4. Compatibility matrix

Current axis values (update every release that moves one):

| Axis | Value | Since |
|---|---|---|
| Snapshot protocol | `/wavesync/snapshot/4.0.0` | 0.9.0 |
| Push protocol | `/wavesync/push/1.0.0` | 0.8.0 |
| Mailbox protocol | `/wavesync/mailbox/1.0.0` | 0.9.0 |
| Topic derivation | BLAKE3(user_topic + group_key), prefix `wavesync2-` | 0.9.0 |
| Group KDF | Argon2id 19 MiB, t=2, salt = user topic | 0.9.0 |
| Tombstone retention default | 7 days, wire-carried `deleted_ts` | 0.9.0 |
| SQLite mode (managed connections) | WAL, busy_timeout 5s, synchronous=NORMAL | 0.9.0 |

Client ↔ relay:

| | relay 0.3.x | relay 0.4.x |
|---|---|---|
| **lib 0.8.x** | full (no mailbox anywhere) | full (mailbox unused by client) |
| **lib 0.9.x** | **degraded**: mailbox unsupported ⇒ reconcile-only both-offline delivery | full |

Client ↔ client: **0.8.x and 0.9.x peers never pair** (snapshot protocol +
KDF/topic both changed). All peers of a group must be on the same minor.

## 5. History

| Version | Date | One-liner |
|---|---|---|
| v0.9.0 | 2026-07-20 | trigger capture, Argon2id keys, protocol 4.0.0, relay mailbox, web multi-group, iOS push round |
| v0.8.0 | 2026-07-03 | consolidation release: July hardening wave |
