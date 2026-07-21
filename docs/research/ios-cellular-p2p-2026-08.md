# iOS mobile connectivity day — investigation (#109, M1/P1)

> Status: PREPARED, awaiting the hardware day (two iPhones). This file is
> the protocol + findings container; base device mechanics live in
> docs/ios-device-protocol-2026-07.md (Sections C–E).

## Objective

One hardware day resolving the open iOS connectivity cluster with device
evidence: #77 (cellular DCUtR / relay-only sessions), #74 (NWPathMonitor
vs 3 s polling), #73 (if_watch hang premise / QUIC bind simplification),
plus M1's carrier-NAT classification and the push-budget reality. Feeds
#92 scheduling if time allows.

## Session plan (ordered so early failures don't block later items)

### S1 — Carrier NAT classification (M1; mirrors Android's protocol)

Same two-run structure as the Android doc: iPhone A cellular-only vs
(a) iPhone B same-carrier cellular, (b) iPhone B home WiFi. Use the demo
app; classification signals are the same (`PeerInfo.via_relay`, direct
connections, relay ratio — read via the app's status surface or the
Console.app engine logs; the m1-diag beacon is Android-side only, iOS
reads the same counters through the debug status view).

Record: carrier, radio (5G/LTE), per-phone verdict.

### S2 — #77: relay-only sessions + cellular DCUtR

With the phones in whatever state S1 found: if circuits persist
(symmetric-like), confirm sync completes relay-only (writes both ways,
convergence). Note any `dcutr` terminal outcomes in logs — remembering
the #110 finding: `attempted` counts only terminal outcomes; zero over a
short window proves nothing about engagement.

### S3 — #73: bind premise A/B

`WAVESYNC_IOS_UNSPECIFIED_QUIC` toggle (both values), per
docs/ios-device-protocol-2026-07.md Section C. Verdict decides #74's
scope and lets the losing bind path be deleted.

### S4 — #74 input: interface-change latency

On the winning bind: WiFi↔cellular flips while foregrounded; measure
re-listen/reconnect latency with the current 3 s polling. This is the
baseline NWPathMonitor would have to beat.

### S5 — Push-budget reality (M1)

Over the day's natural use: count silent pushes sent (relay logs) vs
delivered (device logs), foreground/background/killed. Bounds how much
wake-dependence can be engineered away.

### S6 — Suspension vs live P2P (M1)

Foreground sync session → home button → measure how long the direct QUIC
session survives suspension (peer-side observation: when does the
connection die) and whether resume re-establishes direct or degrades to
circuits.

## Findings

_(per section, pending the hardware day)_

## Verdicts

_(to fill: #73/#74/#77 outcomes; iOS rows in the M1 KPI table; NSE wake
implications for #92)_
