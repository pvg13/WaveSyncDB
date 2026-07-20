# M1 — Minimize relay dependence

> GitHub milestone: https://github.com/pvg13/WaveSyncDB/milestone/1
> Force-added (`git add -f`) because the repo gitignores `**/*.md`.

## Objective

Reduce the relay's role to **introduction + last-resort durability** on every
network class — including mobile — and prove it with measurement, not
assumption. WaveSyncDB's premise is local-first sync with minimal centralized
infrastructure; this milestone makes that claim quantitative.

## KPIs and methodology

| KPI | Definition | Source | Baseline (2026-07-20) | Target |
|---|---|---|---|---|
| Relay payload ratio | relayed sync bytes / total sync bytes, per NAT class | client diagnostics `relay_bytes_*`/`direct_bytes_*` (counted at the payload verify/sign sites, classified by carrying connection); `relay_traffic_ratio()` | LAN: 0.000 · port-restricted-cone WAN: **0.000 measured** (`dcutr_validation`, 43 KB payload, 0 relayed) · symmetric NAT: **unknown** | set after P0 baseline; direction: as low as the NAT physically allows |
| Relay bytes per device-day | all relay-bound bytes incl. mailbox appends/drains and signaling | relay Prometheus (`relay_mailbox_*`, bandwidth counters) / device count; Grafana panel (P0 adds it) | unknown — P0 reads it off production | mailbox dial (#107) cuts steady-state appends by >80% for daily-overlapping fleets |
| Mobile wake efficiency | after a push wake, sync completes over a direct path when one is possible | wake-scenario diagnostics (`relayed_connections_established` vs `direct_…` in the wake window) | unknown — P1 measures per platform | no lingering relay circuits after a wake when direct is possible |

**Measurement caveats (pinned so numbers stay honest):**
- The payload ratio counts sync payload only. Mailbox traffic is AEAD-sealed
  and deliberately **excluded** from the ratio — it is tracked by the second
  KPI. Don't mix them.
- The 0.000 good-NAT result predates any P2 work: for common NAT shapes the
  introduction machinery already yields direct connections without DCUtR.
  The unknown, and the reason P0 comes first, is hostile NAT (symmetric /
  CGNAT / 464XLAT on cellular).

## Phases

### P0 — Measurement foundation
- **#51** symmetric-NAT harness shape (SNAT/DNAT-based) so DCUtR is actually
  exercised; byte-split readouts (landed in `dcutr_validation` 2026-07-20)
  extended across the relevant scenarios.
- Grafana: relay-payload-ratio + bytes/device-day panels on the wavesync-relay
  dashboard.
- Output: worst-case baseline table; KPI targets set from it.

### P1 — Mobile platform investigations (parallel; reports in docs/research/)
- **#108** Android: carrier NAT reality (CGNAT/464XLAT on 5G), Doze/App
  Standby vs live P2P sessions, FCM delivery classes under throttling, wake
  efficiency.
- **#109** iOS umbrella: #77 cellular DCUtR verdict, #74 NWPathMonitor, #73
  bind premise, silent-push budget reality, NSE wake paths. One hardware day;
  feeds #92 scheduling.

### P2 — Reducers, ordered by measured impact from P0/P1
- **#107** mailbox cost dial: fallback-only append after an ack threshold
  (both-offline guarantee preserved; steady-state appends → ~0 for
  overlapping fleets).
- **#31** AutoRelay: attach to the relay only when AutoNAT says Private;
  publicly reachable peers hold no reservation.
- **#87** peer-assisted gossip re-dissemination: design-stage; commit only if
  P0/P1 show relay fan-out is a real cost.

### P3 — Verification & exit
- Re-run the harness matrix (netem profiles × NAT shapes) with byte splits;
  before/after fleet numbers from Grafana; exit report appended to this doc.

## Acceptance

- [ ] Symmetric-NAT scenario exists; worst-case baseline documented here.
- [ ] KPI panels live on the relay dashboard.
- [ ] Android + iOS reports in docs/research/ with concrete verdicts;
      #73/#74/#77 closed or converted.
- [ ] #107 shipped with e2e proof both-offline delivery still converges.
- [ ] #31 shipped; publicly reachable peers hold no relay reservation.
- [ ] Measured KPI improvement recorded in this doc's exit section.

## Risks / scheduling

- **Mediterranea web v1 (2026-08-30)** competes for time; M1 is engine-side
  and parallelizable around it, but the two hardware days (#108 device half,
  #109) need calendar slots.
- Carrier NAT behavior varies by operator; the Android verdict should name
  the carriers tested and avoid overgeneralizing.
- #87 is deliberately gated on evidence — it is the largest design effort and
  the easiest to cut if the numbers say the relay fan-out cost is small.

## Issue roster

| Issue | Phase | State at kickoff |
|---|---|---|
| #51 symmetric-NAT harness shape | P0 | open (pre-existing) |
| #108 Android investigation | P1 | filed at kickoff |
| #109 iOS umbrella (#73/#74/#77, feeds #92) | P1 | filed at kickoff |
| #107 mailbox cost dial | P2 | filed at kickoff |
| #31 AutoRelay | P2 | open (pre-existing) |
| #87 gossip re-dissemination | P2 (gated) | open (pre-existing, design) |
