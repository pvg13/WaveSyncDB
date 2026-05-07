//! `/benchmarks` page — published wall-clock numbers from the netem
//! WAN harness in `tests-e2e/`.
//!
//! Numbers come from `cargo test -p wavesyncdb-e2e --test
//! wan_netem_profiles netem_profile_bench_full_matrix --
//! --ignored --nocapture --test-threads=1`. They're a snapshot of
//! engine behaviour at the commit listed below — re-run locally to
//! reproduce; numbers shift between CPUs and kernels but the
//! relative shape across profiles is stable.

use dioxus::prelude::*;

/// Commit/tag the published numbers were taken from. Update when
/// re-running the matrix.
const BENCH_REVISION: &str = "feat/nat-autonat-permissive @ 2026-05-07 — AMD x86-64, Linux 6.19";

struct ProfileRow {
    profile: &'static str,
    rtt_one_way: &'static str,
    jitter: &'static str,
    loss: &'static str,
    bandwidth: &'static str,
    first_sync_ms: &'static str,
    p50_ms: &'static str,
    p95_ms: &'static str,
    partition_ms: &'static str,
}

/// Profile-matrix results. Keep in sync with the most recent
/// `netem_profile_bench_full_matrix` run.
const PROFILE_ROWS: &[ProfileRow] = &[
    ProfileRow {
        profile: "ideal",
        rtt_one_way: "0 ms",
        jitter: "0 ms",
        loss: "0%",
        bandwidth: "unshaped",
        first_sync_ms: "16 ms",
        p50_ms: "16 ms",
        p95_ms: "22 ms",
        partition_ms: "2266 ms",
    },
    ProfileRow {
        profile: "lan_fast",
        rtt_one_way: "1 ms",
        jitter: "0 ms",
        loss: "0%",
        bandwidth: "unshaped",
        first_sync_ms: "23 ms",
        p50_ms: "69 ms",
        p95_ms: "71 ms",
        partition_ms: "505 ms",
    },
    ProfileRow {
        profile: "ethernet_gigabit",
        rtt_one_way: "1 ms",
        jitter: "0 ms",
        loss: "0%",
        bandwidth: "1 Gbps",
        first_sync_ms: "24 ms",
        p50_ms: "69 ms",
        p95_ms: "71 ms",
        partition_ms: "1776 ms",
    },
    ProfileRow {
        profile: "wifi_home",
        rtt_one_way: "5 ms",
        jitter: "1 ms",
        loss: "0%",
        bandwidth: "50 Mbps",
        first_sync_ms: "104 ms",
        p50_ms: "95 ms",
        p95_ms: "98 ms",
        partition_ms: "512 ms",
    },
    ProfileRow {
        profile: "wifi_busy",
        rtt_one_way: "15 ms",
        jitter: "10 ms",
        loss: "0.1%",
        bandwidth: "10 Mbps",
        first_sync_ms: "160 ms",
        p50_ms: "172 ms",
        p95_ms: "213 ms",
        partition_ms: "3619 ms",
    },
    ProfileRow {
        profile: "wifi_distant",
        rtt_one_way: "80 ms",
        jitter: "30 ms",
        loss: "1%",
        bandwidth: "2 Mbps",
        first_sync_ms: "798 ms",
        p50_ms: "616 ms",
        p95_ms: "824 ms",
        partition_ms: "2191 ms",
    },
    ProfileRow {
        profile: "mobile_5g",
        rtt_one_way: "15 ms",
        jitter: "5 ms",
        loss: "0.1%",
        bandwidth: "100 Mbps",
        first_sync_ms: "216 ms",
        p50_ms: "167 ms",
        p95_ms: "198 ms",
        partition_ms: "992 ms",
    },
    ProfileRow {
        profile: "cellular_fair",
        rtt_one_way: "40 ms",
        jitter: "20 ms",
        loss: "0.5%",
        bandwidth: "5 Mbps",
        first_sync_ms: "544 ms",
        p50_ms: "357 ms",
        p95_ms: "519 ms",
        partition_ms: "3529 ms",
    },
    ProfileRow {
        profile: "cellular_bad",
        rtt_one_way: "200 ms",
        jitter: "100 ms",
        loss: "3%",
        bandwidth: "1 Mbps",
        first_sync_ms: "1973 ms",
        p50_ms: "1761 ms",
        p95_ms: "2718 ms",
        partition_ms: "4098 ms",
    },
    ProfileRow {
        profile: "mobile_3g",
        rtt_one_way: "100 ms",
        jitter: "30 ms",
        loss: "1%",
        bandwidth: "384 kbps",
        first_sync_ms: "759 ms",
        p50_ms: "769 ms",
        p95_ms: "1497 ms",
        partition_ms: "1075 ms",
    },
    ProfileRow {
        profile: "satellite",
        rtt_one_way: "600 ms",
        jitter: "50 ms",
        loss: "0.5%",
        bandwidth: "1 Mbps",
        first_sync_ms: "4607 ms",
        p50_ms: "3813 ms",
        p95_ms: "5046 ms",
        partition_ms: ">240 s †",
    },
    ProfileRow {
        profile: "lossy_lan",
        rtt_one_way: "1 ms",
        jitter: "0 ms",
        loss: "5%",
        bandwidth: "unshaped",
        first_sync_ms: "26 ms",
        p50_ms: "71 ms",
        p95_ms: "1111 ms ‡",
        partition_ms: "2695 ms",
    },
];

struct ScenarioRow {
    scenario: &'static str,
    profile: &'static str,
    recovery_ms: &'static str,
    recovered: &'static str,
    note: &'static str,
}

const SCENARIO_ROWS: &[ScenarioRow] = &[
    ScenarioRow {
        scenario: "long_offline_30s",
        profile: "cellular_fair",
        recovery_ms: "5472 ms",
        recovered: "10/10",
        note: "Bob writes 10 rows during 30s blackhole; Alice catches up after heal.",
    },
    ScenarioRow {
        scenario: "long_offline_30s",
        profile: "cellular_bad",
        recovery_ms: "186198 ms ‡",
        recovered: "9/10",
        note: "1 of 10 writes fails to converge under heavy loss + long offline. Tracked as #39.",
    },
    ScenarioRow {
        scenario: "container_restart",
        profile: "lan_fast",
        recovery_ms: "10802 ms",
        recovered: "5/5",
        note: "docker stop+start of Alice; cached peer-addrs (#29) drives reconnect.",
    },
    ScenarioRow {
        scenario: "container_restart",
        profile: "cellular_fair",
        recovery_ms: "10296 ms",
        recovered: "5/5",
        note: "Same scenario on cellular — restart cost dominates the network cost.",
    },
    ScenarioRow {
        scenario: "asymmetric",
        profile: "alice=cellular_bad, bob=wifi",
        recovery_ms: "1944 ms",
        recovered: "40/40",
        note: "p95 across 40 writes (20 each direction). Convergence gated by the worse peer.",
    },
];

#[component]
pub fn Benchmarks() -> Element {
    rsx! {
        section { class: "page-header",
            div { class: "section-inner",
                h1 { class: "page-title", "Benchmarks" }
                p { class: "page-subtitle",
                    "Wall-clock measurements from the netem-driven WAN harness, across 12 simulated network profiles + 5 reliability scenarios. Numbers below are a snapshot — the entire matrix runs locally in ~25 minutes."
                }
            }
        }

        section { class: "section",
            div { class: "section-inner",
                h2 { "Methodology" }
                p {
                    "Two test-peer Docker containers run on a per-test bridge network with a real "
                    code { "wavesync-relay" }
                    " between them. Linux "
                    code { "tc netem" }
                    " inside each peer container shapes egress traffic to match a named profile (RTT, jitter, packet loss, bandwidth cap). For each profile we measure:"
                }
                ul {
                    li { strong { "Time-to-first-sync" } " — wall-clock from harness "
                        code { "start()" }
                        " until the first row written on Alice arrives on Bob. Captures cold-start discovery + circuit-relay reservation + first push."
                    }
                    li { strong { "Steady-state p50 / p95" } " — Alice writes 50 rows; we time Alice-insert to Bob-receive for each. Median + 95th percentile across the run."
                    }
                    li { strong { "Partition recovery" } " — apply 100% packet loss on Alice's interface for 5 seconds, restore the original profile, measure the time from heal until a write made via Bob during the blackhole reaches Alice."
                    }
                }
                p { class: "muted",
                    "Source: "
                    code { "tests-e2e/tests/wan_netem_profiles.rs" }
                    ". Run with "
                    code { "cargo test -p wavesyncdb-e2e --test wan_netem_profiles netem_profile_bench_full_matrix -- --ignored --nocapture --test-threads=1" }
                    "."
                }
            }
        }

        section { class: "section",
            div { class: "section-inner",
                h2 { "Profile matrix" }
                p { class: "muted", "Snapshot from " code { "{BENCH_REVISION}" } "." }
                div { class: "bench-table-wrapper",
                    table { class: "bench-table",
                        thead {
                            tr {
                                th { "Profile" }
                                th { "RTT (one-way)" }
                                th { "Jitter" }
                                th { "Loss" }
                                th { "Bandwidth" }
                                th { "First sync" }
                                th { "p50" }
                                th { "p95" }
                                th { "Partition recovery" }
                            }
                        }
                        tbody {
                            for r in PROFILE_ROWS.iter() {
                                tr {
                                    td { code { "{r.profile}" } }
                                    td { "{r.rtt_one_way}" }
                                    td { "{r.jitter}" }
                                    td { "{r.loss}" }
                                    td { "{r.bandwidth}" }
                                    td { "{r.first_sync_ms}" }
                                    td { "{r.p50_ms}" }
                                    td { "{r.p95_ms}" }
                                    td { "{r.partition_ms}" }
                                }
                            }
                        }
                    }
                }
                p { class: "muted",
                    "† satellite partition recovery exceeded the 240 s test cap on this run. Earlier runs recorded ~4.5 s — the 1.2 s RTT amplifies any retry cycle, so partition-recovery on satellite has high run-to-run variance."
                }
                p { class: "muted",
                    "‡ "
                    code { "lossy_lan" }
                    "'s p95 is 15× its p50 — 5% packet loss inflates the long tail without affecting the median, suggesting retransmission backoff is more conservative than necessary on lossy paths. Tracked as #38."
                }
            }
        }

        section { class: "section",
            div { class: "section-inner",
                h2 { "Extended scenarios" }
                p {
                    "Beyond steady-state propagation, the harness exercises specific failure shapes:"
                }
                div { class: "bench-table-wrapper",
                    table { class: "bench-table",
                        thead {
                            tr {
                                th { "Scenario" }
                                th { "Profile" }
                                th { "Recovery" }
                                th { "Recovered / total" }
                                th { "What it tests" }
                            }
                        }
                        tbody {
                            for r in SCENARIO_ROWS.iter() {
                                tr {
                                    td { code { "{r.scenario}" } }
                                    td { "{r.profile}" }
                                    td { "{r.recovery_ms}" }
                                    td { "{r.recovered}" }
                                    td { class: "bench-table-note", "{r.note}" }
                                }
                            }
                        }
                    }
                }
            }
        }

        section { class: "section",
            div { class: "section-inner",
                h2 { "DCUtR (direct-connection upgrade) status" }
                p {
                    "The engine ships with libp2p's "
                    code { "dcutr" }
                    " behaviour wired in (issue #40). Runtime counters "
                    code { "dcutr_upgrades_attempted" }
                    " / "
                    code { "_succeeded" }
                    " on the diagnostics snapshot quantify how often hole-punching wins under the network conditions an engine actually faces. Local Docker-bridge benches don't yet exercise DCUtR end-to-end (the simulated NAT shapes either let peers connect directly or block AutoNAT verification — see issue #51 for the "
                    code { "SymmetricNat" }
                    " profile that would force DCUtR coordination). On real-world cellular pairs the expected success rate is 10–30%; on home / office NAT, ~70%."
                }
            }
        }

        section { class: "section",
            div { class: "section-inner",
                h2 { "Run it yourself" }
                pre { code {
"./tests-e2e/build-images.sh   # one-time, pulls iproute2 + iptables
cargo test -p wavesyncdb-e2e --test wan_netem_profiles \\
    netem_profile_bench_full_matrix \\
    -- --ignored --nocapture --test-threads=1"
                } }
                p { class: "muted",
                    "Requires Docker + a Linux host (kernel netem). Macs and Windows users run the harness via Docker Desktop's Linux VM. Numbers depend on host CPU and kernel version — they're for relative comparison across engine changes, not for absolute SLA-style targets."
                }
            }
        }
    }
}
