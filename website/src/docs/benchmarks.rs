use dioxus::prelude::*;
use super::{CodeBlock, H2, H3};

#[component]
pub fn Page() -> Element {
    rsx! {
        h1 { "Benchmarks" }
        p {
            "Real measurements from the WaveSyncDB test suite, run on a 13th Gen Intel i7-13700KF. "
            "These numbers show what sync costs and what it saves you."
        }

        // ── Performance ──
        H2 { id: "performance", text: "Performance" }

        H3 { id: "write-overhead", text: "Write interception overhead" }
        p {
            "Every write goes through SQL parsing, shadow table updates, and changeset creation. "
            "This is the cost of making a write syncable."
        }
        div { class: "bench-table",
            table {
                thead {
                    tr {
                        th { "Operation" }
                        th { "Latency" }
                        th { "Notes" }
                    }
                }
                tbody {
                    tr {
                        td { "Raw SQLite INSERT" }
                        td { class: "num", "43 \u{00B5}s" }
                        td { "Direct SeaORM, no sync" }
                    }
                    tr {
                        td { "WaveSyncDb INSERT" }
                        td { class: "num", "286 \u{00B5}s" }
                        td { "SQL parse + shadow upsert + changeset" }
                    }
                    tr {
                        td { "Overhead" }
                        td { class: "num accent", "~243 \u{00B5}s/write" }
                        td { "Per-write cost of sync" }
                    }
                }
            }
        }

        p {
            "The overhead breaks down into:"
        }
        ul {
            li { "SQL parsing (classify + extract columns): ~0.5 \u{00B5}s" }
            li { "Shadow table upsert (INSERT OR REPLACE per column): ~46 \u{00B5}s/column" }
            li { "db_version increment + persist: ~45 \u{00B5}s" }
            li { "Changeset creation + channel send: ~5 \u{00B5}s" }
            li { "SeaORM overhead (statement preparation): rest" }
        }

        H3 { id: "reads", text: "Read performance" }
        p { "Reads bypass the sync interceptor entirely \u{2014} zero overhead." }
        div { class: "bench-table",
            table {
                thead {
                    tr {
                        th { "Operation" }
                        th { "Latency" }
                    }
                }
                tbody {
                    tr {
                        td { "SELECT * FROM tasks (100 rows)" }
                        td { class: "num", "98 \u{00B5}s" }
                    }
                }
            }
        }

        H3 { id: "sync-latency", text: "Sync propagation" }
        p { "Time from a write on peer A to the data being visible on peer B (same machine, mDNS)." }
        div { class: "bench-table",
            table {
                thead {
                    tr {
                        th { "Metric" }
                        th { "Time" }
                    }
                }
                tbody {
                    tr {
                        td { "Peer discovery (mDNS)" }
                        td { class: "num", "~100 ms" }
                    }
                    tr {
                        td { "Write \u{2192} remote visible" }
                        td { class: "num accent", "~11 ms" }
                    }
                }
            }
        }
        p {
            "11ms includes: shadow table write, changeset serialization, libp2p request-response "
            "round-trip, HMAC verification, conflict resolution, remote SQL apply, and shadow update on the receiver."
        }

        H3 { id: "throughput", text: "Write throughput" }
        div { class: "bench-table",
            table {
                thead {
                    tr {
                        th { "Batch size" }
                        th { "Total time" }
                        th { "Per write" }
                        th { "Throughput" }
                    }
                }
                tbody {
                    tr {
                        td { "10 writes" }
                        td { class: "num", "4.2 ms" }
                        td { class: "num", "422 \u{00B5}s" }
                        td { class: "num", "2,367 ops/s" }
                    }
                    tr {
                        td { "50 writes" }
                        td { class: "num", "17 ms" }
                        td { class: "num", "340 \u{00B5}s" }
                        td { class: "num", "2,940 ops/s" }
                    }
                    tr {
                        td { "100 writes" }
                        td { class: "num", "32 ms" }
                        td { class: "num", "319 \u{00B5}s" }
                        td { class: "num", "3,133 ops/s" }
                    }
                    tr {
                        td { "500 writes" }
                        td { class: "num", "144 ms" }
                        td { class: "num", "288 \u{00B5}s" }
                        td { class: "num accent", "3,473 ops/s" }
                    }
                }
            }
        }
        p {
            "Throughput improves with batch size due to amortized connection and lock costs. "
            "At 500 writes, the engine sustains ~3,400 synced writes per second on a single core."
        }

        H3 { id: "conflict", text: "Conflict resolution" }
        div { class: "bench-table",
            table {
                thead {
                    tr {
                        th { "Operation" }
                        th { "Throughput" }
                    }
                }
                tbody {
                    tr {
                        td { "should_apply_column (3-level tiebreak)" }
                        td { class: "num accent", "~4 million/s" }
                    }
                }
            }
        }
        p { "Conflict resolution is pure CPU \u{2014} no I/O, no allocation. Never a bottleneck." }

        // ── What you don't have to build ──
        H2 { id: "developer-effort", text: "What WaveSyncDB replaces" }
        p {
            "Building the equivalent sync infrastructure from scratch requires significant "
            "engineering effort across multiple domains. Here's what WaveSyncDB gives you out of the box."
        }

        div { class: "comparison-grid",
            ComparisonCard {
                title: "Sync server",
                without: "Design API, handle auth, manage state, deploy, monitor, scale. \
                          ~2,000\u{2013}5,000 lines of backend code + infrastructure.",
                with_wavesync: "No server needed. Peers sync directly. Optional relay for NAT traversal \
                                (single static binary, no database)."
            }
            ComparisonCard {
                title: "Conflict resolution",
                without: "Choose a strategy (LWW, OT, CRDT). Implement merge logic per field. \
                          Write tests for every concurrent edit scenario. ~500\u{2013}1,500 lines.",
                with_wavesync: "Built-in per-column Lamport clocks with deterministic 3-level tiebreak. \
                                Zero application code. Tested with 139+ unit tests."
            }
            ComparisonCard {
                title: "Offline queue",
                without: "Buffer writes locally, track sync state, retry on reconnect, \
                          handle partial failures. ~500\u{2013}1,000 lines.",
                with_wavesync: "Writes go to SQLite immediately. Version vectors track what each \
                                peer has seen. Catch-up sync fills gaps automatically on reconnect."
            }
            ComparisonCard {
                title: "Push notifications",
                without: "Set up FCM + APNs credentials, build notification service, \
                          handle token refresh, implement background sync handler. ~800\u{2013}2,000 lines across 3 languages.",
                with_wavesync: "One function call: initWaveSyncFCM(). Token registration, refresh, \
                                background wake-up, and cold sync are handled automatically."
            }
            ComparisonCard {
                title: "Peer discovery",
                without: "Build peer registry, implement heartbeats, handle NAT traversal \
                          (STUN/TURN or relay). ~1,000\u{2013}3,000 lines + infrastructure.",
                with_wavesync: "mDNS for LAN (zero config). Relay + rendezvous for WAN. \
                                AutoNAT detection and circuit relay built in."
            }
            ComparisonCard {
                title: "Real-time updates",
                without: "WebSocket server, connection management, pub/sub per table, \
                          reconnection logic. ~500\u{2013}1,500 lines.",
                with_wavesync: "Every write fans out to connected peers in real-time (~11ms). \
                                Change notifications bridge to reactive UI frameworks."
            }
        }

        H3 { id: "summary", text: "Summary" }
        div { class: "bench-table",
            table {
                thead {
                    tr {
                        th { "" }
                        th { "Without WaveSyncDB" }
                        th { "With WaveSyncDB" }
                    }
                }
                tbody {
                    tr {
                        td { "Backend code" }
                        td { class: "num", "5,000\u{2013}15,000 lines" }
                        td { class: "num accent", "0 lines" }
                    }
                    tr {
                        td { "Infrastructure" }
                        td { class: "num", "API server + database + push service" }
                        td { class: "num accent", "Optional relay (static binary)" }
                    }
                    tr {
                        td { "Client sync code" }
                        td { class: "num", "1,000\u{2013}3,000 lines" }
                        td { class: "num accent", "~10 lines (init + register)" }
                    }
                    tr {
                        td { "Conflict handling" }
                        td { class: "num", "Custom per-field logic" }
                        td { class: "num accent", "Automatic (CRDT)" }
                    }
                    tr {
                        td { "Ongoing maintenance" }
                        td { class: "num", "Server ops, schema migrations, scaling" }
                        td { class: "num accent", "None (peer-to-peer)" }
                    }
                }
            }
        }

        H3 { id: "tradeoffs", text: "Tradeoffs" }
        p { "WaveSyncDB is not free \u{2014} here's what it costs:" }
        ul {
            li { "Write latency: ~243\u{00B5}s overhead per write (shadow table + CRDT bookkeeping)" }
            li { "Storage: shadow tables add ~100 bytes per column per row" }
            li { "Binary size: the Rust engine adds ~18MB to your Android APK (native .so)" }
            li { "No server authority: all peers are equal. No central \"source of truth\" to query." }
            li { "SQLite only: not compatible with PostgreSQL, MySQL, or other databases" }
        }

        CodeBlock { html: BENCH_RUN }
    }
}

#[component]
fn ComparisonCard(title: &'static str, without: &'static str, with_wavesync: &'static str) -> Element {
    rsx! {
        div { class: "comparison-card",
            h4 { "{title}" }
            div { class: "comparison-row",
                div { class: "comparison-col without",
                    span { class: "comparison-label", "Without" }
                    p { "{without}" }
                }
                div { class: "comparison-col with",
                    span { class: "comparison-label", "With WaveSyncDB" }
                    p { "{with_wavesync}" }
                }
            }
        }
    }
}

const BENCH_RUN: &str = r##"<span class="cmt"># Run benchmarks yourself</span>
<span class="prompt">$ </span>cargo bench --bench sync_benchmarks"##;
