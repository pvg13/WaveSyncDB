use dioxus::prelude::*;
use crate::Route;

#[component]
pub fn Home() -> Element {
    rsx! {
        // ── Hero ──
        section { class: "hero",
            h1 {
                "P2P sync for " span { class: "highlight", "SQLite" }
            }
            p { class: "subtitle",
                "A Rust library that adds peer-to-peer replication to SQLite. \
                 Writes on one device propagate to all connected peers automatically. \
                 Per-column CRDTs resolve conflicts deterministically \
                 without any application code."
            }
            div { class: "hero-buttons",
                Link { to: Route::DocsOverview {}, class: "btn-primary", "Get Started" }
                a {
                    href: "https://github.com/pvg13/wavesyncdb",
                    target: "_blank",
                    class: "btn-secondary",
                    "View on GitHub"
                }
            }
        }

        // ── Features ──
        section { class: "features",
            p { class: "section-label", "Features" }
            h2 { class: "section-title", "What it does" }
            p { class: "section-subtitle",
                "WaveSyncDB wraps a SeaORM database connection and intercepts every write. \
                 Column-level CRDT metadata is tracked in shadow tables, and changes are \
                 exchanged with peers over libp2p."
            }

            div { class: "features-grid",
                FeatureCard {
                    icon: "\u{1F4E1}",
                    icon_class: "blue",
                    title: "Peer-to-peer sync",
                    desc: "Devices discover each other via mDNS on local networks. \
                           For WAN connectivity, a lightweight relay server provides \
                           NAT traversal and peer discovery via rendezvous."
                }
                FeatureCard {
                    icon: "\u{1F6E1}",
                    icon_class: "green",
                    title: "Per-column conflict resolution",
                    desc: "Each column has its own Lamport clock. Concurrent edits \
                           to different columns on the same row both survive. \
                           Ties are broken deterministically by value bytes then site ID."
                }
                FeatureCard {
                    icon: "\u{26A1}",
                    icon_class: "purple",
                    title: "Write interception",
                    desc: "WaveSyncDb implements SeaORM's ConnectionTrait. All four \
                           SQL execution methods are intercepted — INSERTs, UPDATEs, \
                           and DELETEs are parsed and dispatched to the sync engine."
                }
                FeatureCard {
                    icon: "\u{1F4F1}",
                    icon_class: "orange",
                    title: "Mobile support",
                    desc: "Android lifecycle detection, FCM push wake-up for background sync, \
                           and cold sync when the app is killed. iOS support via Dioxus, \
                           with React Native iOS in progress."
                }
                FeatureCard {
                    icon: "\u{1F510}",
                    icon_class: "blue",
                    title: "Authenticated sync groups",
                    desc: "Sync groups are authenticated with HMAC derived from a shared \
                           passphrase via BLAKE3. Peers with mismatched passphrases are \
                           permanently rejected. Transport is encrypted via libp2p Noise."
                }
                FeatureCard {
                    icon: "\u{1F310}",
                    icon_class: "green",
                    title: "Dioxus and React Native",
                    desc: "Reactive hooks for Dioxus (use_synced_table, use_network_status). \
                           React Native native module for Android with UniFFI-generated \
                           Kotlin bindings. iOS React Native bindings are in development."
                }
            }
        }

        // ── Architecture ──
        section { class: "how-it-works",
            p { class: "section-label", "Architecture" }
            h2 { class: "section-title", "Three layers" }
            p { class: "section-subtitle",
                "The core engine parses raw SQL and manages CRDTs. Framework \
                 integrations wrap it for ergonomic use in each ecosystem."
            }

            div { class: "arch-stack",
                // Layer 3: Reactive
                div { class: "arch-layer",
                    div { class: "arch-layer-label", "Layer 3 \u{00B7} Reactive UI (optional)" }
                    div { class: "arch-layer-cards",
                        div { class: "arch-card purple",
                            h4 { "Dioxus Hooks" }
                            ul {
                                li { "use_synced_table \u{2014} auto-refreshing signal" }
                                li { "use_network_status \u{2014} live peer count" }
                            }
                        }
                        div { class: "arch-card purple",
                            h4 { "WatermelonDB" }
                            ul {
                                li { "query().observe() \u{2014} reactive queries" }
                                li { "Remote CRDT events bridge automatically" }
                            }
                        }
                    }
                }

                div { class: "arch-arrow", "\u{2193}" }

                // Layer 2: Connection wrappers
                div { class: "arch-layer",
                    div { class: "arch-layer-label", "Layer 2 \u{00B7} Connection Wrappers" }
                    div { class: "arch-layer-cards",
                        div { class: "arch-card blue",
                            h4 { "SeaORM ConnectionTrait" }
                            ul {
                                li { "Rust / Dioxus apps" }
                                li { "Replace DatabaseConnection with WaveSyncDb" }
                                li { "All ORM operations go through sync" }
                            }
                        }
                        div { class: "arch-card blue",
                            h4 { "UniFFI Native Module" }
                            ul {
                                li { "React Native (Android + iOS)" }
                                li { "execute(sql) and query(sql)" }
                                li { "Same Rust SQL parser under the hood" }
                            }
                        }
                    }
                }

                div { class: "arch-arrow", "\u{2193}" }

                // Layer 1: Core engine
                div { class: "arch-layer",
                    div { class: "arch-layer-label", "Layer 1 \u{00B7} Core Engine (Rust)" }
                    div { class: "arch-layer-cards",
                        div { class: "arch-card green wide",
                            ul {
                                li { "Parses INSERT / UPDATE / DELETE from raw SQL" }
                                li { "Tracks per-column Lamport clocks in shadow tables" }
                                li { "Creates CRDT changesets and sends to peers via libp2p" }
                                li { "Discovers peers via mDNS (LAN) or relay server (WAN)" }
                                li { "Resolves conflicts deterministically across all peers" }
                            }
                        }
                    }
                }
            }
        }

        // ── Code examples ──
        section { class: "code-section",
            p { class: "section-label", "Examples" }
            h2 { class: "section-title", "See it in action" }

            div { class: "code-tabs",
                div { class: "code-tab",
                    h3 { class: "code-tab-title rust", "Rust / Dioxus" }
                    CodeBlock { html: CODE_DIOXUS }
                }
                div { class: "code-tab",
                    h3 { class: "code-tab-title rn", "React Native / WatermelonDB" }
                    CodeBlock { html: CODE_RN }
                }
            }
        }

        // ── Platforms ──
        section { class: "platforms",
            p { class: "section-label", "Platform support" }
            h2 { class: "section-title", "Devices \u{00D7} Frameworks" }

            div { class: "platform-matrix",
                // Devices column
                div { class: "platform-col",
                    h3 { class: "platform-col-title", "Devices" }
                    PlatformRow { svg: SVG_ANDROID, name: "Android", status: "stable" }
                    PlatformRow { svg: SVG_APPLE, name: "iOS", status: "in progress" }
                    PlatformRow { svg: SVG_DESKTOP, name: "Desktop", status: "stable" }
                    PlatformRow { svg: SVG_WEB, name: "Web", status: "planned" }
                }

                // Divider
                div { class: "platform-divider" }

                // Frameworks column
                div { class: "platform-col",
                    h3 { class: "platform-col-title", "Frameworks" }
                    PlatformRow { svg: SVG_DIOXUS, name: "Dioxus", status: "stable" }
                    PlatformRow { svg: SVG_REACT, name: "React Native", status: "stable" }
                    PlatformRow { svg: SVG_WATERMELON, name: "WatermelonDB", status: "stable" }
                    PlatformRow { svg: SVG_SQL, name: "Raw SQL / FFI", status: "stable" }
                }
            }
        }

        // ── CTA ──
        section { class: "cta",
            h2 { "Try it out" }
            p { "Read the docs or check out the example apps." }
            div { class: "hero-buttons",
                Link { to: Route::DocsOverview {}, class: "btn-primary", "Read the Docs" }
                Link { to: Route::DocsInstallation {}, class: "btn-secondary", "Installation" }
            }
        }

        // ── Footer ──
        footer {
            p { "WaveSyncDB \u{00B7} P2P SQLite replication in Rust" }
        }
    }
}

#[component]
fn FeatureCard(icon: &'static str, icon_class: &'static str, title: &'static str, desc: &'static str) -> Element {
    rsx! {
        div { class: "feature-card",
            div { class: "feature-icon {icon_class}", "{icon}" }
            h3 { "{title}" }
            p { "{desc}" }
        }
    }
}

#[component]
fn Step(number: &'static str, title: &'static str, desc: &'static str) -> Element {
    rsx! {
        div { class: "step",
            div { class: "step-number", "{number}" }
            h3 { "{title}" }
            p { "{desc}" }
        }
    }
}

#[component]
fn PlatformRow(svg: &'static str, name: &'static str, status: &'static str) -> Element {
    let status_class = match status {
        "stable" => "status-stable",
        "in progress" => "status-wip",
        _ => "status-planned",
    };
    rsx! {
        div { class: "platform-row",
            span { class: "platform-icon", dangerous_inner_html: "{svg}" }
            span { class: "platform-name", "{name}" }
            span { class: "platform-status {status_class}", "{status}" }
        }
    }
}

#[component]
fn CodeBlock(html: &'static str) -> Element {
    rsx! {
        div { class: "code-block", dangerous_inner_html: "{html}" }
    }
}

const CODE_DIOXUS: &str = r##"<span class="cmt">// Build and connect</span>
<span class="kw">let</span> db = <span class="fn">WaveSyncDbBuilder</span>::<span class="fn">new</span>(<span class="str">"sqlite:app.db?mode=rwc"</span>, <span class="str">"my-topic"</span>)
    .<span class="fn">with_passphrase</span>(<span class="str">"shared-secret"</span>)
    .<span class="fn">build</span>().<span class="kw">await</span>?;

<span class="cmt">// Register SeaORM entities</span>
db.<span class="fn">schema</span>().<span class="fn">register</span>(task::Entity).<span class="fn">sync</span>().<span class="kw">await</span>?;

<span class="cmt">// Write — synced to all peers automatically</span>
task::ActiveModel {
    id: <span class="fn">Set</span>(<span class="str">"task-1"</span>.<span class="fn">into</span>()),
    title: <span class="fn">Set</span>(<span class="str">"Buy milk"</span>.<span class="fn">into</span>()),
    ..Default::default()
}.<span class="fn">insert</span>(&amp;db).<span class="kw">await</span>?;

<span class="cmt">// Reactive hook — re-renders on local + remote changes</span>
<span class="kw">let</span> tasks = <span class="fn">use_synced_table</span>::&lt;task::Entity&gt;(db);</span>"##;

const CODE_RN: &str = r##"<span class="cmt">// Setup adapter</span>
<span class="kw">const</span> adapter = <span class="kw">new</span> <span class="fn">WaveSyncAdapter</span>({
  schema, topic: <span class="str">'my-topic'</span>, passphrase: <span class="str">'shared-secret'</span>,
});
<span class="kw">const</span> database = <span class="kw">new</span> <span class="fn">Database</span>({ adapter, modelClasses: [Task] });
adapter.<span class="fn">setDatabase</span>(database);

<span class="cmt">// Write — synced to all peers automatically</span>
<span class="kw">await</span> database.<span class="fn">write</span>(<span class="kw">async</span> () =&gt; {
  <span class="kw">await</span> database.<span class="fn">get</span>(<span class="str">'tasks'</span>).<span class="fn">create</span>(t =&gt; {
    t.title = <span class="str">'Buy milk'</span>;
  });
});

<span class="cmt">// Reactive — re-renders on local + remote changes</span>
database.<span class="fn">get</span>(<span class="str">'tasks'</span>).<span class="fn">query</span>().<span class="fn">observe</span>()
  .<span class="fn">subscribe</span>(tasks =&gt; <span class="fn">setTasks</span>(tasks));"##;

// ── Platform SVG logos (20x20, currentColor) ──

const SVG_ANDROID: &str = r#"<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="11" width="14" height="10" rx="2"/><path d="M8 11V7a4 4 0 018 0v4"/><circle cx="9" cy="7" r=".5" fill="currentColor"/><circle cx="15" cy="7" r=".5" fill="currentColor"/><path d="M5 15H3"/><path d="M21 15h-2"/><path d="M9 21v2"/><path d="M15 21v2"/><path d="M7 3L5 1"/><path d="M17 3l2-2"/></svg>"#;

const SVG_APPLE: &str = r#"<svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor"><path d="M18.71 19.5c-.83 1.24-1.71 2.45-3.05 2.47-1.34.03-1.77-.79-3.29-.79-1.53 0-2 .77-3.27.82-1.31.05-2.3-1.32-3.14-2.53C4.25 17 2.94 12.45 4.7 9.39c.87-1.52 2.43-2.48 4.12-2.51 1.28-.02 2.5.87 3.29.87.78 0 2.26-1.07 3.8-.91.65.03 2.47.26 3.64 1.98-.09.06-2.17 1.28-2.15 3.81.03 3.02 2.65 4.03 2.68 4.04-.03.07-.42 1.44-1.38 2.83M13 3.5c.73-.83 1.94-1.46 2.94-1.5.13 1.17-.34 2.35-1.04 3.19-.69.85-1.83 1.51-2.95 1.42-.15-1.15.41-2.35 1.05-3.11z"/></svg>"#;

const SVG_DESKTOP: &str = r#"<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="3" width="20" height="14" rx="2"/><path d="M8 21h8"/><path d="M12 17v4"/></svg>"#;

const SVG_WEB: &str = r#"<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="M12 2a14.5 14.5 0 000 20 14.5 14.5 0 000-20"/><path d="M2 12h20"/></svg>"#;

const SVG_DIOXUS: &str = r#"<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/></svg>"#;

const SVG_REACT: &str = r#"<svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor"><path d="M12 10.11c1.03 0 1.87.84 1.87 1.89 0 1-.84 1.85-1.87 1.85S10.13 13 10.13 12c0-1.05.84-1.89 1.87-1.89M7.37 20c.63.38 2.01-.2 3.6-1.7-.52-.59-1.03-1.23-1.51-1.9a22.7 22.7 0 01-2.4-.36c-.51 2.14-.32 3.61.31 3.96m.71-5.74l-.29-.51c-.11.29-.22.58-.29.86.27.06.57.11.88.16l-.3-.51m6.54-.76l.81-1.5-.81-1.5c-.3-.53-.62-1-.91-1.47C13.17 9 12.6 9 12 9c-.6 0-1.17 0-1.71.03-.29.47-.61.94-.91 1.47L8.57 12l.81 1.5c.3.53.62 1 .91 1.47.54.03 1.11.03 1.71.03.6 0 1.17 0 1.71-.03.29-.47.61-.94.91-1.47M12 6.78c-.19.22-.39.45-.59.72h1.18c-.2-.27-.4-.5-.59-.72m0 10.44c.19-.22.39-.45.59-.72h-1.18c.2.27.4.5.59.72M16.62 4c-.62-.38-2 .2-3.59 1.7.52.59 1.03 1.23 1.51 1.9.82.08 1.63.2 2.4.36.51-2.14.32-3.61-.32-3.96m-.7 5.74l.29.51c.11-.29.22-.58.29-.86-.27-.06-.57-.11-.88-.16l.3.51m1.45-7.05c1.47.84 1.63 3.05 1.01 5.63 2.54.75 4.37 1.99 4.37 3.68 0 1.69-1.83 2.93-4.37 3.68.62 2.58.46 4.79-1.01 5.63-1.46.84-3.45-.12-5.37-1.95-1.92 1.83-3.91 2.79-5.38 1.95-1.46-.84-1.62-3.05-1-5.63-2.54-.75-4.37-1.99-4.37-3.68 0-1.69 1.83-2.93 4.37-3.68-.62-2.58-.46-4.79 1-5.63 1.47-.84 3.46.12 5.38 1.95 1.92-1.83 3.91-2.79 5.37-1.95M17.08 12c.34.75.64 1.5.89 2.26 2.1-.63 3.28-1.53 3.28-2.26 0-.73-1.18-1.63-3.28-2.26-.25.76-.55 1.51-.89 2.26M6.92 12c-.34-.75-.64-1.5-.89-2.26-2.1.63-3.28 1.53-3.28 2.26 0 .73 1.18 1.63 3.28 2.26.25-.76.55-1.51.89-2.26m9 2.26l-.3.51c.31-.05.61-.1.88-.16-.07-.28-.18-.57-.29-.86l-.29.51m-9.82 1.7c.52.59 1.03 1.23 1.51 1.9a22.7 22.7 0 01-2.4-.36c-.51 2.14-.32 3.61.31 3.96"/></svg>"#;

const SVG_WATERMELON: &str = r#"<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M4.5 19.5L19.5 4.5"/><path d="M4.5 19.5c-2-2-2-6.5 2.5-11s9-4.5 11-2.5"/><path d="M4.5 19.5c2 2 6.5 2 11-2.5s4.5-9 2.5-11"/><circle cx="10" cy="11" r=".5" fill="currentColor"/><circle cx="13" cy="14" r=".5" fill="currentColor"/><circle cx="11" cy="14.5" r=".5" fill="currentColor"/></svg>"#;

const SVG_SQL: &str = r#"<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg>"#;
