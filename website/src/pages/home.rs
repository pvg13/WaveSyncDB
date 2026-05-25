use dioxus::prelude::*;

use crate::app::Route;
use crate::components::{CodeBlock, FeatureCard, FlowDiagram};

const QUICKSTART_CODE: &str = r#"use sea_orm::*;
use wavesyncdb::WaveSyncDbBuilder;

#[tokio::main]
async fn main() -> Result<(), DbErr> {
    let db = WaveSyncDbBuilder::new("sqlite:./app.db?mode=rwc", "my-app-topic")
        .with_passphrase("shared-secret")
        .build()
        .await?;

    db.get_schema_registry(module_path!().split("::").next().unwrap())
        .sync()
        .await?;

    // Standard SeaORM — sync happens transparently
    let task = task::ActiveModel {
        id: Set(Uuid::new_v4().to_string()),
        title: Set("Buy milk".into()),
        completed: Set(false),
        ..Default::default()
    };
    task.insert(&db).await?;

    Ok(())
}"#;

const ENTITY_CODE: &str = r#"use sea_orm::entity::prelude::*;
use wavesyncdb_derive::SyncEntity;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, SyncEntity)]
#[sea_orm(table_name = "tasks")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub title: String,
    pub completed: bool,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}"#;

#[component]
pub fn Home() -> Element {
    rsx! {
        document::Title { "WaveSyncDB — Local-first SQLite that syncs itself" }
        section { class: "hero",
            div { class: "hero-inner",
                div { class: "hero-copy",
                    span { class: "hero-eyebrow", "Open-source · Rust · Dual-licensed" }
                    h1 { class: "hero-title",
                        "Local-first SQLite "
                        span { class: "hero-title-accent", "that syncs itself." }
                    }
                    p { class: "hero-subtitle",
                        "A drop-in SeaORM connection that replicates every write to peers "
                        "automatically. Per-column conflict resolution, P2P networking, "
                        "offline by default. No server required for LAN; optional relay for WAN."
                    }
                    div { class: "hero-cta",
                        Link {
                            class: "btn btn-primary",
                            to: Route::DocPage { slug: "quickstart".to_string() },
                            "Get started →"
                        }
                        Link {
                            class: "btn btn-secondary",
                            to: Route::TodoDemo {},
                            "Try the live demo"
                        }
                    }
                    div { class: "hero-install",
                        code { "cargo add wavesyncdb" }
                    }
                }
                div { class: "hero-code",
                    div { class: "hero-code-tab", "main.rs" }
                    CodeBlock { lang: "rust".to_string(), code: QUICKSTART_CODE.to_string() }
                }
            }
        }

        section { class: "features",
            div { class: "section-inner",
                h2 { class: "section-title", "What you get" }
                p { class: "section-subtitle",
                    "Built for small-group collaboration apps that need to work offline and converge when peers reconnect. Not a general-purpose database — a sync layer for your existing SeaORM code."
                }
                div { class: "feature-grid",
                    FeatureCard {
                        icon: "🪶".to_string(),
                        title: "Drop-in SeaORM".to_string(),
                        body: "Replace DatabaseConnection with WaveSyncDb. Your existing inserts, updates, and deletes replicate without API changes.".to_string(),
                    }
                    FeatureCard {
                        icon: "🧬".to_string(),
                        title: "Per-column CRDTs".to_string(),
                        body: "Concurrent edits to different columns both survive. Same-column conflicts resolve deterministically — every peer converges to the same state.".to_string(),
                    }
                    FeatureCard {
                        icon: "📱".to_string(),
                        title: "Cross-platform".to_string(),
                        body: "Desktop, Android, iOS, and browser (wasm32) from one Rust codebase. Dioxus reactive hooks included.".to_string(),
                    }
                    FeatureCard {
                        icon: "🌐".to_string(),
                        title: "P2P networking".to_string(),
                        body: "mDNS for LAN discovery. Circuit relay + DCUtR hole-punching for WAN. No central server in the data path.".to_string(),
                    }
                    FeatureCard {
                        icon: "🔔".to_string(),
                        title: "Mobile push wake-up".to_string(),
                        body: "Silent FCM/APNs notifications wake sleeping phones. A desktop write reaches every device in seconds via the relay.".to_string(),
                    }
                    FeatureCard {
                        icon: "🔐".to_string(),
                        title: "Group authentication".to_string(),
                        body: "Shared passphrase derives the topic and signs every message with HMAC-BLAKE3. Unauthenticated peers are silently dropped.".to_string(),
                    }
                }
            }
        }

        section { class: "how-it-works",
            div { class: "section-inner",
                h2 { class: "section-title", "How it works" }
                p { class: "section-subtitle",
                    "Every write hits local SQLite first (your app never blocks on the network), then propagates to peers via real-time fan-out and periodic version-vector catch-up."
                }
                FlowDiagram {}
            }
        }

        section { class: "perf-strip",
            div { class: "section-inner section-inner-narrow",
                h2 { class: "section-title", "Measured performance" }
                p { class: "section-subtitle",
                    "Both peers in process on the same machine, loopback networking. Real WAN adds relay round-trip latency."
                }
                div { class: "perf-grid",
                    div { class: "perf-stat",
                        div { class: "perf-value", "0.42 ms" }
                        div { class: "perf-label", "p50 peer-to-peer latency (loopback)" }
                    }
                    div { class: "perf-stat",
                        div { class: "perf-value", "~10 000 /s" }
                        div { class: "perf-label", "local writes through WaveSyncDb" }
                    }
                    div { class: "perf-stat",
                        div { class: "perf-value", "~5 900 /s" }
                        div { class: "perf-label", "rows reconciled on catch-up" }
                    }
                }
                div { class: "perf-cta",
                    Link {
                        class: "btn btn-secondary",
                        to: Route::Benchmarks {},
                        "See full benchmarks →"
                    }
                }
            }
        }

        section { class: "entity-section",
            div { class: "section-inner section-inner-narrow",
                h2 { class: "section-title", "One derive, full sync" }
                p { class: "section-subtitle",
                    "Add #[derive(SyncEntity)] to a standard SeaORM entity. WaveSyncDB handles the rest at the storage layer."
                }
                CodeBlock { lang: "rust".to_string(), code: ENTITY_CODE.to_string() }
            }
        }

        section { class: "cta",
            div { class: "section-inner",
                h2 { class: "cta-title", "See it running in your browser" }
                p { class: "cta-subtitle",
                    "The live demo runs the real sync engine compiled to wasm32 with IndexedDB storage. "
                    "Two virtual devices sync over a loopback channel — try going offline and reconnecting."
                }
                div { class: "cta-actions",
                    Link {
                        class: "btn btn-primary",
                        to: Route::TodoDemo {},
                        "Try the live demo →"
                    }
                    Link {
                        class: "btn btn-secondary",
                        to: Route::DocPage { slug: "quickstart".to_string() },
                        "Quickstart"
                    }
                    Link { class: "btn btn-secondary", to: Route::Examples {}, "Examples" }
                }
            }
        }
    }
}
