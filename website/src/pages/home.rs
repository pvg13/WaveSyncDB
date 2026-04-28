use dioxus::prelude::*;

use crate::app::Route;
use crate::components::{CodeBlock, FeatureCard, FlowDiagram};

const QUICKSTART_CODE: &str = r#"use sea_orm::*;
use wavesyncdb::WaveSyncDbBuilder;

#[tokio::main]
async fn main() -> Result<(), DbErr> {
    // Build the connection — starts the P2P engine automatically
    let db = WaveSyncDbBuilder::new("sqlite:./app.db?mode=rwc", "my-app-topic")
        .build()
        .await?;

    // Auto-discover entities annotated with #[derive(SyncEntity)]
    db.get_schema_registry(module_path!().split("::").next().unwrap())
        .sync()
        .await?;

    // Standard SeaORM usage — sync happens transparently
    let task = task::ActiveModel {
        id: Set("1".into()),
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
        section { class: "hero",
            div { class: "hero-inner",
                div { class: "hero-copy",
                    span { class: "hero-eyebrow", "Open-source · Rust · AGPL-3.0 + Commercial" }
                    h1 { class: "hero-title",
                        "Local-first SQLite "
                        span { class: "hero-title-accent", "that syncs itself." }
                    }
                    p { class: "hero-subtitle",
                        "Drop-in SeaORM connection. Per-column CRDTs. Peer-to-peer over libp2p. "
                        "Mobile push wake-up included. Your existing app code keeps working — sync happens in the background."
                    }
                    div { class: "hero-cta",
                        Link {
                            class: "btn btn-primary",
                            to: Route::DocPage { slug: "quickstart".to_string() },
                            "Get started →"
                        }
                        a {
                            class: "btn btn-secondary",
                            href: "https://github.com/pvg13/WaveSyncDB",
                            target: "_blank",
                            rel: "noopener",
                            "Star on GitHub"
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
                h2 { class: "section-title", "Why WaveSyncDB" }
                p { class: "section-subtitle",
                    "Built for apps that want to feel instant offline and converge automatically when peers come back online. Dual-licensed: AGPL-3.0-or-later for open source, commercial license available for proprietary use."
                }
                div { class: "feature-grid",
                    FeatureCard {
                        icon: "🪶".to_string(),
                        title: "Drop-in SeaORM".to_string(),
                        body: "Replace your DatabaseConnection with WaveSyncDb. Every insert, update, and delete syncs in the background — no API changes.".to_string(),
                    }
                    FeatureCard {
                        icon: "🧬".to_string(),
                        title: "Per-column CRDTs".to_string(),
                        body: "Concurrent edits to different columns of the same row both survive. Lamport clocks give every peer the same final state, deterministically.".to_string(),
                    }
                    FeatureCard {
                        icon: "📱".to_string(),
                        title: "Cross-platform".to_string(),
                        body: "Desktop, Android, and iOS from one Rust codebase. First-class Dioxus reactive hooks.".to_string(),
                    }
                    FeatureCard {
                        icon: "🌐".to_string(),
                        title: "P2P over libp2p".to_string(),
                        body: "Direct sync via mDNS on the LAN, AutoNAT and circuit relay for WAN, DCUtR hole-punching for direct connections when possible.".to_string(),
                    }
                    FeatureCard {
                        icon: "🔔".to_string(),
                        title: "Push wake-up".to_string(),
                        body: "Silent FCM and APNs notifications wake sleeping mobile peers so a single desktop write reaches every device in seconds.".to_string(),
                    }
                    FeatureCard {
                        icon: "🔐".to_string(),
                        title: "Group authentication".to_string(),
                        body: "A shared passphrase derives the topic and signs every message via HMAC. Peers without the passphrase are silently rejected.".to_string(),
                    }
                }
            }
        }

        section { class: "how-it-works",
            div { class: "section-inner",
                h2 { class: "section-title", "How it works" }
                p { class: "section-subtitle",
                    "WaveSyncDB wraps the SeaORM connection trait. Writes go to your local SQLite first, then propagate to peers through two complementary paths — real-time fan-out and version-vector catch-up."
                }
                FlowDiagram {}
            }
        }

        section { class: "perf-strip",
            div { class: "section-inner section-inner-narrow",
                h2 { class: "section-title", "Built for live collaboration" }
                p { class: "section-subtitle",
                    "Real numbers, measured on a desktop with both peers in process."
                }
                div { class: "perf-grid",
                    div { class: "perf-stat",
                        div { class: "perf-value", "0.42 ms" }
                        div { class: "perf-label", "p50 peer-to-peer write latency" }
                    }
                    div { class: "perf-stat",
                        div { class: "perf-value", "10 000+ /s" }
                        div { class: "perf-label", "local writes through WaveSyncDb" }
                    }
                    div { class: "perf-stat",
                        div { class: "perf-value", "5 900+ /s" }
                        div { class: "perf-label", "rows reconciled during catch-up" }
                    }
                }
                div { class: "perf-cta",
                    Link {
                        class: "btn btn-secondary",
                        to: Route::DocPage { slug: "benchmarks".to_string() },
                        "See full benchmarks →"
                    }
                }
            }
        }

        section { class: "entity-section",
            div { class: "section-inner section-inner-narrow",
                h2 { class: "section-title", "Plain SeaORM entities, sync-aware" }
                p { class: "section-subtitle",
                    "Add a single derive. WaveSyncDB tracks every column at the storage layer; you keep writing the same code."
                }
                CodeBlock { lang: "rust".to_string(), code: ENTITY_CODE.to_string() }
            }
        }

        section { class: "cta",
            div { class: "section-inner",
                h2 { class: "cta-title", "Ready to make your app local-first?" }
                p { class: "cta-subtitle",
                    "Read the quickstart, clone an example, or jump into the docs."
                }
                div { class: "cta-actions",
                    Link {
                        class: "btn btn-primary",
                        to: Route::DocPage { slug: "quickstart".to_string() },
                        "Quickstart"
                    }
                    Link { class: "btn btn-secondary", to: Route::Examples {}, "Browse examples" }
                }
            }
        }
    }
}
