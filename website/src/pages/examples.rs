use dioxus::prelude::*;

const REPO_BASE: &str = "https://github.com/pvg13/WaveSyncDB/tree/main/examples";

struct ExampleCard {
    title: &'static str,
    path: &'static str,
    summary: &'static str,
    tags: &'static [&'static str],
}

const EXAMPLES: &[ExampleCard] = &[
    ExampleCard {
        title: "dioxus_app",
        path: "dioxus_app",
        summary: "A minimal cross-platform task list. The simplest possible WaveSyncDB integration with reactive Dioxus hooks.",
        tags: &["Desktop", "Dioxus", "Hooks"],
    },
    ExampleCard {
        title: "dioxus_dynamic",
        path: "dioxus_dynamic",
        summary: "Dynamic-schema example: registers entities at runtime, useful when your tables aren't known at compile time.",
        tags: &["Desktop", "Dioxus", "Dynamic schema"],
    },
    ExampleCard {
        title: "dioxus_fcm_sync",
        path: "dioxus_fcm_sync",
        summary: "Cross-platform desktop + Android + iOS. Demonstrates the FCM/APNs push wake-up flow with a relay server.",
        tags: &["Desktop", "Android", "iOS", "Push"],
    },
    ExampleCard {
        title: "p2p",
        path: "p2p",
        summary: "Headless multi-peer sync without any UI. Useful for understanding the engine in isolation.",
        tags: &["CLI", "P2P"],
    },
    ExampleCard {
        title: "wan",
        path: "wan",
        summary: "WAN sync via the relay server. Shows how peers on different networks discover and exchange changes.",
        tags: &["WAN", "Relay"],
    },
];

#[component]
pub fn Examples() -> Element {
    rsx! {
        section { class: "page-header",
            div { class: "section-inner",
                h1 { class: "page-title", "Examples" }
                p { class: "page-subtitle",
                    "Working code in the repository. Each example is a self-contained crate you can run in minutes."
                }
            }
        }
        section { class: "examples-grid-section",
            div { class: "section-inner",
                div { class: "examples-grid",
                    for ex in EXAMPLES.iter() {
                        a {
                            class: "example-card",
                            href: "{REPO_BASE}/{ex.path}",
                            target: "_blank",
                            rel: "noopener",
                            div { class: "example-card-header",
                                h3 { class: "example-card-title", "{ex.title}" }
                                span { class: "example-card-arrow", "→" }
                            }
                            p { class: "example-card-summary", "{ex.summary}" }
                            div { class: "example-card-tags",
                                for tag in ex.tags.iter() {
                                    span { class: "example-card-tag", "{tag}" }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
