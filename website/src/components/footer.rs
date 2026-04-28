use dioxus::prelude::*;

#[component]
pub fn Footer() -> Element {
    rsx! {
        footer { class: "site-footer",
            div { class: "site-footer-inner",
                div { class: "site-footer-col",
                    h4 { "WaveSyncDB" }
                    p { class: "site-footer-tagline",
                        "Local-first SQLite that syncs itself."
                    }
                    p { class: "site-footer-tagline",
                        "Dual-licensed: AGPL-3.0-or-later for open source, "
                        "commercial license for proprietary use."
                    }
                }
                div { class: "site-footer-col",
                    h4 { "Project" }
                    a { href: "https://github.com/pvg13/WaveSyncDB", target: "_blank", rel: "noopener", "GitHub" }
                    a { href: "https://github.com/pvg13/WaveSyncDB/issues", target: "_blank", rel: "noopener", "Issues" }
                    a { href: "https://crates.io/crates/wavesyncdb", target: "_blank", rel: "noopener", "crates.io" }
                }
                div { class: "site-footer-col",
                    h4 { "Built with" }
                    a { href: "https://dioxuslabs.com", target: "_blank", rel: "noopener", "Dioxus" }
                    a { href: "https://www.sea-ql.org/SeaORM/", target: "_blank", rel: "noopener", "SeaORM" }
                    a { href: "https://libp2p.io", target: "_blank", rel: "noopener", "libp2p" }
                }
            }
            div { class: "site-footer-bottom",
                "© 2026 WaveSyncDB contributors"
            }
        }
    }
}
