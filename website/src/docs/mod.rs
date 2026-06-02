pub mod overview;
pub mod installation;
pub mod dioxus_guide;
pub mod react_native;
pub mod watermelondb;
pub mod raw_sql;
pub mod sync_protocol;
pub mod conflict;
pub mod push;
pub mod relay;
pub mod network;
pub mod api_reference;
pub mod security;
pub mod benchmarks;

use dioxus::prelude::*;
use crate::Route;

/// Shared code block component — renders pre-formatted HTML safely.
/// Use static `&str` consts to avoid RSX curly-brace escaping issues.
#[component]
pub fn CodeBlock(html: &'static str) -> Element {
    rsx! { div { class: "code-block", dangerous_inner_html: "{html}" } }
}

/// Inline code snippet.
#[component]
pub fn C(text: &'static str) -> Element {
    rsx! { code { "{text}" } }
}

/// Section heading with anchor.
#[component]
pub fn H2(id: &'static str, text: &'static str) -> Element {
    rsx! { h2 { id: id, "{text}" } }
}

/// Subsection heading.
#[component]
pub fn H3(id: &'static str, text: &'static str) -> Element {
    rsx! { h3 { id: id, "{text}" } }
}

/// A docs sidebar link entry.
struct SidebarLink {
    route: Route,
    label: &'static str,
}

#[component]
pub fn DocsLayout(children: Element) -> Element {
    let sections: Vec<(&str, Vec<SidebarLink>)> = vec![
        ("Getting Started", vec![
            SidebarLink { route: Route::DocsOverview {}, label: "Overview" },
            SidebarLink { route: Route::DocsInstallation {}, label: "Installation" },
        ]),
        ("Framework Guides", vec![
            SidebarLink { route: Route::DocsDioxus {}, label: "Dioxus" },
            SidebarLink { route: Route::DocsReactNative {}, label: "React Native" },
            SidebarLink { route: Route::DocsWatermelondb {}, label: "WatermelonDB" },
            SidebarLink { route: Route::DocsRawSql {}, label: "Raw SQL" },
        ]),
        ("Concepts", vec![
            SidebarLink { route: Route::DocsSyncProtocol {}, label: "Sync Protocol" },
            SidebarLink { route: Route::DocsConflictResolution {}, label: "Conflict Resolution" },
            SidebarLink { route: Route::DocsSecurity {}, label: "Security" },
        ]),
        ("Infrastructure", vec![
            SidebarLink { route: Route::DocsPushNotifications {}, label: "Push Notifications" },
            SidebarLink { route: Route::DocsRelayServer {}, label: "Relay Server" },
        ]),
        ("Reference", vec![
            SidebarLink { route: Route::DocsNetworkStatus {}, label: "Network & Events" },
            SidebarLink { route: Route::DocsApiReference {}, label: "API Reference" },
            SidebarLink { route: Route::DocsBenchmarks {}, label: "Benchmarks" },
        ]),
    ];

    rsx! {
        div { class: "docs-layout",
            aside { class: "docs-sidebar",
                for (title, links) in &sections {
                    h4 { "{title}" }
                    for link in links {
                        Link {
                            to: link.route.clone(),
                            "{link.label}"
                        }
                    }
                }
            }
            div { class: "docs-content",
                {children}
            }
        }
    }
}
