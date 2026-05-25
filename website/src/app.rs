use dioxus::prelude::*;

use crate::components::{Footer, Nav};
use crate::pages::{Benchmarks, DocPage, Examples, Home, NotFound, SyncDemo, TodoDemo};

const MAIN_CSS: Asset = asset!("/assets/main.css");
const FAVICON: Asset = asset!("/assets/favicon.svg");
const OG_IMAGE: Asset = asset!("/assets/og.png");

const SITE_URL: &str = "https://wavesyncdb.com";
const SITE_DESCRIPTION: &str =
    "Local-first SQLite that syncs itself. Drop-in SeaORM connection with per-column CRDTs and P2P sync over libp2p.";

#[derive(Clone, Routable, PartialEq, Debug)]
pub enum Route {
    #[layout(Layout)]
    #[route("/")]
    Home {},
    #[route("/docs")]
    DocsIndex {},
    #[route("/docs/:slug")]
    DocPage { slug: String },
    #[route("/examples")]
    Examples {},
    #[route("/benchmarks")]
    Benchmarks {},
    #[route("/demo")]
    TodoDemo {},
    #[route("/sync-demo")]
    SyncDemo {},
    #[end_layout]
    #[route("/:..route")]
    NotFound { route: Vec<String> },
}

#[component]
fn Layout() -> Element {
    rsx! {
        Nav {}
        main { class: "site-main", Outlet::<Route> {} }
        Footer {}
    }
}

#[component]
fn DocsIndex() -> Element {
    rsx! {
        DocPage { slug: "introduction".to_string() }
    }
}

#[component]
pub fn App() -> Element {
    rsx! {
        document::Link { rel: "icon", href: FAVICON }
        document::Stylesheet { href: MAIN_CSS }

        // Primary meta
        document::Meta { name: "description", content: SITE_DESCRIPTION }

        // Open Graph
        document::Meta { property: "og:type", content: "website" }
        document::Meta { property: "og:site_name", content: "WaveSyncDB" }
        document::Meta { property: "og:title", content: "WaveSyncDB — Local-first SQLite that syncs itself" }
        document::Meta { property: "og:description", content: SITE_DESCRIPTION }
        document::Meta { property: "og:url", content: SITE_URL }
        document::Meta { property: "og:image", content: "{SITE_URL}{OG_IMAGE}" }

        // Twitter Card
        document::Meta { name: "twitter:card", content: "summary_large_image" }
        document::Meta { name: "twitter:title", content: "WaveSyncDB — Local-first SQLite that syncs itself" }
        document::Meta { name: "twitter:description", content: SITE_DESCRIPTION }
        document::Meta { name: "twitter:image", content: "{SITE_URL}{OG_IMAGE}" }

        // Canonical
        document::Link { rel: "canonical", href: SITE_URL }

        Router::<Route> {}
    }
}
