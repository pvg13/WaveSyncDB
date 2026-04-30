use dioxus::prelude::*;

use crate::components::{Footer, Nav};
use crate::pages::{DocPage, Examples, Home, NotFound};

const MAIN_CSS: Asset = asset!("/assets/main.css");
const FAVICON: Asset = asset!("/assets/favicon.svg");

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
    // Default docs route lands on the introduction page.
    rsx! {
        DocPage { slug: "introduction".to_string() }
    }
}

#[component]
pub fn App() -> Element {
    rsx! {
        document::Link { rel: "icon", href: FAVICON }
        document::Stylesheet { href: MAIN_CSS }
        document::Meta {
            name: "description",
            content: "Local-first SQLite that syncs itself. Drop-in SeaORM connection with per-column CRDTs and P2P sync over libp2p.",
        }
        Router::<Route> {}
    }
}
