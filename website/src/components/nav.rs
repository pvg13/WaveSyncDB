use dioxus::prelude::*;

use crate::app::Route;

const LOGO: Asset = asset!("/assets/logo.svg");

#[component]
pub fn Nav() -> Element {
    rsx! {
        header { class: "site-nav",
            div { class: "site-nav-inner",
                Link { class: "site-brand", to: Route::Home {},
                    img { src: LOGO, alt: "WaveSyncDB", class: "site-brand-logo" }
                    span { class: "site-brand-name", "WaveSyncDB" }
                }
                nav { class: "site-nav-links",
                    Link { class: "site-nav-link", to: Route::DocsIndex {}, "Docs" }
                    Link { class: "site-nav-link", to: Route::Examples {}, "Examples" }
                    a {
                        class: "site-nav-link site-nav-github",
                        href: "https://github.com/pvg13/WaveSyncDB",
                        target: "_blank",
                        rel: "noopener",
                        "GitHub"
                    }
                }
            }
        }
    }
}
