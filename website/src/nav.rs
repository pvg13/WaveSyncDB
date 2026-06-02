use dioxus::prelude::*;
use crate::Route;

#[component]
pub fn NavBar() -> Element {
    rsx! {
        nav {
            div { class: "nav-inner",
                Link { to: Route::Home {}, class: "nav-logo",
                    "Wave" span { "Sync" } "DB"
                }
                div { class: "nav-links",
                    Link { to: Route::Home {}, "Home" }
                    Link { to: Route::DocsOverview {}, "Docs" }
                    a {
                        href: "https://github.com/pvg13/wavesyncdb",
                        target: "_blank",
                        class: "btn-github",
                        "GitHub"
                    }
                }
            }
        }
    }
}
