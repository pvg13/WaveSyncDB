use dioxus::prelude::*;

use crate::app::Route;

#[component]
pub fn NotFound(route: Vec<String>) -> Element {
    let path = route.join("/");
    rsx! {
        section { class: "not-found",
            div { class: "section-inner",
                h1 { class: "not-found-code", "404" }
                p { class: "not-found-path", "Nothing here at /{path}" }
                p { class: "not-found-help",
                    "The page may have moved. Try the docs or jump back to the landing page."
                }
                div { class: "not-found-actions",
                    Link { class: "btn btn-primary", to: Route::Home {}, "Home" }
                    Link { class: "btn btn-secondary", to: Route::DocsIndex {}, "Docs" }
                }
            }
        }
    }
}
