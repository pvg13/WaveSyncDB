use dioxus::prelude::*;

use crate::app::Route;
use crate::docs_content::pages;

#[component]
pub fn DocsSidebar(active_slug: String) -> Element {
    rsx! {
        aside { class: "docs-sidebar",
            nav { class: "docs-sidebar-nav",
                h4 { class: "docs-sidebar-heading", "Documentation" }
                ul { class: "docs-sidebar-list",
                    for page in pages().iter() {
                        li {
                            Link {
                                class: if page.slug == active_slug { "docs-sidebar-link active" } else { "docs-sidebar-link" },
                                to: Route::DocPage { slug: page.slug.to_string() },
                                "{page.title}"
                            }
                        }
                    }
                }
            }
        }
    }
}
