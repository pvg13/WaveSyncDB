mod benchmarks;
mod docs;
mod examples;
mod home;
mod not_found;

// `sync_demo` and `todo_demo` use the wasm-only `WebSyncClient`,
// `BrowserEntity`, `LoopbackPair`, etc. — these are cfg-gated to
// wasm32 in `wavesyncdb::lib`. The `Route` enum in `app.rs` references
// the components on every target, so we ship browser-only stubs on
// native to keep the route table buildable for the dioxus-fullstack
// server side and `cargo check` runs.
#[cfg(target_arch = "wasm32")]
mod sync_demo;
#[cfg(target_arch = "wasm32")]
mod todo_demo;

pub use benchmarks::Benchmarks;
pub use docs::DocPage;
pub use examples::Examples;
pub use home::Home;
pub use not_found::NotFound;
#[cfg(target_arch = "wasm32")]
pub use sync_demo::SyncDemo;
#[cfg(target_arch = "wasm32")]
pub use todo_demo::TodoDemo;

#[cfg(not(target_arch = "wasm32"))]
mod browser_only_stubs {
    use dioxus::prelude::*;

    #[component]
    pub fn SyncDemo() -> Element {
        rsx! {
            div { class: "panel",
                p { "This demo runs in the browser only — open it in a wasm build." }
            }
        }
    }

    #[component]
    pub fn TodoDemo() -> Element {
        rsx! {
            div { class: "panel",
                p { "This demo runs in the browser only — open it in a wasm build." }
            }
        }
    }
}
#[cfg(not(target_arch = "wasm32"))]
pub use browser_only_stubs::{SyncDemo, TodoDemo};
