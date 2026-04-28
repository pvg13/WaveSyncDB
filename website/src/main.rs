mod app;
mod components;
mod docs_content;
mod pages;

use app::App;

fn main() {
    dioxus::launch(App);
}
