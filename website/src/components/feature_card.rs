use dioxus::prelude::*;

#[component]
pub fn FeatureCard(icon: String, title: String, body: String) -> Element {
    rsx! {
        div { class: "feature-card",
            div { class: "feature-card-icon", "{icon}" }
            h3 { class: "feature-card-title", "{title}" }
            p { class: "feature-card-body", "{body}" }
        }
    }
}
