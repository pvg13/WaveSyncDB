use dioxus::prelude::*;

#[component]
pub fn CodeBlock(lang: String, code: String) -> Element {
    let class = format!("language-{lang}");
    rsx! {
        pre { class: "code-block",
            code { class: "{class}", "{code}" }
        }
    }
}
