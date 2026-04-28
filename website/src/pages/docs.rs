use dioxus::prelude::*;

use crate::components::DocsSidebar;
use crate::docs_content::find_page;

/// JS wired into every docs render. Two responsibilities:
///
///   1. Re-highlight any new code blocks (highlight.js doesn't watch the
///      DOM — it has to be triggered).
///   2. Convert `<pre><code class="language-mermaid">…</code></pre>`
///      blocks into `<div class="mermaid">…</div>` and ask Mermaid to
///      render them as SVG.
///
/// Re-running on route change is essential: SPA navigation between
/// `/docs/architecture` and `/docs/networking` swaps the inner HTML
/// without a full reload, so a one-shot script in `index.html` would
/// never see the new content.
const POST_RENDER_JS: &str = r#"
(function() {
    // Convert pulldown-cmark's <pre><code class="language-mermaid">…</code></pre>
    // into the <div class="mermaid">…</div> shape that mermaid.run() expects.
    // Done synchronously up-front, BEFORE highlight.js runs, so highlight.js
    // never sees a code block whose language is "mermaid" (it would warn
    // about an unknown language and the dev tools log-relay would error on
    // the multi-arg console.warn).
    function extractMermaidBlocks() {
        document.querySelectorAll('pre > code.language-mermaid').forEach(function(el) {
            const pre = el.parentElement;
            const div = document.createElement('div');
            div.className = 'mermaid';
            div.textContent = el.textContent;
            pre.replaceWith(div);
        });
    }

    function highlight() {
        if (!window.hljs) return;
        // Skip mermaid blocks that may not have been extracted yet (defense
        // in depth — if the Mermaid CDN script is slow to load, the blocks
        // are still in their <pre><code class="language-mermaid"> shape).
        document.querySelectorAll(
            'pre code:not(.hljs):not(.language-mermaid)'
        ).forEach(function(el) {
            try { window.hljs.highlightElement(el); } catch (e) {}
        });
    }

    function renderMermaid() {
        if (!window.mermaid) {
            // CDN script may still be loading on first render — retry shortly.
            return setTimeout(renderMermaid, 120);
        }
        if (!window.__mermaid_initialized) {
            window.mermaid.initialize({
                startOnLoad: false,
                theme: 'dark',
                themeVariables: {
                    background: '#0a0d12',
                    primaryColor: '#11151c',
                    primaryTextColor: '#e6ecf2',
                    primaryBorderColor: '#303949',
                    lineColor: '#6ce0c9',
                    secondaryColor: '#161b24',
                    tertiaryColor: '#0e1117',
                    fontSize: '14px',
                },
                securityLevel: 'loose',
                fontFamily: 'inherit',
            });
            window.__mermaid_initialized = true;
        }
        try {
            window.mermaid.run({ querySelector: '.mermaid:not([data-processed="true"])', suppressErrors: true });
        } catch (e) {
            console.warn('Mermaid render failed:', e);
        }
    }

    extractMermaidBlocks();
    highlight();
    renderMermaid();
})();
"#;

#[component]
pub fn DocPage(slug: String) -> Element {
    let page = find_page(&slug);

    // Re-run highlight.js + Mermaid every time the slug changes. `use_effect`
    // alone tracks signal reads, and a plain `String` prop is not a signal —
    // so we wrap it in `use_reactive!` which makes the closure re-run when
    // any captured argument changes by value.
    use_effect(use_reactive!(|slug| {
        let _ = slug; // captured by the macro for dependency tracking
        document::eval(POST_RENDER_JS);
    }));

    rsx! {
        div { class: "docs-layout",
            DocsSidebar { active_slug: slug.clone() }
            article { class: "docs-content",
                match page {
                    Some(p) => rsx! {
                        div { class: "docs-content-inner",
                            div { class: "docs-content-eyebrow", "Documentation" }
                            div { dangerous_inner_html: "{p.html}" }
                        }
                    },
                    None => rsx! {
                        div { class: "docs-content-inner",
                            h1 { "Not found" }
                            p { "No documentation page exists at /docs/{slug}." }
                        }
                    },
                }
            }
        }
    }
}
