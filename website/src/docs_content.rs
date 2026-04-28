//! Compile-time embedded documentation pages.
//!
//! Markdown files live under `content/docs/`. They are pulled in via
//! `include_str!` so the WASM bundle ships with the rendered docs and
//! does not require any runtime filesystem access.
//!
//! Filenames follow `NN-slug.md` so the numeric prefix gives the sidebar
//! a stable order without a manual TOC. The slug exposed in the URL is
//! the part after the prefix.

use std::sync::OnceLock;

use pulldown_cmark::{Options, Parser, html};

pub struct DocPage {
    pub slug: &'static str,
    pub title: &'static str,
    pub html: String,
}

struct RawDoc {
    slug: &'static str,
    title: &'static str,
    markdown: &'static str,
}

const RAW_DOCS: &[RawDoc] = &[
    RawDoc {
        slug: "introduction",
        title: "Introduction",
        markdown: include_str!("../content/docs/01-introduction.md"),
    },
    RawDoc {
        slug: "installation",
        title: "Installation",
        markdown: include_str!("../content/docs/02-installation.md"),
    },
    RawDoc {
        slug: "quickstart",
        title: "Quickstart",
        markdown: include_str!("../content/docs/03-quickstart.md"),
    },
    RawDoc {
        slug: "architecture",
        title: "Architecture",
        markdown: include_str!("../content/docs/04-architecture.md"),
    },
    RawDoc {
        slug: "sync-protocol",
        title: "Sync protocol",
        markdown: include_str!("../content/docs/05-sync-protocol.md"),
    },
    RawDoc {
        slug: "conflict-resolution",
        title: "Conflict resolution",
        markdown: include_str!("../content/docs/06-conflict-resolution.md"),
    },
    RawDoc {
        slug: "authentication",
        title: "Authentication & security",
        markdown: include_str!("../content/docs/07-authentication.md"),
    },
    RawDoc {
        slug: "networking",
        title: "Networking & discovery",
        markdown: include_str!("../content/docs/08-networking.md"),
    },
    RawDoc {
        slug: "schema",
        title: "Schema & registration",
        markdown: include_str!("../content/docs/09-schema.md"),
    },
    RawDoc {
        slug: "dioxus-integration",
        title: "Dioxus integration",
        markdown: include_str!("../content/docs/10-dioxus-integration.md"),
    },
    RawDoc {
        slug: "mobile-and-push",
        title: "Mobile & push notifications",
        markdown: include_str!("../content/docs/11-mobile-and-push.md"),
    },
    RawDoc {
        slug: "relay-deployment",
        title: "Relay deployment",
        markdown: include_str!("../content/docs/12-relay-deployment.md"),
    },
    RawDoc {
        slug: "configuration",
        title: "Configuration reference",
        markdown: include_str!("../content/docs/13-configuration.md"),
    },
    RawDoc {
        slug: "faq",
        title: "FAQ & troubleshooting",
        markdown: include_str!("../content/docs/14-faq.md"),
    },
    RawDoc {
        slug: "benchmarks",
        title: "Benchmarks",
        markdown: include_str!("../content/docs/15-benchmarks.md"),
    },
    RawDoc {
        slug: "api-reference",
        title: "API reference",
        markdown: include_str!("../content/docs/16-api-reference.md"),
    },
];

fn render_markdown(input: &str) -> String {
    let mut opts = Options::empty();
    opts.insert(Options::ENABLE_TABLES);
    opts.insert(Options::ENABLE_STRIKETHROUGH);
    opts.insert(Options::ENABLE_FOOTNOTES);
    opts.insert(Options::ENABLE_TASKLISTS);
    let parser = Parser::new_ext(input, opts);
    let mut out = String::new();
    html::push_html(&mut out, parser);
    out
}

fn build() -> Vec<DocPage> {
    RAW_DOCS
        .iter()
        .map(|raw| DocPage {
            slug: raw.slug,
            title: raw.title,
            html: render_markdown(raw.markdown),
        })
        .collect()
}

pub fn pages() -> &'static [DocPage] {
    static CACHE: OnceLock<Vec<DocPage>> = OnceLock::new();
    CACHE.get_or_init(build).as_slice()
}

pub fn find_page(slug: &str) -> Option<&'static DocPage> {
    pages().iter().find(|p| p.slug == slug)
}
