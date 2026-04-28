use dioxus::prelude::*;

/// Polished SVG of the WaveSyncDB write-path. Rendered as a raw HTML
/// string via `dangerous_inner_html` because Dioxus's RSX silently
/// drops several SVG elements (`marker`, `linearGradient`, `defs`,
/// `feDropShadow`, ...) that aren't in `dioxus-html`'s element table,
/// and camel-cased SVG attributes (`viewBox`, `markerWidth`) get
/// re-cased to lowercase, which the browser SVG parser ignores.
const FLOW_SVG: &str = r##"
<svg viewBox="0 0 920 520" xmlns="http://www.w3.org/2000/svg" role="img"
     aria-label="WaveSyncDB write path: app writes go through WaveSyncDb, which fans out to local SQLite, shadow tables, and the libp2p engine, and broadcasts a ChangeNotification.">
  <defs>
    <linearGradient id="wsd-accent" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="#6ce0c9"/>
      <stop offset="100%" stop-color="#7c9ff7"/>
    </linearGradient>
    <marker id="wsd-arrow" viewBox="0 0 10 10" refX="9" refY="5"
            markerWidth="6" markerHeight="6" orient="auto-start-reverse">
      <path d="M0,0 L10,5 L0,10 z" fill="#6ce0c9"/>
    </marker>
    <filter id="wsd-shadow" x="-20%" y="-20%" width="140%" height="140%">
      <feDropShadow dx="0" dy="8" stdDeviation="10" flood-color="#000" flood-opacity="0.35"/>
    </filter>
  </defs>

  <!-- App writes -->
  <g transform="translate(360, 24)">
    <rect width="200" height="60" rx="10" fill="#11151c" stroke="#303949" stroke-width="1.5" filter="url(#wsd-shadow)"/>
    <text x="100" y="27" text-anchor="middle" fill="#e6ecf2" font-size="14" font-weight="600">App writes via SeaORM</text>
    <text x="100" y="45" text-anchor="middle" fill="#9aa6b6" font-size="12" font-family="ui-monospace, monospace">task.insert(&amp;db).await?</text>
  </g>

  <path d="M460 92 L460 122" stroke="url(#wsd-accent)" stroke-width="2" fill="none" marker-end="url(#wsd-arrow)"/>

  <!-- WaveSyncDb -->
  <g transform="translate(280, 130)">
    <rect width="360" height="70" rx="12" fill="#161b24" stroke="url(#wsd-accent)" stroke-width="2" filter="url(#wsd-shadow)"/>
    <text x="180" y="32" text-anchor="middle" fill="#e6ecf2" font-size="16" font-weight="700">WaveSyncDb</text>
    <text x="180" y="52" text-anchor="middle" fill="#9aa6b6" font-size="12">ConnectionTrait wrapper · intercepts every write</text>
  </g>

  <!-- Branch arrows -->
  <path d="M340 200 Q 220 240 160 268" stroke="#6ce0c9" stroke-width="1.5" fill="none" marker-end="url(#wsd-arrow)"/>
  <path d="M460 200 L460 268" stroke="#6ce0c9" stroke-width="1.5" fill="none" marker-end="url(#wsd-arrow)"/>
  <path d="M580 200 Q 700 240 760 268" stroke="#6ce0c9" stroke-width="1.5" fill="none" marker-end="url(#wsd-arrow)"/>

  <!-- Local SQLite -->
  <g transform="translate(40, 270)">
    <rect width="240" height="68" rx="10" fill="#11151c" stroke="#303949" stroke-width="1.5" filter="url(#wsd-shadow)"/>
    <text x="120" y="26" text-anchor="middle" fill="#e6ecf2" font-size="14" font-weight="600">Local SQLite</text>
    <text x="120" y="44" text-anchor="middle" fill="#9aa6b6" font-size="11">Synchronous · always commits first</text>
    <text x="120" y="58" text-anchor="middle" fill="#6ce0c9" font-size="11" font-family="ui-monospace, monospace">INSERT / UPDATE / DELETE</text>
  </g>

  <!-- Shadow tables -->
  <g transform="translate(340, 270)">
    <rect width="240" height="68" rx="10" fill="#11151c" stroke="#303949" stroke-width="1.5" filter="url(#wsd-shadow)"/>
    <text x="120" y="26" text-anchor="middle" fill="#e6ecf2" font-size="14" font-weight="600">Shadow tables</text>
    <text x="120" y="44" text-anchor="middle" fill="#9aa6b6" font-size="11">Per-column Lamport clocks</text>
    <text x="120" y="58" text-anchor="middle" fill="#6ce0c9" font-size="11" font-family="ui-monospace, monospace">_wavesync_*_clock</text>
  </g>

  <!-- libp2p engine -->
  <g transform="translate(640, 270)">
    <rect width="240" height="68" rx="10" fill="#11151c" stroke="#303949" stroke-width="1.5" filter="url(#wsd-shadow)"/>
    <text x="120" y="26" text-anchor="middle" fill="#e6ecf2" font-size="14" font-weight="600">libp2p engine</text>
    <text x="120" y="44" text-anchor="middle" fill="#9aa6b6" font-size="11">Real-time fan-out · catch-up</text>
    <text x="120" y="58" text-anchor="middle" fill="#6ce0c9" font-size="11" font-family="ui-monospace, monospace">SyncChangeset</text>
  </g>

  <!-- Output arrows -->
  <path d="M160 338 Q 200 380 290 412" stroke="#7c9ff7" stroke-width="1.5" fill="none" marker-end="url(#wsd-arrow)"/>
  <path d="M760 338 Q 720 380 630 412" stroke="#7c9ff7" stroke-width="1.5" fill="none" marker-end="url(#wsd-arrow)"/>

  <!-- Reactive UI -->
  <g transform="translate(180, 414)">
    <rect width="220" height="60" rx="10" fill="#11151c" stroke="#303949" stroke-width="1.5" filter="url(#wsd-shadow)"/>
    <text x="110" y="26" text-anchor="middle" fill="#e6ecf2" font-size="13" font-weight="600">Reactive UI</text>
    <text x="110" y="44" text-anchor="middle" fill="#9aa6b6" font-size="11">ChangeNotification → signals</text>
  </g>

  <!-- Remote peers -->
  <g transform="translate(520, 414)">
    <rect width="220" height="60" rx="10" fill="#11151c" stroke="#303949" stroke-width="1.5" filter="url(#wsd-shadow)"/>
    <text x="110" y="26" text-anchor="middle" fill="#e6ecf2" font-size="13" font-weight="600">Remote peers</text>
    <text x="110" y="44" text-anchor="middle" fill="#9aa6b6" font-size="11">Direct · LAN · WAN via relay</text>
  </g>
</svg>
"##;

#[component]
pub fn FlowDiagram() -> Element {
    rsx! {
        div { class: "flow-diagram", dangerous_inner_html: FLOW_SVG }
    }
}
