// Headless-Chrome driver for the qr-pairing web build.
//
// Opens the page, waits for the WebSyncClient to come up (signalled by
// the QR appearing — `qr_svg` is non-empty only after the local peer-id
// is known and a relay connection round-trip has completed), then types
// "from-web" into the Tasks input and clicks Add. Pipes the page's
// console (which is where wasm `log::*!` lands via `wasm-logger`) to
// stderr, prefixed with [wasm], so the diagnostic `WebSyncClient:`
// lines show up in the same stream as the dx serve output.
//
// After Add, the driver also waits for `EXPECT_TASK_TEXT` to appear in
// the page DOM — this lets the orchestrator assert the *other*
// direction (phone→web) too. The phone-side maestro flow types
// "from-phone" and clicks Add a moment after seeing "from-web"; if
// that string never shows up here within EXPECT_TIMEOUT_MS, phone→web
// push is broken and the driver exits non-zero.
//
// Stays running until that assertion completes (or until killed if
// EXPECT_TASK_TEXT is empty). Without keeping the swarm alive the
// connection drops as soon as we close the page.
//
// Used by test.sh --full to chain web Add → phone Add → both
// assertions in one command. Standalone:
//   WEB_URL=http://192.168.1.150:8087 TASK_TEXT=from-web node web_driver.js
//
// Env vars:
//   WEB_URL            — page to open (default http://localhost:8087)
//   TASK_TEXT          — string typed into the Tasks input (default 'from-web')
//   EXPECT_TASK_TEXT   — string we wait for in the DOM after Add (default 'from-phone'; set empty to disable)
//   HEADLESS           — '0' to launch a visible window (debugging)
//   READY_TIMEOUT_MS   — abort if the QR doesn't appear in this window (default 60000)
//   EXPECT_TIMEOUT_MS  — give up waiting for EXPECT_TASK_TEXT after this (default 60000)

const puppeteer = require('puppeteer-core');

const WEB_URL = process.env.WEB_URL || 'http://localhost:8087';
const TASK_TEXT = process.env.TASK_TEXT || 'from-web';
const EXPECT_TASK_TEXT = process.env.EXPECT_TASK_TEXT ?? 'from-phone';
const HEADLESS = process.env.HEADLESS !== '0';
const READY_TIMEOUT_MS = Number(process.env.READY_TIMEOUT_MS || 60000);
const EXPECT_TIMEOUT_MS = Number(process.env.EXPECT_TIMEOUT_MS || 60000);

function log(...args) {
    process.stderr.write('[web_driver] ' + args.join(' ') + '\n');
}

async function main() {
    const browser = await puppeteer.launch({
        executablePath: '/usr/bin/google-chrome-stable',
        headless: HEADLESS ? 'new' : false,
        args: ['--no-sandbox', '--disable-dev-shm-usage'],
    });
    process.on('SIGINT', () => browser.close().then(() => process.exit(130)));
    process.on('SIGTERM', () => browser.close().then(() => process.exit(143)));

    const page = await browser.newPage();
    page.on('console', msg => process.stderr.write('[wasm] ' + msg.text() + '\n'));
    page.on('pageerror', err => process.stderr.write('[wasm-err] ' + err.message + '\n'));

    log('opening', WEB_URL);
    await page.goto(WEB_URL, { waitUntil: 'domcontentloaded' });

    // QR rendering only proves the engine has a local peer-id — it
    // shows up the moment `WebSyncClient::start` returns. To prove the
    // peer-discovery round-trip actually landed someone in the
    // engine's `connected` set we wait on the DebugPanel's "Peers N
    // connected" text. With N >= 1 the next Add will fan out to that
    // peer; without it, the push goes to nobody (silently — engine
    // logs `pushing changeset to 0 peer(s)`) and the bug looks the
    // same as a real delivery failure. Distinguishing the two is the
    // whole point of this driver.
    log('waiting for QR…');
    await page.waitForSelector('.qr svg', { timeout: READY_TIMEOUT_MS });
    log('QR visible (engine has local peer-id) — now waiting for >=1 connected peer');

    try {
        await page.waitForFunction(
            // The Peers row in DebugPanel is rendered as
            //   <p class="kv"><strong>Peers </strong><span class="value online|offline">N connected</span></p>
            // — pick the span by its sibling text and assert N>0.
            () => {
                const peersSpan = Array.from(document.querySelectorAll('p.kv'))
                    .find(p => p.querySelector('strong')?.textContent.trim() === 'Peers')
                    ?.querySelector('span');
                if (!peersSpan) return false;
                const n = parseInt(peersSpan.textContent, 10);
                return Number.isFinite(n) && n >= 1;
            },
            { timeout: READY_TIMEOUT_MS, polling: 500 }
        );
        log('engine has >=1 peer connected — ready to push');
    } catch (e) {
        // Don't bail — adding anyway lets us prove the failure mode is
        // "fan-out to 0 peers" via the log diagnostics, rather than a
        // pre-condition timeout that hides what's actually broken.
        log('WARNING: timed out waiting for connected peer — adding task anyway so the failure shows up in logs as "pushing to 0 peers"');
    }

    // Identify the Tasks input via its placeholder so we don't
    // accidentally hit the (separate) topic/passphrase fields. There's
    // exactly one element with placeholder "Add a task…".
    const inputHandle = await page.waitForSelector('input[placeholder="Add a task…"]');
    await inputHandle.click({ clickCount: 3 });   // select existing text if any
    await inputHandle.type(TASK_TEXT, { delay: 25 });

    // The Add button is a sibling — click via XPath text match.
    const [addBtn] = await page.$$('button.primary');
    if (!addBtn) throw new Error('Add button not found');
    log(`clicking Add for task '${TASK_TEXT}'`);
    await addBtn.click();

    if (!EXPECT_TASK_TEXT) {
        log('EXPECT_TASK_TEXT is empty — keeping browser open (Ctrl+C to exit)');
        await new Promise(() => {});
        return;
    }

    log(`waiting for '${EXPECT_TASK_TEXT}' to land in the DOM (phone→web direction)`);
    try {
        await page.waitForFunction(
            text => {
                // Match against the rendered task list. Using innerText
                // catches both the Tasks panel and any debug copy of
                // the same string, but that's fine — what we're proving
                // is that the changeset reached the engine and made it
                // into the reactive view.
                return document.body.innerText.includes(text);
            },
            { timeout: EXPECT_TIMEOUT_MS, polling: 500 },
            EXPECT_TASK_TEXT
        );
        log(`OK — '${EXPECT_TASK_TEXT}' appeared on the web side (phone→web push works)`);
    } catch (e) {
        log(`FAIL — '${EXPECT_TASK_TEXT}' did not appear within ${EXPECT_TIMEOUT_MS}ms`);
        await browser.close();
        process.exit(2);
    }

    await browser.close();
}

main().catch(err => {
    log('FATAL:', err.stack || err.message || String(err));
    process.exit(1);
});
