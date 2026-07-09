// Browser<->native e2e scenarios, driven by the orchestrator in
// serve_and_test.mjs. Two scenarios run in order after the boot smoke check:
//
//   A. scenarioRoundTrip   — browser<->native sync through the relay. This is
//      the test that would have caught the web protocol-id drift: it asserts a
//      real row crosses in BOTH directions (native seed reaches the browser;
//      a browser write is echoed back by the native peer).
//
//   B. scenarioRelayRestart — acceptance for the #30 relay auto-reconnect: kill
//      the relay, watch the browser drop (relayConnected=false), restart the
//      relay on the SAME ports and SAME peer identity, and assert the browser
//      redials on its own (relayConnected=true, reconnectAttempts bumped) with
//      NO page reload — then prove end-to-end sync resumes afterwards.
//
// Assertions poll on ROW STATE / status snapshots (never bare sleeps); peer
// discovery is asynchronous, so we never assume immediate connectivity.

// Condition-based waiter: resolves when `fn()` returns truthy, rejects on
// timeout. `everyMs` is the poll interval; `label` names the wait in errors.
export async function until(fn, timeoutMs, everyMs, label) {
  const t0 = Date.now();
  for (;;) {
    if (await fn()) return;
    if (Date.now() - t0 > timeoutMs) {
      throw new Error(`timeout after ${timeoutMs}ms waiting for ${label}`);
    }
    await new Promise((r) => setTimeout(r, everyMs));
  }
}

// Parsed status snapshot from the page.
async function status(page) {
  return JSON.parse(await page.evaluate('window.e2e_status()'));
}

// Parsed rows of `table` from the page's persistent store. Row shape is
// `{ pk, columns }` (see the page crate's `e2e_rows`).
async function rows(page, table) {
  return JSON.parse(await page.evaluate((t) => window.e2e_rows(t), table));
}

function hasPk(list, pk) {
  return list.some((r) => r.pk === pk);
}

// Scenario A — browser<->native round-trip through the relay.
//
// `cfg` = { relayAddr, topic, passphrase, store }. The page must already be
// loaded (window.e2eReady === true) but NOT yet initialized — this scenario
// owns the `e2e_init` call.
export async function scenarioRoundTrip(page, cfg) {
  process.stdout.write('[scenarioA] init browser client\n');
  await page.evaluate(
    (addr, topic, pass, store) => window.e2e_init(addr, topic, pass, store),
    cfg.relayAddr,
    cfg.topic,
    cfg.passphrase,
    cfg.store,
  );

  await until(async () => (await status(page)).relayConnected, 30_000, 500, 'relay connect');
  process.stdout.write('[scenarioA] relay connected\n');

  // native -> browser: the peer seeded `id="from-native"` at startup; it must
  // reach the browser via catch-up sync.
  await until(
    async () => hasPk(await rows(page, 'e2e_items'), 'from-native'),
    60_000,
    1_000,
    'native row reaches browser',
  );
  process.stdout.write('[scenarioA] native seed row reached browser\n');

  // browser -> native: write a row, then wait for the native peer's echo row
  // (`native-saw-from-browser`) to sync back to the browser.
  await page.evaluate(
    (t, pk, cols) => window.e2e_submit(t, pk, cols),
    'e2e_items',
    'from-browser',
    JSON.stringify([['body', 'hello']]),
  );
  await until(
    async () => hasPk(await rows(page, 'e2e_items'), 'native-saw-from-browser'),
    60_000,
    1_000,
    'browser row echoed by native',
  );
  process.stdout.write('[scenarioA] browser write echoed by native — PASS\n');
}

// Scenario B — relay restart / #30 auto-reconnect acceptance.
//
// `relayCtl` = { kill(), start(), browserAddr, quicAddr, peerId } from the
// orchestrator; `nativeCtl` = { restart() } as a fallback if the native peer
// does not re-announce on the restarted relay within the sync budget. The page
// is already connected from scenario A (same page — reconnect must happen with
// NO reload).
export async function scenarioRelayRestart(page, relayCtl, nativeCtl) {
  const before = (await status(page)).reconnectAttempts;
  process.stdout.write(`[scenarioB] reconnectAttempts before kill = ${before}\n`);

  await relayCtl.kill();
  process.stdout.write('[scenarioB] relay killed\n');
  await until(
    async () => !(await status(page)).relayConnected,
    20_000,
    500,
    'browser notices relay down',
  );
  process.stdout.write('[scenarioB] browser observed relayConnected=false\n');

  // Restart on the SAME ports and SAME peer identity so the browser's stored
  // relay multiaddr (which embeds /p2p/<peerId>) is still dialable.
  await relayCtl.start();
  process.stdout.write(`[scenarioB] relay restarted (peerId=${relayCtl.peerId})\n`);

  await until(
    async () => {
      const s = await status(page);
      return s.relayConnected && s.reconnectAttempts > before;
    },
    60_000,
    1_000,
    'browser reconnects without refresh',
  );
  const afterStatus = await status(page);
  process.stdout.write(
    `[scenarioB] browser reconnected: relayConnected=true reconnectAttempts=${afterStatus.reconnectAttempts}\n`,
  );

  // End-to-end sync must resume after the reconnect: a fresh browser write is
  // echoed by the native peer. The native peer has its own relay-reconnect
  // logic; if it has not re-announced within the budget we restart it once (a
  // native-side finding, reported in the task report) and re-wait.
  await page.evaluate(
    (t, pk, cols) => window.e2e_submit(t, pk, cols),
    'e2e_items',
    'post-restart',
    JSON.stringify([['body', 'back']]),
  );

  let nativeRestarted = false;
  try {
    await until(
      async () => hasPk(await rows(page, 'e2e_items'), 'native-saw-post-restart'),
      90_000,
      1_000,
      'sync resumes after relay restart',
    );
  } catch (e) {
    if (!nativeCtl) throw e;
    process.stdout.write(
      '[scenarioB] WARNING: native peer did not echo within 90s after relay restart; ' +
        'restarting native peer as fallback (native-side reconnect finding)\n',
    );
    await nativeCtl.restart();
    nativeRestarted = true;
    await until(
      async () => hasPk(await rows(page, 'e2e_items'), 'native-saw-post-restart'),
      90_000,
      1_000,
      'sync resumes after native peer restart',
    );
  }
  process.stdout.write(
    `[scenarioB] end-to-end sync resumed after restart — PASS${
      nativeRestarted ? ' (native peer restarted)' : ''
    }\n`,
  );
  return { nativeRestarted };
}
