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

// Parsed rows of `table` in the joined group `topic` (page crate's
// `e2e_group_rows`). Same `{ pk, columns }` shape.
async function groupRows(page, topic, table) {
  return JSON.parse(
    await page.evaluate((t, tbl) => window.e2e_group_rows(t, tbl), topic, table),
  );
}

function hasPk(list, pk) {
  return list.some((r) => r.pk === pk);
}

// Reject `promise` if it hasn't settled within `ms`. Used to bound the
// Argon2id-bearing `e2e_join_group` (page.evaluate itself has no timeout).
function withTimeout(promise, ms, label) {
  let timer;
  const guard = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`timeout after ${ms}ms: ${label}`)), ms);
  });
  return Promise.race([promise, guard]).finally(() => clearTimeout(timer));
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

// Scenario C — multi-group isolation (#93).
//
// `cfg` = { relayAddr, topic, passphrase, store, group2Topic, group2Pass,
// defaultTable, group2Table }. This scenario owns the page's `e2e_init` (fresh
// persistent store, distinct from A/B's) so it can be reloaded in scenario D.
//
// The browser holds the DEFAULT group (topic/passphrase, table `e2e_items`)
// AND joins a SECOND group (group2Topic/group2Pass, kind="e2e", table
// `e2e_group_items`) over the same client. The native peer seeds and echoes in
// both. We prove a real changeset crosses in the second group BOTH directions,
// then assert neither group's data leaks into the other.
export async function scenarioMultiGroup(page, cfg) {
  process.stdout.write('[scenarioC] init browser client (default group)\n');
  await page.evaluate(
    (addr, topic, pass, store) => window.e2e_init(addr, topic, pass, store),
    cfg.relayAddr,
    cfg.topic,
    cfg.passphrase,
    cfg.store,
  );
  await until(async () => (await status(page)).relayConnected, 30_000, 500, 'relay connect');

  // Default group must be live (native seed reaches the browser) before we
  // reason about isolation — otherwise "no leakage" is vacuous.
  await until(
    async () => hasPk(await rows(page, cfg.defaultTable), 'from-native'),
    60_000,
    1_000,
    'native default seed reaches browser',
  );
  process.stdout.write('[scenarioC] default group synced\n');

  // Join the second group. Runs Argon2id on the wasm thread — bounded at 30s.
  process.stdout.write('[scenarioC] joining group2 (Argon2id)...\n');
  await withTimeout(
    page.evaluate(
      (t, p, k) => window.e2e_join_group(t, p, k),
      cfg.group2Topic,
      cfg.group2Pass,
      'e2e',
    ),
    30_000,
    'join group2 (KDF)',
  );
  await until(
    async () => (await status(page)).joinedTopics.includes(cfg.group2Topic),
    30_000,
    500,
    'joinedTopics includes group2',
  );
  process.stdout.write('[scenarioC] joined group2\n');

  // native -> browser (group2): the peer seeded `g2-from-native` in its group2.
  await until(
    async () => hasPk(await groupRows(page, cfg.group2Topic, cfg.group2Table), 'g2-from-native'),
    60_000,
    1_000,
    'native group2 seed reaches browser',
  );
  process.stdout.write('[scenarioC] native group2 seed reached browser\n');

  // browser -> native (group2): write, then wait for the native echo.
  await page.evaluate(
    (t, tbl, pk, cols) => window.e2e_group_submit(t, tbl, pk, cols),
    cfg.group2Topic,
    cfg.group2Table,
    'g2-from-browser',
    JSON.stringify([['body', 'hello g2']]),
  );
  await until(
    async () =>
      hasPk(
        await groupRows(page, cfg.group2Topic, cfg.group2Table),
        'g2-native-saw-g2-from-browser',
      ),
    60_000,
    1_000,
    'group2 browser write echoed by native',
  );
  process.stdout.write('[scenarioC] group2 browser write echoed by native\n');

  // Isolation, both directions: no g2-* pk leaks into the default group, and no
  // default-group pk (from-native / from-browser / native-saw-*) leaks into
  // group2. The two groups even use different tables and different IndexedDB
  // stores, so any leak would be a real cross-group routing bug.
  const defaultRows = await rows(page, cfg.defaultTable);
  const g2Rows = await groupRows(page, cfg.group2Topic, cfg.group2Table);
  const leakedIntoDefault = defaultRows.filter((r) => r.pk.startsWith('g2-'));
  if (leakedIntoDefault.length > 0) {
    throw new Error(
      `isolation breach: default group contains g2 pks: ${leakedIntoDefault
        .map((r) => r.pk)
        .join(', ')}`,
    );
  }
  const leakedIntoGroup2 = g2Rows.filter(
    (r) => r.pk === 'from-native' || r.pk === 'from-browser' || r.pk.startsWith('native-saw-'),
  );
  if (leakedIntoGroup2.length > 0) {
    throw new Error(
      `isolation breach: group2 contains default pks: ${leakedIntoGroup2
        .map((r) => r.pk)
        .join(', ')}`,
    );
  }
  process.stdout.write('[scenarioC] cross-group isolation holds (both directions) — PASS\n');
}

// Scenario D — reload auto-rejoin + force_resync smoke (#93).
//
// Reuses the SAME page (and therefore the SAME persistent store) scenario C
// left joined to group2. A reload wipes all in-memory state; re-`e2e_init` with
// the same store must auto-rejoin group2 from its persisted join record WITHOUT
// the app calling `e2e_join_group` again — proving sync resumes on its own. We
// then write into the rejoined group and require a native echo, and finally
// exercise `e2e_force_resync` and require another echo, proving the engine
// survived the manual resync.
export async function scenarioReloadRejoin(page, cfg) {
  process.stdout.write('[scenarioD] reloading page (drops all in-memory state)...\n');
  await page.reload({ waitUntil: 'load' });
  await page.waitForFunction('window.e2eReady === true', { timeout: 30_000 });

  // Re-init the DEFAULT group with the SAME store: this is what triggers the
  // auto-rejoin of persisted groups. We deliberately do NOT call e2e_join_group.
  await page.evaluate(
    (addr, topic, pass, store) => window.e2e_init(addr, topic, pass, store),
    cfg.relayAddr,
    cfg.topic,
    cfg.passphrase,
    cfg.store,
  );
  await until(
    async () => (await status(page)).relayConnected,
    30_000,
    500,
    'relay reconnect after reload',
  );

  // Auto-rejoin: group2 appears in joinedTopics without an explicit join call.
  await until(
    async () => (await status(page)).joinedTopics.includes(cfg.group2Topic),
    30_000,
    500,
    'group2 auto-rejoined after reload',
  );
  // And its data survived the reload (group2's own IndexedDB store persists).
  await until(
    async () => hasPk(await groupRows(page, cfg.group2Topic, cfg.group2Table), 'g2-from-native'),
    30_000,
    1_000,
    'group2 rows survive reload',
  );
  process.stdout.write('[scenarioD] group2 auto-rejoined and rows survived reload\n');

  // A fresh write into the rejoined group must still be echoed by the native
  // peer — proving the rejoined group is fully live, not just present.
  await page.evaluate(
    (t, tbl, pk, cols) => window.e2e_group_submit(t, tbl, pk, cols),
    cfg.group2Topic,
    cfg.group2Table,
    'g2-post-reload',
    JSON.stringify([['body', 'after reload']]),
  );
  await until(
    async () =>
      hasPk(
        await groupRows(page, cfg.group2Topic, cfg.group2Table),
        'g2-native-saw-g2-post-reload',
      ),
    60_000,
    1_000,
    'post-reload write echoed by native',
  );
  process.stdout.write('[scenarioD] post-reload write echoed by native\n');

  // force_resync smoke: fire the manual resync, then prove the engine survived
  // by writing once more and requiring another native echo.
  process.stdout.write('[scenarioD] force_resync smoke...\n');
  await page.evaluate(() => window.e2e_force_resync());
  await page.evaluate(
    (t, tbl, pk, cols) => window.e2e_group_submit(t, tbl, pk, cols),
    cfg.group2Topic,
    cfg.group2Table,
    'g2-post-resync',
    JSON.stringify([['body', 'after resync']]),
  );
  await until(
    async () =>
      hasPk(
        await groupRows(page, cfg.group2Topic, cfg.group2Table),
        'g2-native-saw-g2-post-resync',
      ),
    60_000,
    1_000,
    'post-resync write echoed by native',
  );
  process.stdout.write('[scenarioD] force_resync survived — PASS\n');
}
