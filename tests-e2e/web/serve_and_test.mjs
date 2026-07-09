// Browser<->native sync e2e orchestrator.
//
// Boots the whole stack from scratch and runs the "boot" smoke scenario:
//   1. relay (TCP + WebSocket listeners)
//   2. native peer (dials the relay's QUIC addr, seeds `from-native`)
//   3. static HTTP server for the wasm page
//   4. headless Chrome via puppeteer-core: load page -> e2e_init ->
//      poll e2e_status() until relayConnected.
//
// All children are spawned in their own process group and killed on exit.
// Richer sync scenarios are layered on top of this harness in Task 14.

import { spawn } from 'node:child_process';
import { createServer } from 'node:http';
import { readFile, access } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join, extname } from 'node:path';
import { tmpdir } from 'node:os';
import { mkdtempSync } from 'node:fs';
import puppeteer from 'puppeteer-core';
import {
  scenarioRoundTrip,
  scenarioRelayRestart,
  scenarioMultiGroup,
  scenarioReloadRejoin,
} from './e2e.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = join(__dirname, '..', '..');
const PAGE_DIR = join(__dirname, 'page');

// Fixed ports so nothing races over ephemeral allocation.
const TCP_PORT = 41100;
const WS_PORT = 41101;
const HTTP_PORT = 41199;
const TOPIC = 'wavesync-e2e';
const PASSPHRASE = 'e2e-pass';
const STORE_NAME = 'e2e-browser';
// Second sync group (multi-group #93) for scenarios C/D. The native peer joins
// it (kind="e2e") when these are in its env; the browser joins it at runtime.
const GROUP2_TOPIC = 'wavesync-e2e-g2';
const GROUP2_PASS = 'e2e-g2-pass';

// Persistent relay identity: the relay generates-and-persists its keypair to
// this file on first boot and reloads it on every restart, so the PeerId is
// STABLE across a kill+restart. This is critical for scenario B — the browser's
// stored relay multiaddr embeds `/p2p/<peerId>`, so a fresh per-process random
// identity could never be redialed.
const RELAY_IDENTITY_FILE = join(
  mkdtempSync(join(tmpdir(), 'wavesync-e2e-relay-id-')),
  'relay-identity',
);

const children = [];

function spawnTracked(cmd, args, opts = {}) {
  // `detached: true` puts the child in its own process group so a
  // group-kill (negative pid) also reaps `cargo run`'s grandchild.
  const child = spawn(cmd, args, { detached: true, ...opts });
  children.push(child);
  return child;
}

function killAll() {
  for (const child of children) {
    if (child.exitCode !== null || child.signalCode !== null) continue;
    try {
      process.kill(-child.pid, 'SIGKILL');
    } catch {
      try {
        child.kill('SIGKILL');
      } catch {
        /* already gone */
      }
    }
  }
}

// Resolve a usable Chrome/Chromium. On this Arch-based host Chrome ships as
// `google-chrome-stable`; the standard `google-chrome` name and `chromium`
// are tried too so the harness is portable.
function resolveChrome() {
  const candidates = [
    process.env.PUPPETEER_EXECUTABLE_PATH,
    '/usr/bin/google-chrome',
    '/usr/bin/google-chrome-stable',
    '/usr/bin/chromium',
    '/usr/bin/chromium-browser',
    '/opt/google/chrome/chrome',
  ].filter(Boolean);
  for (const path of candidates) {
    if (existsSync(path)) return path;
  }
  throw new Error(
    `No Chrome/Chromium found. Tried: ${candidates.join(', ')}. ` +
      `Set PUPPETEER_EXECUTABLE_PATH.`,
  );
}

// Persistently echo a child's stdout/stderr line-by-line with a `[prefix]`
// tag for the lifetime of the process. Attached once at spawn so log output
// keeps flowing after any `waitForLine` has resolved (which detaches its own
// short-lived listeners).
function pipeOutput(child, prefix) {
  const echo = (buf) => {
    for (const line of buf.toString().split('\n')) {
      if (line) process.stdout.write(`[${prefix}] ${line}\n`);
    }
  };
  child.stdout.on('data', echo);
  child.stderr.on('data', echo);
}

// Read a child's stdout/stderr line-by-line until `predicate(line)` is truthy,
// or reject after `timeoutMs`. Line echoing is handled separately by
// `pipeOutput`; this only matches. All listeners it registers are detached on
// resolve/timeout/exit so repeated waits over a process's lifetime never leak
// `data`/`exit` handlers.
function waitForLine(child, prefix, predicate, timeoutMs) {
  return new Promise((resolve, reject) => {
    let settled = false;
    const cleanup = () => {
      clearTimeout(timer);
      child.stdout.off('data', onData);
      child.stderr.off('data', onData);
      child.off('exit', onExit);
    };
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(new Error(`${prefix}: timed out after ${timeoutMs}ms waiting for line`));
    }, timeoutMs);

    let matched;
    const onData = (buf) => {
      for (const line of buf.toString().split('\n')) {
        if (!line) continue;
        if (!settled && (matched = predicate(line)) != null && matched !== false) {
          settled = true;
          cleanup();
          resolve(matched === true ? line : matched);
          return;
        }
      }
    };
    const onExit = (code) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(new Error(`${prefix}: exited (code ${code}) before match`));
    };
    child.stdout.on('data', onData);
    child.stderr.on('data', onData);
    child.on('exit', onExit);
  });
}

// Await a child's exit (used after a SIGKILL so the port is released before a
// same-port restart).
function waitForExit(child) {
  return new Promise((resolve) => {
    if (child.exitCode !== null || child.signalCode !== null) return resolve();
    child.on('exit', () => resolve());
  });
}

// Pick a prebuilt release binary if present, else fall back to `cargo run`.
function binOrCargo(binName, cargoArgs) {
  const prebuilt = join(REPO_ROOT, 'target', 'release', binName);
  if (existsSync(prebuilt)) return { cmd: prebuilt, args: [] };
  return { cmd: 'cargo', args: cargoArgs };
}

// Relay controller. `start()` (re)spawns the relay on the fixed ports with the
// persistent identity file, waits for its PeerId + WebSocket listener, and
// exposes the dial addresses. Because the identity is persisted, `peerId` (and
// therefore both dial addresses) is stable across kill+restart — the property
// scenario B relies on. `kill()` SIGKILLs the process group and waits for exit
// so the ports are free before the next `start()`.
function makeRelayController() {
  let child = null;
  let peerId = null;

  async function start() {
    const { cmd, args } = binOrCargo('wavesync-relay', [
      'run',
      '-p',
      'wavesync_relay',
      '--release',
      '--',
    ]);
    child = spawnTracked(cmd, args, {
      cwd: REPO_ROOT,
      env: {
        ...process.env,
        LISTEN_ADDR: `/ip4/127.0.0.1/tcp/${TCP_PORT}`,
        WS_LISTEN_ADDR: `/ip4/127.0.0.1/tcp/${WS_PORT}/ws`,
        IDENTITY_FILE: RELAY_IDENTITY_FILE,
        // Circuit-relay reservations must carry a dialable address or peers
        // get `NoAddressesInReservation` and never form the browser<->native
        // circuit. Advertise BOTH transports: the native peer reaches the relay
        // over QUIC (udp), the browser over WebSocket (tcp/ws).
        EXTERNAL_ADDRESS: `/ip4/127.0.0.1/udp/${TCP_PORT}/quic-v1,/ip4/127.0.0.1/tcp/${WS_PORT}/ws`,
        METRICS_ADDR: '',
        RUST_LOG: process.env.RUST_LOG || 'info',
      },
    });
    pipeOutput(child, 'relay');
    const seenPeerId = await waitForLine(
      child,
      'relay',
      (line) => {
        const m = line.match(/Relay server PeerId:\s*([0-9A-Za-z]+)/);
        return m ? m[1] : false;
      },
      120000,
    );
    await waitForLine(
      child,
      'relay',
      (line) => line.includes('listening on WebSocket') || line.includes('/ws/p2p/'),
      30000,
    );
    // The persistent identity file must yield the same PeerId every restart.
    if (peerId && seenPeerId !== peerId) {
      throw new Error(`relay identity changed across restart: ${peerId} -> ${seenPeerId}`);
    }
    peerId = seenPeerId;
    return peerId;
  }

  async function kill() {
    if (!child) return;
    const dead = waitForExit(child);
    try {
      process.kill(-child.pid, 'SIGKILL');
    } catch {
      try {
        child.kill('SIGKILL');
      } catch {
        /* already gone */
      }
    }
    await dead;
    child = null;
  }

  return {
    start,
    kill,
    get peerId() {
      return peerId;
    },
    // Native engine is QUIC-first (plain TCP is opt-in); the relay auto-listens
    // QUIC on the same port number as its TCP listener.
    get quicAddr() {
      return `/ip4/127.0.0.1/udp/${TCP_PORT}/quic-v1/p2p/${peerId}`;
    },
    // Browsers dial the WebSocket listener.
    get browserAddr() {
      return `/ip4/127.0.0.1/tcp/${WS_PORT}/ws/p2p/${peerId}`;
    },
  };
}

// Native peer controller. Uses a STABLE db path so a restart (scenario B
// fallback) resumes from prior state rather than re-onboarding. `start()`
// spawns the peer and waits for its `READY` line; `restart()` kills and
// re-starts it against the same relay dial address.
function makeNativeController(relayDialAddr) {
  const dbDir = mkdtempSync(join(tmpdir(), 'wavesync-e2e-native-'));
  const dbPath = join(dbDir, 'native.db');
  let child = null;

  async function start() {
    const { cmd, args } = binOrCargo('web_e2e_peer', [
      'run',
      '-p',
      'wavesyncdb-e2e',
      '--bin',
      'web_e2e_peer',
      '--release',
    ]);
    child = spawnTracked(cmd, args, {
      cwd: REPO_ROOT,
      env: {
        ...process.env,
        RELAY_ADDR: relayDialAddr,
        TOPIC,
        PASSPHRASE,
        DB_PATH: dbPath,
        // Second group: the native peer joins it (kind="e2e") and seeds/echoes
        // `e2e_group_items` for scenarios C/D.
        GROUP2_TOPIC,
        GROUP2_PASS,
        RUST_LOG: process.env.RUST_LOG || 'info',
      },
    });
    pipeOutput(child, 'native');
    await waitForLine(child, 'native', (line) => line.trim() === 'READY', 120000);
  }

  async function kill() {
    if (!child) return;
    const dead = waitForExit(child);
    try {
      process.kill(-child.pid, 'SIGKILL');
    } catch {
      try {
        child.kill('SIGKILL');
      } catch {
        /* already gone */
      }
    }
    await dead;
    child = null;
  }

  async function restart() {
    await kill();
    await start();
  }

  return { start, restart };
}

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.wasm': 'application/wasm',
  '.json': 'application/json',
};

function startStaticServer() {
  const server = createServer(async (req, res) => {
    try {
      const urlPath = decodeURIComponent(new URL(req.url, 'http://x').pathname);
      const rel = urlPath === '/' ? '/index.html' : urlPath;
      const filePath = join(PAGE_DIR, rel);
      // Contain traversal to PAGE_DIR.
      if (!filePath.startsWith(PAGE_DIR)) {
        res.writeHead(403).end('forbidden');
        return;
      }
      await access(filePath);
      const body = await readFile(filePath);
      res.writeHead(200, { 'content-type': MIME[extname(filePath)] || 'application/octet-stream' });
      res.end(body);
    } catch {
      res.writeHead(404).end('not found');
    }
  });
  return new Promise((resolve) => server.listen(HTTP_PORT, '127.0.0.1', () => resolve(server)));
}

function launchBrowser() {
  return puppeteer.launch({
    executablePath: resolveChrome(),
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  });
}

// Fresh page with console/pageerror piped to our stdout and the wasm module
// loaded (window.e2eReady === true).
async function openPage(browser) {
  const page = await browser.newPage();
  page.on('console', (msg) => process.stdout.write(`[page] ${msg.text()}\n`));
  page.on('pageerror', (err) => process.stdout.write(`[page-error] ${err.message}\n`));
  await page.goto(`http://127.0.0.1:${HTTP_PORT}/index.html`, { waitUntil: 'load' });
  await page.waitForFunction('window.e2eReady === true', { timeout: 30000 });
  return page;
}

// Existing boot smoke check: a browser initializes and reaches relayConnected.
async function runBootScenario(browserRelayAddr) {
  const browser = await launchBrowser();
  try {
    const page = await openPage(browser);
    await page.evaluate(
      (addr, topic, pass, store) => window.e2e_init(addr, topic, pass, store),
      browserRelayAddr,
      TOPIC,
      PASSPHRASE,
      STORE_NAME,
    );

    // Condition-based poll: relayConnected within 30s.
    const deadline = Date.now() + 30000;
    let status;
    for (;;) {
      status = JSON.parse(await page.evaluate('window.e2e_status()'));
      if (status.relayConnected) break;
      if (Date.now() > deadline) {
        throw new Error(`relay never connected. last status: ${JSON.stringify(status)}`);
      }
      await new Promise((r) => setTimeout(r, 500));
    }
    process.stdout.write(`[boot] PASS relayConnected=true status=${JSON.stringify(status)}\n`);
  } finally {
    await browser.close();
  }
}

// Scenarios A + B share one browser page: scenario A initializes and connects,
// scenario B kills/restarts the relay and asserts the SAME page reconnects
// (no reload). `store-a` is a fresh IndexedDB name distinct from the boot run's.
async function runScenarios(relayCtl, nativeCtl) {
  const browser = await launchBrowser();
  const durations = {};
  try {
    const page = await openPage(browser);
    const cfg = {
      relayAddr: relayCtl.browserAddr,
      topic: TOPIC,
      passphrase: PASSPHRASE,
      store: 'e2e-store-a',
    };

    process.stdout.write('[orch] running scenario A (round-trip)...\n');
    let t0 = Date.now();
    await scenarioRoundTrip(page, cfg);
    durations.roundTrip = Date.now() - t0;
    process.stdout.write(`[orch] scenario A passed in ${(durations.roundTrip / 1000).toFixed(1)}s\n`);

    process.stdout.write('[orch] running scenario B (relay restart)...\n');
    t0 = Date.now();
    const bResult = await scenarioRelayRestart(page, relayCtl, nativeCtl);
    durations.relayRestart = Date.now() - t0;
    process.stdout.write(
      `[orch] scenario B passed in ${(durations.relayRestart / 1000).toFixed(1)}s` +
        `${bResult.nativeRestarted ? ' (native peer restarted)' : ''}\n`,
    );
  } finally {
    await browser.close();
  }
  return durations;
}

// Scenarios C + D share one browser page (a fresh persistent store distinct
// from A/B's): scenario C initializes the default group and joins group2;
// scenario D reloads the SAME page and asserts group2 auto-rejoins. A separate
// page from A/B keeps the multi-group state isolated from the relay-restart run.
async function runScenariosCD(relayCtl) {
  const browser = await launchBrowser();
  const durations = {};
  try {
    const page = await openPage(browser);
    const cfg = {
      relayAddr: relayCtl.browserAddr,
      topic: TOPIC,
      passphrase: PASSPHRASE,
      store: 'e2e-store-cd',
      group2Topic: GROUP2_TOPIC,
      group2Pass: GROUP2_PASS,
      defaultTable: 'e2e_items',
      group2Table: 'e2e_group_items',
    };

    process.stdout.write('[orch] running scenario C (multi-group isolation)...\n');
    let t0 = Date.now();
    await scenarioMultiGroup(page, cfg);
    durations.multiGroup = Date.now() - t0;
    process.stdout.write(
      `[orch] scenario C passed in ${(durations.multiGroup / 1000).toFixed(1)}s\n`,
    );

    process.stdout.write('[orch] running scenario D (reload auto-rejoin + force_resync)...\n');
    t0 = Date.now();
    await scenarioReloadRejoin(page, cfg);
    durations.reloadRejoin = Date.now() - t0;
    process.stdout.write(
      `[orch] scenario D passed in ${(durations.reloadRejoin / 1000).toFixed(1)}s\n`,
    );
  } finally {
    await browser.close();
  }
  return durations;
}

async function main() {
  let server;
  const relayCtl = makeRelayController();
  try {
    process.stdout.write('[orch] starting relay...\n');
    await relayCtl.start();
    process.stdout.write(`[orch] relay PeerId=${relayCtl.peerId}\n`);

    process.stdout.write('[orch] starting native peer...\n');
    const nativeCtl = makeNativeController(relayCtl.quicAddr);
    await nativeCtl.start();

    process.stdout.write('[orch] starting static server...\n');
    server = await startStaticServer();

    process.stdout.write('[orch] running boot scenario...\n');
    await runBootScenario(relayCtl.browserAddr);
    process.stdout.write('[orch] boot scenario passed\n');

    const durations = await runScenarios(relayCtl, nativeCtl);
    const cdDurations = await runScenariosCD(relayCtl);
    process.stdout.write(
      `[orch] all scenarios passed — durations(s): ` +
        `roundTrip=${(durations.roundTrip / 1000).toFixed(1)} ` +
        `relayRestart=${(durations.relayRestart / 1000).toFixed(1)} ` +
        `multiGroup=${(cdDurations.multiGroup / 1000).toFixed(1)} ` +
        `reloadRejoin=${(cdDurations.reloadRejoin / 1000).toFixed(1)}\n`,
    );
  } finally {
    if (server) server.close();
    killAll();
  }
}

main().then(
  () => {
    process.stdout.write('[orch] SUCCESS\n');
    process.exit(0);
  },
  (err) => {
    process.stderr.write(`[orch] FAILURE: ${err.stack || err}\n`);
    process.exit(1);
  },
);
