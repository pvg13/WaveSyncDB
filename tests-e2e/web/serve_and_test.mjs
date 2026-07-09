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

// Read a child's stdout/stderr line-by-line until `predicate(line)` is true,
// or reject after `timeoutMs`. Lines are echoed with a prefix for debugging.
function waitForLine(child, prefix, predicate, timeoutMs) {
  return new Promise((resolve, reject) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      reject(new Error(`${prefix}: timed out after ${timeoutMs}ms waiting for line`));
    }, timeoutMs);

    let matched;
    const onData = (buf) => {
      for (const line of buf.toString().split('\n')) {
        if (!line) continue;
        process.stdout.write(`[${prefix}] ${line}\n`);
        if (!settled && (matched = predicate(line)) != null && matched !== false) {
          settled = true;
          clearTimeout(timer);
          resolve(matched === true ? line : matched);
        }
      }
    };
    child.stdout.on('data', onData);
    child.stderr.on('data', onData);
    child.on('exit', (code) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(new Error(`${prefix}: exited (code ${code}) before match`));
    });
  });
}

// Pick a prebuilt release binary if present, else fall back to `cargo run`.
function binOrCargo(binName, cargoArgs) {
  const prebuilt = join(REPO_ROOT, 'target', 'release', binName);
  if (existsSync(prebuilt)) return { cmd: prebuilt, args: [] };
  return { cmd: 'cargo', args: cargoArgs };
}

async function startRelay() {
  const { cmd, args } = binOrCargo('wavesync-relay', [
    'run',
    '-p',
    'wavesync_relay',
    '--release',
    '--',
  ]);
  const child = spawnTracked(cmd, args, {
    cwd: REPO_ROOT,
    env: {
      ...process.env,
      LISTEN_ADDR: `/ip4/127.0.0.1/tcp/${TCP_PORT}`,
      WS_LISTEN_ADDR: `/ip4/127.0.0.1/tcp/${WS_PORT}/ws`,
      METRICS_ADDR: '',
      RUST_LOG: process.env.RUST_LOG || 'info',
    },
  });
  // Parse the PeerId, then confirm the WebSocket listener is up.
  const peerId = await waitForLine(
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
  return peerId;
}

async function startNativePeer(relayDialAddr) {
  const dbDir = mkdtempSync(join(tmpdir(), 'wavesync-e2e-'));
  const { cmd, args } = binOrCargo('web_e2e_peer', [
    'run',
    '-p',
    'wavesyncdb-e2e',
    '--bin',
    'web_e2e_peer',
    '--release',
  ]);
  const child = spawnTracked(cmd, args, {
    cwd: REPO_ROOT,
    env: {
      ...process.env,
      RELAY_ADDR: relayDialAddr,
      TOPIC,
      PASSPHRASE,
      DB_PATH: join(dbDir, 'native.db'),
      RUST_LOG: process.env.RUST_LOG || 'info',
    },
  });
  await waitForLine(child, 'native', (line) => line.trim() === 'READY', 120000);
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

async function runBootScenario(browserRelayAddr) {
  const browser = await puppeteer.launch({
    executablePath: resolveChrome(),
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  });
  try {
    const page = await browser.newPage();
    page.on('console', (msg) => process.stdout.write(`[page] ${msg.text()}\n`));
    page.on('pageerror', (err) => process.stdout.write(`[page-error] ${err.message}\n`));

    await page.goto(`http://127.0.0.1:${HTTP_PORT}/index.html`, { waitUntil: 'load' });
    await page.waitForFunction('window.e2eReady === true', { timeout: 30000 });

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

async function main() {
  let server;
  try {
    process.stdout.write('[orch] starting relay...\n');
    const relayPeerId = await startRelay();
    // The native engine is QUIC-first (plain TCP is opt-in); the relay
    // auto-listens QUIC on the same port number as its TCP listener, so the
    // native peer dials the QUIC addr. Browsers dial the WebSocket addr.
    const relayQuicAddr = `/ip4/127.0.0.1/udp/${TCP_PORT}/quic-v1/p2p/${relayPeerId}`;
    const browserRelayAddr = `/ip4/127.0.0.1/tcp/${WS_PORT}/ws/p2p/${relayPeerId}`;
    process.stdout.write(`[orch] relay PeerId=${relayPeerId}\n`);

    process.stdout.write('[orch] starting native peer...\n');
    await startNativePeer(relayQuicAddr);

    process.stdout.write('[orch] starting static server...\n');
    server = await startStaticServer();

    process.stdout.write('[orch] running boot scenario...\n');
    await runBootScenario(browserRelayAddr);

    process.stdout.write('[orch] boot scenario passed\n');
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
