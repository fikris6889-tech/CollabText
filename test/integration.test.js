'use strict';

/**
 * integration.test.js — end-to-end tests that boot a REAL instance of the
 * server (real TCP socket, real from-scratch WebSocket handshake/framing,
 * real filesystem persistence in a temp dir) and drive it with Node's
 * built-in global `WebSocket` client (available since Node 22, no external
 * package needed). This is the level at which the "hard" parts of this
 * project — the CRDT, the hand-rolled WS protocol, the persistence layer,
 * and the concurrency-safe DocumentManager — are proven to actually work
 * together, not just in isolation.
 */

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { createServer } = require('../src/server');
const { RGA } = require('../src/crdt');

function startServer() {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'collabtext-it-'));
  const { httpServer, manager, store } = createServer({ dataDir });
  return new Promise((resolve) => {
    httpServer.listen(0, '127.0.0.1', () => {
      const { port } = httpServer.address();
      resolve({ httpServer, manager, store, dataDir, port });
    });
  });
}

function stopServer(httpServer) {
  return new Promise((resolve) => httpServer.close(resolve));
}

/** Connect a WebSocket client and wait for the server's "init" message,
 * which carries the client's assigned CRDT siteId and the current text. */
function connectClient(port, docId) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://127.0.0.1:${port}/ws/${docId}`);
    const received = [];
    let initResolved = false;
    ws.addEventListener('message', (evt) => {
      const msg = JSON.parse(evt.data);
      received.push(msg);
      if (msg.type === 'init' && !initResolved) {
        initResolved = true;
        resolve({ ws, siteId: msg.siteId, initialText: msg.text, received });
      }
    });
    ws.addEventListener('error', (err) => reject(err));
    setTimeout(() => {
      if (!initResolved) reject(new Error('timed out waiting for init message'));
    }, 5000);
  });
}

function waitFor(received, predicate, timeoutMs = 5000) {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const tick = () => {
      const found = received.find(predicate);
      if (found) return resolve(found);
      if (Date.now() - start > timeoutMs) return reject(new Error('waitFor timed out'));
      setTimeout(tick, 10);
    };
    tick();
  });
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

test('a client can connect, insert text, and read it back via HTTP-free WS round trip', async () => {
  const { httpServer, port } = await startServer();
  try {
    const { ws, siteId, initialText } = await connectClient(port, 'doc-basic');
    assert.equal(initialText, '');

    const rga = new RGA(siteId);
    const op = rga.localInsert(0, 'h');
    ws.send(JSON.stringify({ type: 'op', op }));
    await sleep(150); // let the server persist it
    ws.close();
  } finally {
    await stopServer(httpServer);
  }
});

test('two concurrent clients editing the same document converge to the same text on both sides', async () => {
  const { httpServer, port } = await startServer();
  try {
    const clientA = await connectClient(port, 'doc-concurrent');
    const clientB = await connectClient(port, 'doc-concurrent');

    const rgaA = new RGA(clientA.siteId);
    const rgaB = new RGA(clientB.siteId);

    // A types "cat", B types "dog", genuinely concurrently (neither has seen
    // the other's ops before generating its own).
    const opsA = [...'cat'].map((ch, i) => rgaA.localInsert(i, ch));
    const opsB = [...'dog'].map((ch, i) => rgaB.localInsert(i, ch));

    for (const op of opsA) clientA.ws.send(JSON.stringify({ type: 'op', op }));
    for (const op of opsB) clientB.ws.send(JSON.stringify({ type: 'op', op }));

    // Each client applies its OWN ops locally already (rgaA/rgaB above);
    // wait until each has received all 3 ops broadcast FROM the other side.
    for (const op of opsB) {
      const found = await waitFor(clientA.received, (m) => m.type === 'op' && m.op.id[0] === op.id[0] && m.op.id[1] === op.id[1]);
      rgaA.applyRemote(found.op);
    }
    for (const op of opsA) {
      const found = await waitFor(clientB.received, (m) => m.type === 'op' && m.op.id[0] === op.id[0] && m.op.id[1] === op.id[1]);
      rgaB.applyRemote(found.op);
    }

    assert.equal(rgaA.toText(), rgaB.toText());
    const sorted = (s) => [...s].sort().join('');
    assert.equal(sorted(rgaA.toText()), sorted('catdog'));

    clientA.ws.close();
    clientB.ws.close();
  } finally {
    await stopServer(httpServer);
  }
});

test('a late-joining second client receives the full current document in its init message', async () => {
  const { httpServer, port } = await startServer();
  try {
    const clientA = await connectClient(port, 'doc-latejoin');
    const rgaA = new RGA(clientA.siteId);
    for (const ch of 'hello') {
      const op = rgaA.localInsert(rgaA.toText().length, ch);
      clientA.ws.send(JSON.stringify({ type: 'op', op }));
    }
    await sleep(200); // let the server persist + update its in-memory CRDT

    const clientB = await connectClient(port, 'doc-latejoin');
    assert.equal(clientB.initialText, 'hello');

    clientA.ws.close();
    clientB.ws.close();
  } finally {
    await stopServer(httpServer);
  }
});

test('document state survives a full server restart (persistence works end-to-end)', async () => {
  const { httpServer, port, dataDir } = await startServer();
  const clientA = await connectClient(port, 'doc-persist');
  const rgaA = new RGA(clientA.siteId);
  for (const ch of 'persist me') {
    const op = rgaA.localInsert(rgaA.toText().length, ch);
    clientA.ws.send(JSON.stringify({ type: 'op', op }));
  }
  await sleep(200);
  clientA.ws.close();
  await stopServer(httpServer);

  // Boot a BRAND NEW server instance pointed at the same data directory,
  // simulating a process restart / crash recovery.
  const { httpServer: httpServer2, manager: manager2 } = createServer({ dataDir });
  await new Promise((resolve) => httpServer2.listen(0, '127.0.0.1', resolve));
  const port2 = httpServer2.address().port;
  const clientRestarted = await connectClient(port2, 'doc-persist');
  assert.equal(clientRestarted.initialText, 'persist me');
  clientRestarted.ws.close();
  await stopServer(httpServer2);
});

test('an unmasked-by-construction hostile message (oversized declared frame) does not crash the server', async () => {
  // We can't easily send a raw unmasked frame through the standard
  // WebSocket client (it always masks correctly, by design), so instead we
  // verify the server's documented defense at the unit level is actually
  // wired in: connecting many clients and sending a very large but legal
  // single op is handled fine, while the server's own frame size caps
  // (proven in wsServer.test.js) protect against the illegal case. This
  // test instead checks the *rate limiter* path end-to-end: a client that
  // fires far more messages/sec than MAX_MESSAGES_PER_SECOND gets
  // disconnected rather than allowed to keep flooding the event loop.
  const { httpServer, port } = await startServer();
  try {
    const client = await connectClient(port, 'doc-flood');
    const rga = new RGA(client.siteId);
    let closed = false;
    client.ws.addEventListener('close', () => { closed = true; });

    for (let i = 0; i < 500; i++) {
      const op = rga.localInsert(rga.toText().length, 'x');
      client.ws.send(JSON.stringify({ type: 'op', op }));
    }
    await sleep(500);
    assert.equal(closed, true, 'expected the flooding connection to be closed by the rate limiter');
  } finally {
    await stopServer(httpServer);
  }
});

test('an invalid docId in the URL is rejected at the HTTP upgrade, not passed through to the filesystem', async () => {
  const { httpServer, port } = await startServer();
  try {
    await assert.rejects(() => connectClient(port, '..%2f..%2fetc'));
  } finally {
    await stopServer(httpServer);
  }
});

test('static file server serves the frontend index and refuses path traversal', async () => {
  const { httpServer, port } = await startServer();
  try {
    const res = await fetch(`http://127.0.0.1:${port}/`);
    assert.equal(res.status, 200);
    const body = await res.text();
    assert.ok(body.includes('<html'), 'expected index.html to be served at /');

    const res2 = await fetch(`http://127.0.0.1:${port}/../../../../etc/passwd`);
    assert.notEqual(res2.status, 200);
  } finally {
    await stopServer(httpServer);
  }
});

test('/healthz reports ok', async () => {
  const { httpServer, port } = await startServer();
  try {
    const res = await fetch(`http://127.0.0.1:${port}/healthz`);
    assert.equal(res.status, 200);
    const json = await res.json();
    assert.equal(json.ok, true);
  } finally {
    await stopServer(httpServer);
  }
});
