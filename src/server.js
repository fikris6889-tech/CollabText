/**
 * server.js — wires together the HTTP static file server, the from-scratch
 * WebSocket layer, and the DocumentManager into one running process.
 *
 * PROTOCOL (client <-> server, JSON messages over the WebSocket)
 * ------------------------------------------------------------
 * Connect to  ws://host:port/ws/<docId>
 *
 * Server -> Client, immediately on connect:
 *   { type: "init", siteId, text }
 *     siteId: this connection's unique CRDT site id (server-assigned, so two
 *     tabs/users can never collide even if they'd otherwise pick the same
 *     id). text: the document's current full text, for the client to render.
 *
 * Client -> Server:
 *   { type: "op", op: { type: "insert", id, originId, value } }
 *   { type: "op", op: { type: "delete", id } }
 *     The client generates the op locally (using its own RGA mirror, see
 *     public/client.js) and sends the resulting operation, NOT a raw
 *     "insert at position N" — the server never re-derives positions,
 *     which is what makes this safe under concurrency.
 *   { type: "cursor", pos }
 *     Ephemeral presence info (not persisted, not CRDT state) so peers can
 *     show where everyone's cursor is.
 *
 * Server -> Client (broadcast to every OTHER connection on the same doc):
 *   { type: "op", op }               -- relayed after being durably persisted
 *   { type: "cursor", connId, pos }  -- relayed cursor position
 *   { type: "presence", connId, event: "join"|"leave" }
 *
 * SECURITY NOTES
 * ---------------
 * - Every inbound WS message is size-capped by the frame parser
 *   (MAX_FRAME_PAYLOAD / MAX_MESSAGE_BYTES in wsServer.js) before it's even
 *   handed to JSON.parse, so a client can't OOM the server with one giant
 *   frame.
 * - `documentManager._validateOp` re-validates structure server-side —
 *   the server never trusts that a client-supplied op is well-formed just
 *   because it parsed as JSON.
 * - `docId` is taken from the URL path and passed straight to
 *   DocumentStore, which itself whitelists the character set (see
 *   persistence.js `_docDir`) to rule out path traversal.
 * - Static file serving resolves requested paths against the `public/`
 *   root and rejects anything that normalizes outside of it, so
 *   `GET /../server.js` can't read server source.
 * - A per-connection message-rate limiter guards against a single client
 *   flooding the event loop with a tight send loop (accidental infinite
 *   loop in a buggy client, or a hostile one).
 */

'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const { performHandshake, FrameParser, encodeTextFrame, encodeCloseFrame, encodePongFrame } = require('./wsServer');
const { DocumentManager } = require('./documentManager');
const { DocumentStore } = require('./persistence');

const PUBLIC_DIR = path.join(__dirname, '..', 'public');
const DATA_DIR = process.env.COLLABTEXT_DATA_DIR || path.join(__dirname, '..', 'data');
const PORT = Number(process.env.PORT) || 8080;
const MAX_MESSAGES_PER_SECOND = 200; // per connection; generous for real typing, hostile for a flood

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
};

function serveStatic(req, res) {
  let reqPath = decodeURIComponent(req.url.split('?')[0]);
  if (reqPath === '/') reqPath = '/index.html';

  // /crdt.js is served directly from src/crdt.js rather than a duplicated
  // copy under public/, so the browser runs the EXACT SAME CRDT
  // implementation that's unit- and fuzz-tested in test/crdt.test.js —
  // one source of truth, not two implementations that could quietly drift
  // apart.
  if (reqPath === '/crdt.js') {
    fs.readFile(path.join(__dirname, 'crdt.js'), (err, data) => {
      if (err) { res.writeHead(404).end('Not found'); return; }
      res.writeHead(200, { 'Content-Type': MIME_TYPES['.js'] }).end(data);
    });
    return;
  }

  const resolved = path.normalize(path.join(PUBLIC_DIR, reqPath));
  if (!resolved.startsWith(PUBLIC_DIR)) {
    res.writeHead(403).end('Forbidden');
    return;
  }
  fs.readFile(resolved, (err, data) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain' }).end('Not found');
      return;
    }
    const ext = path.extname(resolved);
    res.writeHead(200, { 'Content-Type': MIME_TYPES[ext] || 'application/octet-stream' });
    res.end(data);
  });
}

function createServer({ dataDir = DATA_DIR } = {}) {
  const store = new DocumentStore(dataDir);
  /** connId -> { socket, docId, sendFrame } */
  const connections = new Map();

  const manager = new DocumentManager(store, (docId, message, exceptConnId) => {
    const payload = encodeTextFrame(JSON.stringify(message));
    for (const [connId, conn] of connections) {
      if (connId === exceptConnId) continue;
      if (conn.docId !== docId) continue;
      try {
        conn.socket.write(payload);
      } catch {
        // socket already gone; its 'close' handler will clean up the map.
      }
    }
  });

  const httpServer = http.createServer((req, res) => {
    if (req.url === '/healthz') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true, openDocs: manager.docs.size }));
      return;
    }
    serveStatic(req, res);
  });

  httpServer.on('upgrade', (req, socket) => {
    const match = req.url.match(/^\/ws\/([A-Za-z0-9_-]{1,128})$/);
    if (!match) {
      socket.write('HTTP/1.1 404 Not Found\r\nConnection: close\r\n\r\n');
      socket.destroy();
      return;
    }
    const docId = match[1];
    if (!performHandshake(req, socket)) return;

    const connId = crypto.randomBytes(8).toString('hex');
    let messageTimestamps = [];

    const parser = new FrameParser({
      onMessage: (buf) => handleClientMessage(connId, docId, buf),
      onClose: () => closeConnection(connId, 1000),
      onPing: (payload) => {
        try {
          socket.write(encodePongFrame(payload));
        } catch {
          /* ignore */
        }
      },
      onError: ({ code, reason }) => closeConnection(connId, code, reason),
    });

    socket.on('data', (chunk) => parser.push(chunk));
    socket.on('error', () => closeConnection(connId, 1011));
    socket.on('close', () => finishClose(connId));

    connections.set(connId, { socket, docId });

    manager.join(docId, connId).then(({ text, siteId }) => {
      const conn = connections.get(connId);
      if (!conn) return; // client disconnected again before join() resolved
      conn.socket.write(encodeTextFrame(JSON.stringify({ type: 'init', siteId, text })));
      for (const [otherId, other] of connections) {
        if (otherId === connId || other.docId !== docId) continue;
        try {
          other.socket.write(encodeTextFrame(JSON.stringify({ type: 'presence', connId, event: 'join' })));
        } catch {
          /* ignore */
        }
      }
    });

    function handleClientMessage(id, doc, buf) {
      // Rate limit: drop (and eventually disconnect) a connection sending
      // an unreasonable number of messages per second.
      const now = Date.now();
      messageTimestamps.push(now);
      messageTimestamps = messageTimestamps.filter((t) => now - t < 1000);
      if (messageTimestamps.length > MAX_MESSAGES_PER_SECOND) {
        closeConnection(id, 1008, 'rate limit exceeded');
        return;
      }

      let msg;
      try {
        msg = JSON.parse(buf.toString('utf8'));
      } catch {
        return; // malformed JSON: ignore rather than crash the connection
      }

      if (msg && msg.type === 'op' && msg.op) {
        manager.applyClientOp(doc, id, msg.op).catch(() => {
          // Invalid op from this client (failed _validateOp, or a causal
          // dependency that can genuinely never resolve e.g. because it
          // references a document that was reset). We deliberately do not
          // crash or close the connection over one bad message.
        });
      } else if (msg && msg.type === 'cursor' && typeof msg.pos === 'number') {
        for (const [otherId, conn] of connections) {
          if (otherId === id || conn.docId !== doc) continue;
          try {
            conn.socket.write(encodeTextFrame(JSON.stringify({ type: 'cursor', connId: id, pos: msg.pos })));
          } catch {
            /* ignore */
          }
        }
      }
    }

    function closeConnection(id, code, reason) {
      const conn = connections.get(id);
      if (!conn) return;
      try {
        conn.socket.write(encodeCloseFrame(code, reason || ''));
        conn.socket.end();
      } catch {
        /* ignore */
      }
    }

    function finishClose(id) {
      const conn = connections.get(id);
      if (!conn) return;
      connections.delete(id);
      manager.leave(conn.docId, id);
      for (const [otherId, other] of connections) {
        if (other.docId !== conn.docId) continue;
        try {
          other.socket.write(encodeTextFrame(JSON.stringify({ type: 'presence', connId: id, event: 'leave' })));
        } catch {
          /* ignore */
        }
      }
    }
  });

  return { httpServer, manager, store, connections };
}

function main() {
  const { httpServer, manager } = createServer();
  httpServer.listen(PORT, () => {
    // eslint-disable-next-line no-console
    console.log(`collabtext-crdt listening on http://localhost:${PORT} (data dir: ${DATA_DIR})`);
  });

  const shutdown = () => {
    // eslint-disable-next-line no-console
    console.log('shutting down: flushing all open documents to disk...');
    manager.flushAll();
    httpServer.close(() => process.exit(0));
    // Force-exit if something keeps the event loop alive too long.
    setTimeout(() => process.exit(0), 3000).unref();
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

if (require.main === module) {
  main();
}

module.exports = { createServer };
