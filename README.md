# CollabText — a real-time collaborative text editor with a from-scratch CRDT

A minimal Google-Docs-style plain-text editor: multiple people can open the
same document and type at the same time, and everyone converges to the same
result, even if their edits land on the network in a different order than
they were made. There is no operational-transform server, no `ws` package,
no database — the CRDT algorithm, the WebSocket protocol, and the
persistence layer are all implemented from scratch in plain Node.js, using
only the standard library. Zero runtime dependencies (`package.json`'s
`dependencies` is `{}`).

## Why this project

Real-time collaborative editing is the canonical "genuinely hard"
distributed-systems problem hiding behind a UI simple enough to demo in ten
seconds: a `<textarea>`. Under the hood it requires:

- **A conflict-free replicated data type (CRDT)** so that two people typing
  in the same spot at the same moment don't corrupt or silently drop each
  other's edits, and so every replica arrives at an *identical* document
  without a central authority resolving disputes op-by-op.
- **A real-time transport** — here, a WebSocket connection whose handshake
  and framing are implemented by hand, because relying on a library would
  hide exactly the part of "real-time web apps" that's worth understanding:
  how a byte stream becomes a sequence of discrete messages, and what a
  server must defend against (unmasked frames, oversized declared lengths,
  reserved bits) to not fall over when a client misbehaves.
- **Durable persistence** that survives a crash without either losing recent
  edits or paying an ever-growing replay cost on every restart.

This is also, honestly, a project where the first implementation had a real
bug that only a property-based (fuzz) test caught — see "A bug I actually
caught while testing" below. That's included deliberately: it's the kind of
thing that actually happens when you build a CRDT from scratch, and pretending
otherwise would undersell how easy it is to get subtly, silently wrong.

## Architecture

```
                     ┌─────────────────────────┐
   Browser tab A     │        server.js         │      Browser tab B
 ┌───────────────┐   │  ┌────────────────────┐  │   ┌───────────────┐
 │ <textarea>    │   │  │  DocumentManager   │  │   │ <textarea>    │
 │ + RGA (crdt.js)│◄─┼─►│  (one RGA per doc, │◄─┼──►│ + RGA (crdt.js)│
 │ mirrored from  │ WS │  per-doc op queue) │ WS │  │ mirrored from  │
 │ server's crdt.js│  │  └─────────┬──────────┘  │   │ server's crdt.js│
 └───────────────┘   │            │             │   └───────────────┘
                      │            ▼             │
                      │  ┌────────────────────┐  │
                      │  │  DocumentStore      │  │
                      │  │  oplog.jsonl +      │  │
                      │  │  snapshot.json      │  │
                      │  └────────────────────┘  │
                      │                          │
                      │  wsServer.js: hand-rolled│
                      │  RFC 6455 handshake +    │
                      │  frame parser/encoder    │
                      └─────────────────────────┘
```

- **`src/crdt.js`** — the RGA (Replicated Growable Array) CRDT. The document
  is a tree: every inserted character is a node whose parent is "the
  character it was typed immediately after". Concurrent insertions under the
  same parent are ordered deterministically by comparing globally-unique,
  never-reused ids — so every replica, no matter what order it hears about
  concurrent edits, sorts them identically. Deletions are tombstones, not
  physical removals, so a concurrent edit that references a just-deleted
  character as its insertion point still has somewhere to attach. The exact
  same file is served to the browser at `/crdt.js` (see `server.js`), so the
  client and server run one implementation, not two that could drift apart.

- **`src/wsServer.js`** — a WebSocket server built directly on Node's `http`
  `upgrade` event and raw sockets: the Sec-WebSocket-Accept handshake
  (SHA-1 + base64, per RFC 6455 §1.3), and an incremental frame parser that
  correctly handles masking, the 7/16/64-bit payload length encoding,
  fragmented messages, and partial TCP delivery (a frame's bytes can arrive
  split across any number of `data` events — the parser is a small state
  machine, not a "wait for everything then parse" function).

- **`src/persistence.js`** — an append-only, per-document operation log
  (`oplog.jsonl`) with periodic snapshot compaction, so persisting an edit is
  one line appended to a file (not a full-document rewrite), and startup
  replay time is bounded by "ops since the last snapshot", not "every edit
  ever made to this document".

- **`src/documentManager.js`** — owns the live CRDT state and connected
  sockets per document, and serializes all mutation of one document through
  a per-document promise chain, so persistence order always matches
  integration order even under genuinely concurrent inbound messages.

- **`public/`** — the browser UI: a `<textarea>`, a diff-based translator
  from raw DOM `input` events into per-character RGA operations, and a
  WebSocket client using the browser's native `WebSocket`.

## Setup & run

Requires Node.js ≥ 20 (developed and tested against Node 22). No
`npm install` step is needed — there are no dependencies.

```bash
node src/server.js
# or: npm start
# CollabText listening on http://localhost:8080 (data dir: ./data)
```

Open `http://localhost:8080/?doc=demo` in two browser tabs (or two
different browsers) and type in both — edits sync live. Different `?doc=`
values are independent documents. `COLLABTEXT_DATA_DIR` and `PORT`
environment variables override where documents are persisted and which
port the server listens on.

## Running the tests

```bash
node --test
```

48 tests, zero test dependencies (Node's built-in `node:test` +
`node:assert`), covering:

- `test/crdt.test.js` — the CRDT algorithm in isolation: sequential edits,
  duplicate-delivery idempotency, out-of-order/causal-buffering delivery,
  concurrent inserts at the same position, inserting after a tombstoned
  character, snapshot round-tripping, and a **randomized fuzz test** that
  runs 4 simulated replicas through independently-generated concurrent
  edits delivered to each replica in a different random order, asserting
  all 4 converge to an identical document.
- `test/wsServer.test.js` — the WebSocket protocol: the RFC 6455 worked
  handshake example, byte-at-a-time frame delivery, multi-frame-per-chunk
  delivery, fragmented-message reassembly, the extended length encodings,
  and rejecting unmasked frames / oversized declared lengths / reserved
  bits.
- `test/persistence.test.js` — op-log append/replay, snapshot compaction,
  tolerating a truncated final line (simulated crash mid-write), and
  rejecting a path-traversal document id.
- `test/documentManager.test.js` — including a dedicated concurrency test
  that fires ten inserts from two different "clients" at the same document
  via `Promise.all` and verifies the persisted op-log replays to exactly
  the same text the live CRDT converged to.
- `test/integration.test.js` — end-to-end tests that boot a **real** server
  on a real TCP socket and drive it with Node's built-in `WebSocket` client:
  multi-client convergence over the actual wire protocol, a late joiner
  receiving full document state, **surviving a full process restart**
  (kills the server, boots a new one against the same data directory,
  confirms the document is intact), a rate-limiter flood test, and static
  file / path-traversal checks.

## A bug I actually caught while testing

The first version of the CRDT followed the RGA paper's own presentation
literally: a flat array of elements, and integrating an insert meant
scanning forward past "sibling" elements (same insertion parent) with a
larger id. The very first fuzz test run (4 replicas, random concurrent
edits, random delivery order) failed: two replicas ended up with
`"psauznvvtgwcfvj"` and `"psauzvfvnvtgjwc"` — the same characters, in a
different order. The bug: a sibling's own descendants (things inserted
*after* it) sit physically between it and the next sibling in a flat
array, and a "same parent" scan condition stops as soon as it hits the
first descendant, because a descendant's parent is the sibling, not the
original target. Different replicas built up different descendant chains
before hearing about each other's concurrent edits, so the scan truncated
at different points on each one, and they diverged — a real, silent
correctness bug, not a crash, which is exactly the dangerous kind.

The fix: stop using a flat array with a scan condition at all. Represent
the document as an explicit tree (every node stores its own children,
kept sorted by descending id) and read the document back out with a
pre-order traversal. Insertion becomes "put this into the correct sorted
position among this one parent's direct children" — there's no subtree
boundary to accidentally scan past, because the tree structure encodes it
directly. Re-ran the same fuzz test 2,000 times with different random
seeds afterward with zero failures before trusting it enough to build the
rest of the server on top of it.

## Known limitations (deliberately out of scope)

- **No tombstone garbage collection.** Deleted characters are kept forever
  as tombstones so concurrent inserts can still reference them. A
  long-lived, heavily-edited document's tombstone count grows without
  bound. A real system would add this once all connected replicas have
  acknowledged a delete (or after a configurable retention window), which
  needs a small acknowledgment protocol this project doesn't implement.
- **Per-character operations.** Every inserted or deleted character is its
  own CRDT operation and its own JSON message. This keeps the algorithm
  and the protocol simple to reason about and test, at the cost of more
  messages for large pastes than a batching scheme would produce.
- **Single-process, in-memory document state.** `DocumentManager` holds
  every open document's live CRDT in one Node process's memory. Scaling
  beyond one process would need either sharding documents across
  processes/machines by doc id, or moving the "current CRDT state" into a
  shared store — the append-only log design here would support either
  without changing the CRDT itself.
- **No authentication/authorization.** Anyone who can reach the server can
  open and edit any document id. Fine for a local demo or an internal tool
  behind existing network access controls; a real deployment would add
  per-document access control before the WebSocket upgrade is accepted.

## Security considerations addressed

- WebSocket frames are size-capped (`MAX_FRAME_PAYLOAD`, `MAX_MESSAGE_BYTES`
  in `src/wsServer.js`) before any JSON parsing happens, so a single
  oversized frame can't exhaust server memory.
- Every client-supplied operation is re-validated server-side
  (`DocumentManager._validateOp`) independent of whether it parsed as JSON —
  the server never trusts client-side CRDT logic to have been followed
  honestly.
- Document ids are restricted to a safe character set and checked against
  path traversal both at the WebSocket URL route and inside
  `DocumentStore` itself (defense in depth — two independent checks).
- Per-connection message-rate limiting protects the event loop from a
  flooding client (see `test/integration.test.js`'s flood test).
- Static file serving resolves paths against the `public/` root and
  refuses anything that normalizes outside of it.

## License

MIT.
