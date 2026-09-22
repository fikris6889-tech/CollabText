/**
 * documentManager.js — owns the in-memory CRDT state for every open
 * document, the set of WebSocket connections currently editing each one,
 * and the persistence lifecycle (load on first access, append every op,
 * compact periodically).
 *
 * THE CONCURRENCY PROBLEM THIS FILE SOLVES
 * -------------------------------------------
 * Node is single-threaded, so two `applyRemote()` calls can never
 * interleave at the instruction level — but that does NOT mean this server
 * is free of race conditions. `fs.appendFileSync` is synchronous, so the
 * write itself is atomic with respect to other JS code, but nothing stops
 * two *logically* concurrent client messages (both arrived, both parsed,
 * both about to be processed) from having their CRDT-integration-then-
 * persist-then-broadcast sequences interleaved in a way that breaks the
 * invariant this server promises: "an op is broadcast to other clients only
 * after it is durably persisted", and "ops are persisted in the exact order
 * they were integrated into this replica's CRDT state". If handling one
 * message ever awaited something (a future switch to async I/O, a database
 * call, anything) before persisting, a second message for the *same
 * document* could jump the queue.
 *
 * The fix is a tiny per-document operation queue: `_withDocLock(docId, fn)`
 * chains `fn` calls for the same docId onto a running promise, so they
 * always execute in the order they were enqueued and never overlap, while
 * different documents remain fully concurrent with each other. This is
 * exercised directly in test/documentManager.test.js by firing many
 * concurrent inserts at the same document "at once" (Promise.all) and
 * asserting the op-log ends up in a single consistent, gap-free order that
 * replays to the same text the in-memory CRDT converged to.
 */

'use strict';

const { RGA } = require('./crdt');

class DocumentManager {
  /**
   * @param {import('./persistence').DocumentStore} store
   * @param {(docId: string, message: object, exceptSocketId: string|null) => void} broadcast
   *   Called with a JSON-serializable message to send to every OTHER
   *   connection subscribed to docId (the caller supplies the transport).
   */
  constructor(store, broadcast) {
    this.store = store;
    this.broadcast = broadcast;
    /** docId -> { rga: RGA, sockets: Set<connId>, lock: Promise } */
    this.docs = new Map();
  }

  /** Get (loading + replaying from disk if necessary) the live state for a
   * document. Safe to call repeatedly; state is cached in memory after the
   * first load. */
  _getOrLoad(docId) {
    let state = this.docs.get(docId);
    if (state) return state;

    const { snapshot, ops } = this.store.load(docId);
    const rga = snapshot ? RGA.fromSnapshot(snapshot) : new RGA('server');
    for (const op of ops) rga.applyRemote(op);

    state = { rga, sockets: new Set(), lock: Promise.resolve() };
    this.docs.set(docId, state);
    return state;
  }

  /** Serialize all mutating work for one document through a single chain of
   * promises, so persistence + broadcast for one op can never race with
   * another op on the SAME document (see file header). Different documents
   * proceed fully independently. */
  _withDocLock(docId, fn) {
    const state = this._getOrLoad(docId);
    const result = state.lock.then(() => fn(state));
    // Swallow rejections in the chain itself (so one failed op doesn't wedge
    // the queue forever) while still propagating the error to THIS caller.
    state.lock = result.catch(() => {});
    return result;
  }

  /** A connection joins a document. Returns the current text and a fresh
   * per-connection site id to use for all of that connection's local edits. */
  join(docId, connId) {
    return this._withDocLock(docId, (state) => {
      state.sockets.add(connId);
      return { text: state.rga.toText(), siteId: `s${connId}` };
    });
  }

  leave(docId, connId) {
    const state = this.docs.get(docId);
    if (state) state.sockets.delete(connId);
  }

  /** Integrate a remote op (already generated on the client with the siteId
   * we gave it in join()), persist it, and broadcast it to every other
   * connection on the same document. Returns the resulting document text
   * (mainly for tests / diagnostics). */
  async applyClientOp(docId, connId, op) {
    return this._withDocLock(docId, async (state) => {
      this._validateOp(op);
      state.rga.applyRemote(op);
      this.store.appendOp(docId, op);
      if (this.store.shouldCompact(docId)) {
        this.store.compact(docId, state.rga.snapshot());
      }
      this.broadcast(docId, { type: 'op', op }, connId);
      return state.rga.toText();
    });
  }

  /** Basic structural + bounds validation of a client-supplied operation,
   * independent of CRDT semantics (the CRDT itself is safe against replay/
   * reordering, but it trusts its inputs to be well-formed — this is the
   * boundary that keeps a malformed or hostile message from ever reaching
   * it). Throws on anything invalid. */
  _validateOp(op) {
    if (!op || typeof op !== 'object') throw new Error('op must be an object');
    if (op.type !== 'insert' && op.type !== 'delete') throw new Error('invalid op type');
    if (!Array.isArray(op.id) || op.id.length !== 2) throw new Error('invalid op id');
    const [counter, site] = op.id;
    if (!Number.isInteger(counter) || counter < 0) throw new Error('invalid op counter');
    if (typeof site !== 'string' || site.length === 0 || site.length > 128) {
      throw new Error('invalid op siteId');
    }
    if (op.type === 'insert') {
      if (op.originId !== null && !(Array.isArray(op.originId) && op.originId.length === 2)) {
        throw new Error('invalid originId');
      }
      if (typeof op.value !== 'string' || [...op.value].length !== 1) {
        throw new Error('insert value must be exactly one character');
      }
    }
  }

  /** Current text of a document, loading it if needed. Mainly for the
   * plain-HTTP status/debug endpoint. */
  getText(docId) {
    return this._getOrLoad(docId).rga.toText();
  }

  connectionCount(docId) {
    const state = this.docs.get(docId);
    return state ? state.sockets.size : 0;
  }

  /** Force-flush an in-memory document to a snapshot immediately, regardless
   * of the periodic-compaction threshold. Used on graceful shutdown so nothing
   * relies purely on the next process's op-log replay. */
  flushAll() {
    for (const [docId, state] of this.docs) {
      this.store.compact(docId, state.rga.snapshot());
    }
  }
}

module.exports = { DocumentManager };
