/**
 * crdt.js — a from-scratch implementation of RGA (Replicated Growable Array),
 * the CRDT (Conflict-free Replicated Data Type) that powers the collaborative
 * text document. Based on Roh et al., "Replicated abstract data types:
 * Building blocks for collaborative applications" (JPDC 2011), implemented
 * here as an explicit causal tree rather than the paper's flat-array-with-scan
 * presentation — see "WHY A TREE, NOT A SCANNED ARRAY" below for why.
 *
 * WHY RGA (and not Operational Transformation)
 * ---------------------------------------------
 * OT requires a central authority to transform concurrent operations against
 * each other in a specific, carefully-proven order, and every client must
 * apply transformed ops in the exact order the server decides. CRDTs instead
 * guarantee that *any* two replicas which have seen the same set of
 * operations converge to the *same* document, regardless of the order those
 * operations were applied in (as long as causal dependencies are respected).
 * That "strong eventual consistency" property is what test/crdt.test.js
 * exhaustively fuzz-tests below.
 *
 * DATA MODEL
 * ----------
 * Every character insertion is a node with a globally-unique id (a Lamport
 * pair `[counter, siteId]`) and an `originId`: the id of the character it
 * was inserted immediately after (null = "the very start of the document").
 * Deletions never remove a node — they set `tombstone = true` — because a
 * *concurrent* insert from another site may still reference a just-deleted
 * node as its origin; physically removing it would strand that insert with
 * nowhere to attach. (See README.md "Known limitations" for the tombstone
 * garbage-collection trade-off this implies.)
 *
 * WHY A TREE, NOT A SCANNED ARRAY
 * --------------------------------
 * The RGA paper describes integration as: find `originId` in a flat array,
 * then linear-scan forward past any "sibling" (same origin) with a larger
 * id, and insert just before the first sibling with a smaller id. The first
 * version of this file did exactly that — and a fuzz test
 * (test/crdt.test.js, "fuzz: N replicas...") caught it producing genuinely
 * different final documents on different replicas. The bug: a sibling's own
 * *descendants* (things inserted after IT) sit physically between it and
 * the next sibling in the flat array, but a same-origin scan that only
 * checks "is this element's origin still `originId`?" stops as soon as it
 * hits the first descendant — because a descendant's origin is the sibling,
 * not the original `originId`. That truncates the scan early and different
 * replicas, having built up different descendant chains before receiving
 * each other's concurrent siblings, truncate it at different points and
 * diverge. (This is a well-known gotcha in naive RGA implementations.)
 *
 * The fix used here sidesteps the whole problem: represent the document as
 * an explicit tree. Every node stores its children directly, kept sorted by
 * descending id. Integrating an insert is just "insert into the parent's
 * children list at the correct sorted position" — O(children of that one
 * node), and there is no flat array to accidentally scan past a subtree
 * boundary. Reading the document back out is a pre-order traversal (visit a
 * node, then all its children before any sibling), which is exactly RGA's
 * intended linearization and is now correct by construction rather than by
 * getting a scan condition exactly right.
 *
 * INTEGRATION ALGORITHM
 * ----------------------
 * To integrate a remote insert (id, originId, value):
 *   1. Its causal dependency — the node with id === originId, unless
 *      originId is null (meaning "child of the virtual root") — must
 *      already be present locally. If the network delivered operations out
 *      of order and it isn't, the operation is buffered until that
 *      dependency arrives (see `pending` below), then flushed.
 *   2. Once the parent exists, insert a new child into the parent's
 *      `children` array at the position that keeps it sorted by descending
 *      id. Concurrent inserts under the same parent always end up in the
 *      same relative order on every replica, because that order depends
 *      only on the (globally comparable, never-reused) ids, never on
 *      arrival order.
 *
 * To integrate a remote delete(id): the target node must already exist
 * locally (otherwise buffer it exactly like an insert). Mark
 * tombstone = true. Deletes are idempotent — deleting an already-deleted id
 * is a silent no-op, which matters because at-least-once delivery can
 * redeliver a delete.
 */

'use strict';

const ROOT_KEY = 'ROOT';

/** Compare two ids (`[counter, site]`). Returns -1, 0, or 1. Higher counter
 * wins; site id is only a tiebreaker for the (in practice impossible, but
 * defensively handled) case of two sites racing to the same counter value. */
function compareIds(a, b) {
  if (a === null && b === null) return 0;
  if (a === null) return -1; // null (virtual root) sorts before everything
  if (b === null) return 1;
  const [ac, as] = a;
  const [bc, bs] = b;
  if (ac !== bc) return ac > bc ? 1 : -1;
  if (as === bs) return 0;
  return as > bs ? 1 : -1;
}

function idsEqual(a, b) {
  if (a === null || b === null) return a === b;
  return a[0] === b[0] && a[1] === b[1];
}

function idKey(id) {
  return id === null ? ROOT_KEY : `${id[0]}:${id[1]}`;
}

class RGA {
  /**
   * @param {string} siteId - globally-unique id for this replica (server
   *   assigns one per connection; tests use fixed ids like "A", "B").
   */
  constructor(siteId) {
    if (!siteId) throw new Error('RGA requires a siteId');
    this.siteId = siteId;
    this.counter = 0;

    /** idKey -> { id, value, tombstone, children: Node[] } (children sorted
     * by DESCENDING id, i.e. children[0] has the highest id). The virtual
     * root has id=null, an empty/irrelevant value, and is never tombstoned. */
    this.nodes = new Map();
    this.nodes.set(ROOT_KEY, { id: null, value: null, tombstone: true, children: [] });

    /** originId-key (or target-id-key for deletes) -> operations buffered
     * because their causal dependency hasn't arrived yet. */
    this.pending = new Map();
    /** every id we've ever integrated, for idempotent duplicate delivery. */
    this.seen = new Set();
  }

  // ---- local edits (called when the *local* user types / deletes) ----

  /** Generate + apply a local insert of `value` (single character) at
   * document position `pos` (0-based, counting only visible characters).
   * Returns the operation to broadcast to other replicas. */
  localInsert(pos, value) {
    if (typeof value !== 'string' || [...value].length !== 1) {
      throw new Error('localInsert requires exactly one character');
    }
    const originId = this._visiblePositionToOriginId(pos);
    this.counter += 1;
    const id = [this.counter, this.siteId];
    this._integrateInsert(id, originId, value);
    return { type: 'insert', id, originId, value };
  }

  /** Generate + apply a local delete of the visible character at `pos`.
   * Returns the operation to broadcast, or null if pos is out of range
   * (e.g. an empty document — a harmless no-op). */
  localDelete(pos) {
    const id = this._visiblePositionToId(pos);
    if (id === null) return null;
    this._integrateDelete(id);
    return { type: 'delete', id };
  }

  // ---- remote/replayed operations ----

  /** Integrate an operation that came from another replica (or from
   * replaying the persisted op-log on startup). Handles causal buffering
   * and duplicate suppression automatically. */
  applyRemote(op) {
    if (op.type === 'insert') {
      this._tryIntegrateInsert(op.id, op.originId, op.value);
    } else if (op.type === 'delete') {
      this._tryIntegrateDelete(op.id);
    } else {
      throw new Error(`unknown op type: ${op.type}`);
    }
  }

  // ---- internals: integration ----

  _tryIntegrateInsert(id, originId, value) {
    if (this.seen.has(idKey(id))) return; // duplicate delivery, ignore
    if (originId !== null && !this.nodes.has(idKey(originId))) {
      this._buffer(originId, { type: 'insert', id, originId, value });
      return;
    }
    this._integrateInsert(id, originId, value);
  }

  _tryIntegrateDelete(id) {
    if (!this.nodes.has(idKey(id))) {
      this._buffer(id, { type: 'delete', id });
      return;
    }
    this._integrateDelete(id);
  }

  _buffer(waitingOnId, op) {
    const key = idKey(waitingOnId);
    if (!this.pending.has(key)) this.pending.set(key, []);
    this.pending.get(key).push(op);
  }

  /** After a node with this id becomes available, flush any ops that were
   * waiting on it (inserts whose originId===id, and deletes whose
   * target===id share the same buffering key). */
  _flushPending(id) {
    const key = idKey(id);
    const waiting = this.pending.get(key);
    if (!waiting) return;
    this.pending.delete(key);
    for (const op of waiting) {
      if (op.type === 'insert') this._tryIntegrateInsert(op.id, op.originId, op.value);
      else this._tryIntegrateDelete(op.id);
    }
  }

  _integrateInsert(id, originId, value) {
    const key = idKey(id);
    if (this.seen.has(key)) return;
    this.seen.add(key);

    const parent = this.nodes.get(idKey(originId));
    const node = { id, value, tombstone: false, children: [] };
    this.nodes.set(key, node);

    // Insert into parent.children keeping it sorted by DESCENDING id, so
    // concurrent inserts under the same parent land in the same relative
    // order on every replica no matter what order they're integrated in.
    const siblings = parent.children;
    let i = 0;
    while (i < siblings.length && compareIds(siblings[i].id, id) > 0) i += 1;
    siblings.splice(i, 0, node);

    this._flushPending(id);
  }

  _integrateDelete(id) {
    const node = this.nodes.get(idKey(id));
    if (!node) return;
    node.tombstone = true;
    this._flushPending(id);
  }

  // ---- internals: traversal ----

  /** Pre-order walk of the whole causal tree (root's children, each
   * followed immediately by its own children, before any sibling) —
   * exactly RGA's document linearization. Calls `visit(node)` for every
   * *non-root* node in document order (tombstoned nodes included; the
   * caller decides whether to skip them). */
  _preOrderWalk(visit) {
    const stack = [];
    // Push root's children in ascending order so popping (LIFO) yields
    // descending order = correct document order (first-to-render on top of stack last).
    const root = this.nodes.get(ROOT_KEY);
    for (let i = root.children.length - 1; i >= 0; i--) stack.push(root.children[i]);
    while (stack.length > 0) {
      const node = stack.pop();
      visit(node);
      for (let i = node.children.length - 1; i >= 0; i--) stack.push(node.children[i]);
    }
  }

  /** Map a visible-character position to the id that should become a new
   * element's origin: the id of the visible character immediately before
   * `pos`, or null if inserting at the very start (or the document has no
   * visible characters yet). */
  _visiblePositionToOriginId(pos) {
    if (pos <= 0) return null;
    let seenVisible = 0;
    let lastVisibleId = null;
    let found = null;
    this._preOrderWalk((node) => {
      if (found) return;
      if (!node.tombstone) {
        seenVisible += 1;
        lastVisibleId = node.id;
        if (seenVisible === pos) found = node.id;
      }
    });
    return found !== null ? found : lastVisibleId; // pos beyond end -> after last visible char
  }

  /** Map a visible-character position to the id of that character itself
   * (used for delete). Returns null if pos is out of range. */
  _visiblePositionToId(pos) {
    let seenVisible = 0;
    let result = null;
    this._preOrderWalk((node) => {
      if (result) return;
      if (!node.tombstone) {
        if (seenVisible === pos) result = node.id;
        seenVisible += 1;
      }
    });
    return result;
  }

  /** Reconstruct the current visible text. */
  toText() {
    let out = '';
    this._preOrderWalk((node) => {
      if (!node.tombstone) out += node.value;
    });
    return out;
  }

  /** Number of operations still waiting on a causal dependency that hasn't
   * arrived yet. A fully-synced replica has 0. Exposed for tests/diagnostics. */
  pendingCount() {
    let n = 0;
    for (const arr of this.pending.values()) n += arr.length;
    return n;
  }

  /** Full structural snapshot (pre-order, so every node appears after its
   * origin), suitable for JSON persistence or bootstrapping a newly
   * connected client without replaying the entire op-log. */
  snapshot() {
    const elements = [];
    this._preOrderWalk((node) => {
      elements.push({ id: node.id, value: node.value, originId: this._originOf(node), tombstone: node.tombstone });
    });
    return { siteId: this.siteId, counter: this.counter, elements };
  }

  /** Find the originId of `node` by locating which node's children array
   * contains it. Only used for snapshot serialization (infrequent), so a
   * linear scan is an acceptable trade-off against storing a redundant
   * back-pointer on every node. */
  _originOf(node) {
    for (const [key, candidate] of this.nodes) {
      if (candidate.children.includes(node)) return key === ROOT_KEY ? null : candidate.id;
    }
    return null;
  }

  /** Rebuild an RGA from a previously-produced snapshot(). `elements` MUST
   * be in an order where every node appears after its originId (pre-order
   * satisfies this; so does the on-disk op-log, since a node's origin must
   * have existed before the node could be created). */
  static fromSnapshot(snap) {
    const rga = new RGA(snap.siteId);
    rga.counter = snap.counter;
    for (const el of snap.elements) {
      rga._integrateInsert(el.id, el.originId, el.value);
      if (el.tombstone) rga._integrateDelete(el.id);
    }
    return rga;
  }
}

// Export for Node (server + node:test) and also attach to `window`/`self`
// when loaded directly via a <script> tag in the browser, so the exact same
// algorithm implementation runs on both sides with zero build step.
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { RGA, compareIds, idsEqual, idKey };
}
if (typeof window !== 'undefined') {
  window.CollabCRDT = { RGA, compareIds, idsEqual, idKey };
}
