'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { RGA } = require('../src/crdt');

test('single-site sequential inserts produce the typed string', () => {
  const a = new RGA('A');
  for (const ch of 'hello') a.localInsert(a.toText().length, ch);
  assert.equal(a.toText(), 'hello');
});

test('single-site insert in the middle', () => {
  const a = new RGA('A');
  for (const ch of 'helo') a.localInsert(a.toText().length, ch);
  // insert 'l' between the two l's... wait there's only one l: helo -> hello
  a.localInsert(3, 'l'); // "hel|o" -> insert at index 3 -> "hello"... let's verify precisely below
  assert.equal(a.toText(), 'hello');
});

test('local delete removes the correct character', () => {
  const a = new RGA('A');
  for (const ch of 'hexllo') a.localInsert(a.toText().length, ch);
  assert.equal(a.toText(), 'hexllo');
  a.localDelete(2); // remove 'x'
  assert.equal(a.toText(), 'hello');
});

test('delete is idempotent under duplicate delivery', () => {
  const a = new RGA('A');
  const insOp = a.localInsert(0, 'x');
  const delOp = a.localDelete(0);
  const b = new RGA('B');
  b.applyRemote(insOp);
  b.applyRemote(delOp);
  b.applyRemote(delOp); // redelivered
  b.applyRemote(delOp); // redelivered again
  assert.equal(b.toText(), '');
  assert.equal(b.pendingCount(), 0);
});

test('duplicate insert delivery does not duplicate the character', () => {
  const a = new RGA('A');
  const op = a.localInsert(0, 'x');
  const b = new RGA('B');
  b.applyRemote(op);
  b.applyRemote(op);
  b.applyRemote(op);
  assert.equal(b.toText(), 'x');
});

test('two replicas converge after exchanging non-concurrent ops in order', () => {
  const a = new RGA('A');
  const b = new RGA('B');
  const op1 = a.localInsert(0, 'h');
  b.applyRemote(op1);
  const op2 = a.localInsert(1, 'i');
  b.applyRemote(op2);
  assert.equal(a.toText(), b.toText());
  assert.equal(a.toText(), 'hi');
});

test('out-of-order delivery is buffered and resolved once the dependency arrives', () => {
  const a = new RGA('A');
  const opH = a.localInsert(0, 'h');
  const opI = a.localInsert(1, 'i');
  const opJ = a.localInsert(2, '!');

  const b = new RGA('B');
  // Deliver in reverse order: the last op depends (transitively) on the first two.
  b.applyRemote(opJ);
  assert.equal(b.toText(), ''); // buffered, nothing visible yet
  assert.equal(b.pendingCount(), 1);
  b.applyRemote(opI);
  assert.equal(b.toText(), ''); // opI itself is now buffered on opH; opJ still buffered on opI
  b.applyRemote(opH);
  // now everything should cascade-flush
  assert.equal(b.toText(), 'hi!');
  assert.equal(b.pendingCount(), 0);
});

test('a delete that arrives before its insert is buffered correctly', () => {
  const a = new RGA('A');
  const insOp = a.localInsert(0, 'x');
  const delOp = a.localDelete(0);

  const b = new RGA('B');
  b.applyRemote(delOp); // arrives first: target doesn't exist yet
  assert.equal(b.pendingCount(), 1);
  b.applyRemote(insOp);
  assert.equal(b.toText(), ''); // insert then immediately-flushed delete
  assert.equal(b.pendingCount(), 0);
});

test('concurrent inserts at the same position converge identically on both replicas, regardless of delivery order', () => {
  // Both A and B start from the same base document "ac" and simultaneously
  // insert a character between 'a' and 'c' (position 1), without having
  // seen each other's operation yet. RGA must place them in a deterministic,
  // id-based order on BOTH replicas, whichever op each replica hears about
  // first.
  const a = new RGA('A');
  const base1 = a.localInsert(0, 'a');
  const base2 = a.localInsert(1, 'c');

  const b = new RGA('B');
  b.applyRemote(base1);
  b.applyRemote(base2);
  assert.equal(a.toText(), 'ac');
  assert.equal(b.toText(), 'ac');

  const opFromA = a.localInsert(1, 'X'); // A inserts X between a and c
  const opFromB = b.localInsert(1, 'Y'); // B (concurrently) inserts Y between a and c

  // Deliver in DIFFERENT orders to each replica to prove order-independence.
  a.applyRemote(opFromB); // A already has its own X; now learns B's Y
  b.applyRemote(opFromA); // B already has its own Y; now learns A's X

  // Both X and Y were inserted with the SAME origin ('a'), and 'c' is *also*
  // a same-origin sibling (it too was inserted right after 'a'). RGA's
  // convergence guarantee is that the final order is identical on every
  // replica, not that it matches naive "insertion order" intuition — with
  // three same-origin siblings the deterministic (highest-id-first) order
  // is fully determined by their ids, so exactly one string is correct, and
  // it's the same on both replicas.
  assert.equal(a.toText(), b.toText());
});

test('inserting after a tombstoned (deleted) character still works', () => {
  const a = new RGA('A');
  const opA = a.localInsert(0, 'a');
  const opB = a.localInsert(1, 'b');
  a.localInsert(2, 'c');
  a.localDelete(1); // delete 'b' -> "ac", but 'b' node stays as tombstone

  const b = new RGA('B');
  // Give B everything A has done so far via a snapshot-style replay
  // (snapshot() is pre-order, so origins always precede their children).
  for (const el of a.snapshot().elements) {
    b.applyRemote({ type: 'insert', id: el.id, originId: el.originId, value: el.value });
    if (el.tombstone) b.applyRemote({ type: 'delete', id: el.id });
  }
  assert.equal(b.toText(), 'ac');

  // Now insert referencing the tombstoned 'b' as an origin dependency
  // (this happens naturally when a user's cursor was positioned right after
  // 'b' before it got deleted by someone else).
  const opAfterB = { type: 'insert', id: [999, 'A'], originId: opB.id, value: 'Z' };
  a.applyRemote(opAfterB);
  b.applyRemote(opAfterB);
  assert.equal(a.toText(), b.toText());
  assert.equal(a.toText(), 'aZc');
});

test('fromSnapshot/snapshot round-trip preserves full state including tombstones', () => {
  const a = new RGA('A');
  const opH = a.localInsert(0, 'h');
  a.localInsert(1, 'i');
  a.localDelete(0);
  const snap = a.snapshot();
  const restored = require('../src/crdt').RGA.fromSnapshot(snap);
  assert.equal(restored.toText(), a.toText());
  assert.equal(restored.snapshot().elements.length, snap.elements.length);
  // A subsequent remote op referencing a tombstoned origin must still work
  // after restoring from snapshot (proves tombstones, not just visible text,
  // were preserved).
  const opRef = { type: 'insert', id: [50, 'C'], originId: opH.id, value: 'Q' };
  restored.applyRemote(opRef);
  assert.ok(restored.toText().includes('Q'));
});

test('fuzz: N replicas apply the same random concurrent edit stream in random per-replica order and converge', () => {
  const SITES = ['A', 'B', 'C', 'D'];
  const replicas = new Map(SITES.map((s) => [s, new RGA(s)]));
  const allOps = []; // { siteId, op }

  // Seed a common base so there's something to edit concurrently.
  const seedSite = replicas.get('A');
  for (const ch of 'start') {
    const op = seedSite.localInsert(seedSite.toText().length, ch);
    allOps.push({ from: 'A', op });
  }
  for (const [id, r] of replicas) {
    if (id === 'A') continue;
    for (const { op } of allOps) r.applyRemote(op);
  }

  // Each site generates a handful of local edits against ITS OWN current
  // view (which may lag behind others — that's the point: genuine
  // concurrency), without seeing anyone else's edits yet.
  let seed = 12345;
  function rand() {
    // deterministic LCG so a failing run is reproducible
    seed = (seed * 1103515245 + 12345) & 0x7fffffff;
    return seed / 0x7fffffff;
  }

  for (const siteId of SITES) {
    const r = replicas.get(siteId);
    const editCount = 3 + Math.floor(rand() * 4);
    for (let i = 0; i < editCount; i++) {
      const len = r.toText().length;
      const doDelete = len > 0 && rand() < 0.3;
      if (doDelete) {
        const pos = Math.floor(rand() * len);
        const op = r.localDelete(pos);
        if (op) allOps.push({ from: siteId, op });
      } else {
        const pos = Math.floor(rand() * (len + 1));
        const ch = String.fromCharCode(97 + Math.floor(rand() * 26));
        const op = r.localInsert(pos, ch);
        allOps.push({ from: siteId, op });
      }
    }
  }

  // Now deliver EVERY op to EVERY OTHER replica in a distinct random order
  // per destination replica, simulating an unreliable, reordering network.
  function shuffled(arr) {
    const out = arr.slice();
    for (let i = out.length - 1; i > 0; i--) {
      const j = Math.floor(rand() * (i + 1));
      [out[i], out[j]] = [out[j], out[i]];
    }
    return out;
  }

  for (const destSiteId of SITES) {
    const dest = replicas.get(destSiteId);
    const inbound = allOps.filter((e) => e.from !== destSiteId);
    for (const { op } of shuffled(inbound)) dest.applyRemote(op);
  }

  const texts = SITES.map((s) => replicas.get(s).toText());
  for (const s of SITES) {
    assert.equal(
      replicas.get(s).pendingCount(),
      0,
      `replica ${s} still has buffered ops after full delivery`,
    );
  }
  for (let i = 1; i < texts.length; i++) {
    assert.equal(
      texts[i],
      texts[0],
      `replica ${SITES[i]} diverged from replica ${SITES[0]}: "${texts[i]}" vs "${texts[0]}"`,
    );
  }
});

test('localDelete on an empty document returns null and does not throw', () => {
  const a = new RGA('A');
  assert.equal(a.localDelete(0), null);
});

test('inserting past the end of the document appends at the end', () => {
  const a = new RGA('A');
  a.localInsert(0, 'a');
  a.localInsert(100, 'b'); // way past the end
  assert.equal(a.toText(), 'ab');
});
