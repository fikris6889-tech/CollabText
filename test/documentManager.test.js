'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { DocumentManager } = require('../src/documentManager');
const { DocumentStore } = require('../src/persistence');
const { RGA } = require('../src/crdt');

function makeManager() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'collabtext-dm-'));
  const store = new DocumentStore(dir);
  const broadcasts = [];
  const manager = new DocumentManager(store, (docId, message, exceptConnId) => {
    broadcasts.push({ docId, message, exceptConnId });
  });
  return { manager, store, broadcasts, dir };
}

test('join() returns the current text and a unique per-connection siteId', async () => {
  const { manager } = makeManager();
  const a = await manager.join('doc1', 'conn-1');
  const b = await manager.join('doc1', 'conn-2');
  assert.equal(a.text, '');
  assert.equal(b.text, '');
  assert.notEqual(a.siteId, b.siteId);
});

test('applyClientOp persists the op and broadcasts it to everyone except the sender', async () => {
  const { manager, broadcasts } = makeManager();
  const { siteId } = await manager.join('doc1', 'conn-1');
  const rga = new RGA(siteId);
  const op = rga.localInsert(0, 'h');

  const text = await manager.applyClientOp('doc1', 'conn-1', op);
  assert.equal(text, 'h');
  assert.equal(broadcasts.length, 1);
  assert.equal(broadcasts[0].exceptConnId, 'conn-1');
  assert.deepEqual(broadcasts[0].message, { type: 'op', op });
});

test('a second manager instance reading the same store recovers full state', async () => {
  const { manager, store } = makeManager();
  const { siteId } = await manager.join('doc1', 'conn-1');
  const rga = new RGA(siteId);
  await manager.applyClientOp('doc1', 'conn-1', rga.localInsert(0, 'h'));
  await manager.applyClientOp('doc1', 'conn-1', rga.localInsert(1, 'i'));

  const manager2 = new DocumentManager(store, () => {});
  const { text } = await manager2.join('doc1', 'conn-9');
  assert.equal(text, 'hi');
});

test('applyClientOp rejects a structurally invalid op without corrupting state', async () => {
  const { manager } = makeManager();
  await manager.join('doc1', 'conn-1');
  await assert.rejects(() => manager.applyClientOp('doc1', 'conn-1', { type: 'insert', id: [1, 'x'], originId: null, value: 'ab' }));
  await assert.rejects(() => manager.applyClientOp('doc1', 'conn-1', { type: 'insert', id: 'not-an-array', originId: null, value: 'a' }));
  await assert.rejects(() => manager.applyClientOp('doc1', 'conn-1', { type: 'nonsense' }));
  assert.equal(manager.getText('doc1'), '');
});

test('a document id containing path-traversal characters is rejected, not silently written elsewhere', async () => {
  const { manager } = makeManager();
  const { siteId } = await manager.join('safe-doc', 'conn-1');
  const rga = new RGA(siteId);
  await assert.rejects(() => manager.applyClientOp('../evil', 'conn-1', rga.localInsert(0, 'x')));
});

test('CONCURRENCY: many "simultaneous" ops on the same document are serialized into one consistent order', async () => {
  const { manager, store } = makeManager();
  const { siteId: siteA } = await manager.join('doc1', 'conn-A');
  const { siteId: siteB } = await manager.join('doc1', 'conn-B');
  const rgaA = new RGA(siteA);
  const rgaB = new RGA(siteB);

  // Each "client" independently generates a batch of local inserts against
  // its OWN starting view (both start from "" since the doc is fresh), then
  // fires them at the manager concurrently via Promise.all — simulating two
  // browser tabs typing at the same moment with no coordination.
  const opsA = 'hello'.split('').map((ch, i) => rgaA.localInsert(i, ch));
  const opsB = 'world'.split('').map((ch, i) => rgaB.localInsert(i, ch));

  await Promise.all([
    ...opsA.map((op) => manager.applyClientOp('doc1', 'conn-A', op)),
    ...opsB.map((op) => manager.applyClientOp('doc1', 'conn-B', op)),
  ]);

  const finalText = manager.getText('doc1');

  // Independently replay the persisted op-log from scratch (a brand new
  // RGA, nothing shared with the live in-memory one) and confirm it
  // reproduces EXACTLY the in-memory result — proving persistence order
  // matched integration order with no interleaving corruption.
  const { ops } = store.load('doc1');
  assert.equal(ops.length, 10);
  const replay = new RGA('replay-check');
  for (const op of ops) replay.applyRemote(op);
  assert.equal(replay.toText(), finalText);
  assert.equal(replay.pendingCount(), 0);

  // Both original character sets must be present (nothing lost/duplicated).
  const sorted = [...finalText].sort().join('');
  const expected = [...'helloworld'].sort().join('');
  assert.equal(sorted, expected);
});

test('leave() removes a connection from the active socket set', async () => {
  const { manager } = makeManager();
  await manager.join('doc1', 'conn-1');
  await manager.join('doc1', 'conn-2');
  assert.equal(manager.connectionCount('doc1'), 2);
  manager.leave('doc1', 'conn-1');
  assert.equal(manager.connectionCount('doc1'), 1);
});

test('flushAll compacts every open document', async () => {
  const { manager, store } = makeManager();
  const { siteId } = await manager.join('doc1', 'conn-1');
  const rga = new RGA(siteId);
  await manager.applyClientOp('doc1', 'conn-1', rga.localInsert(0, 'x'));
  manager.flushAll();
  const { snapshot, ops } = store.load('doc1');
  assert.ok(snapshot);
  assert.equal(ops.length, 0);
});
