'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { DocumentStore, SNAPSHOT_EVERY_N_OPS } = require('../src/persistence');
const { RGA } = require('../src/crdt');

function tmpDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'collabtext-test-'));
}

test('appendOp + load round-trips a sequence of operations', () => {
  const store = new DocumentStore(tmpDir());
  const rga = new RGA('A');
  const op1 = rga.localInsert(0, 'h');
  const op2 = rga.localInsert(1, 'i');
  store.appendOp('doc1', op1);
  store.appendOp('doc1', op2);

  const { snapshot, ops } = store.load('doc1');
  assert.equal(snapshot, null);
  assert.equal(ops.length, 2);

  const replay = new RGA('replay');
  for (const op of ops) replay.applyRemote(op);
  assert.equal(replay.toText(), 'hi');
});

test('compact() writes a snapshot and truncates the op-log', () => {
  const store = new DocumentStore(tmpDir());
  const rga = new RGA('A');
  for (const ch of 'hello') {
    const op = rga.localInsert(rga.toText().length, ch);
    store.appendOp('doc1', op);
  }
  store.compact('doc1', rga.snapshot());

  const { snapshot, ops } = store.load('doc1');
  assert.ok(snapshot);
  assert.equal(ops.length, 0);

  const restored = RGA.fromSnapshot(snapshot);
  assert.equal(restored.toText(), 'hello');
});

test('load() after compaction plus more ops replays snapshot + tail correctly', () => {
  const store = new DocumentStore(tmpDir());
  const rga = new RGA('A');
  for (const ch of 'abc') store.appendOp('doc1', rga.localInsert(rga.toText().length, ch));
  store.compact('doc1', rga.snapshot());
  for (const ch of 'def') store.appendOp('doc1', rga.localInsert(rga.toText().length, ch));

  const { snapshot, ops } = store.load('doc1');
  const restored = RGA.fromSnapshot(snapshot);
  for (const op of ops) restored.applyRemote(op);
  assert.equal(restored.toText(), 'abcdef');
});

test('shouldCompact() becomes true once SNAPSHOT_EVERY_N_OPS ops have been appended', () => {
  const store = new DocumentStore(tmpDir());
  const rga = new RGA('A');
  assert.equal(store.shouldCompact('doc1'), false);
  for (let i = 0; i < SNAPSHOT_EVERY_N_OPS - 1; i++) {
    store.appendOp('doc1', rga.localInsert(rga.toText().length, 'x'));
  }
  assert.equal(store.shouldCompact('doc1'), false);
  store.appendOp('doc1', rga.localInsert(rga.toText().length, 'x'));
  assert.equal(store.shouldCompact('doc1'), true);
  store.compact('doc1', rga.snapshot());
  assert.equal(store.shouldCompact('doc1'), false);
});

test('load() tolerates a truncated final line in the op-log (simulated crash mid-append)', () => {
  const dir = tmpDir();
  const store = new DocumentStore(dir);
  const rga = new RGA('A');
  const op1 = rga.localInsert(0, 'a');
  const op2 = rga.localInsert(1, 'b');
  store.appendOp('doc1', op1);
  store.appendOp('doc1', op2);

  // Simulate a crash that wrote a partial JSON line at the very end.
  const logPath = path.join(dir, 'doc1', 'oplog.jsonl');
  fs.appendFileSync(logPath, '{"type":"insert","id":[3,"A"],"orig'); // truncated, no trailing newline

  const { ops } = store.load('doc1');
  assert.equal(ops.length, 2); // the two complete ops, partial one dropped
  const replay = new RGA('replay');
  for (const op of ops) replay.applyRemote(op);
  assert.equal(replay.toText(), 'ab');
});

test('_docDir rejects a docId that could escape the data directory (path traversal)', () => {
  const store = new DocumentStore(tmpDir());
  assert.throws(() => store.appendOp('../../etc/passwd', { type: 'insert', id: [1, 'A'], originId: null, value: 'x' }));
  assert.throws(() => store.appendOp('doc/with/slash', { type: 'insert', id: [1, 'A'], originId: null, value: 'x' }));
});

test('listDocIds reflects documents that have been written', () => {
  const store = new DocumentStore(tmpDir());
  const rga = new RGA('A');
  store.appendOp('alpha', rga.localInsert(0, 'a'));
  store.appendOp('beta', rga.localInsert(0, 'b'));
  const ids = store.listDocIds().sort();
  assert.deepEqual(ids, ['alpha', 'beta']);
});
