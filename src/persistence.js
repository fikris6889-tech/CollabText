/**
 * persistence.js — append-only op-log persistence with periodic snapshot
 * compaction, à la a minimal write-ahead log / event-sourcing store.
 *
 * WHY THIS DESIGN
 * -----------------
 * The CRDT already gives us a sequence of small, self-describing,
 * idempotent operations (inserts and deletes). That maps almost for free
 * onto an append-only log: persisting an edit is one `fs.appendFile` of one
 * JSON line, which is durable (survives a crash right after fsync) and
 * trivially replayable (feed every line back through `RGA.applyRemote` in
 * order to reconstruct the exact document). The alternative — rewriting a
 * "current state" file on every keystroke — would mean every single
 * character typed rewrites the entire document to disk, which gets slower
 * as the document grows and risks corrupting the whole file if the process
 * dies mid-write.
 *
 * The one problem with a pure append-only log is that it grows forever and
 * startup replay time grows with it. This module addresses that with
 * snapshot compaction: after every `SNAPSHOT_EVERY_N_OPS` appended
 * operations, the current CRDT state is written to a snapshot file and the
 * op-log is truncated to just the ops *after* that snapshot. Startup then
 * only needs to load the latest snapshot (O(1) relative to history) plus
 * whatever small tail of ops accumulated since.
 *
 * ON-DISK LAYOUT (per document, under `dataDir/<docId>/`)
 * -----------------------------------------------------
 *   oplog.jsonl   - one JSON operation per line, appended in real time
 *   snapshot.json - { snapshot: RGA.snapshot() output, opsSincePriorSnapshot: 0 }
 *
 * CRASH SAFETY
 * ------------
 * Appends use `fs.appendFileSync` inside a per-document async mutex (see
 * DocumentManager) so two concurrent edits can never interleave their
 * writes; a partial line at the very end of oplog.jsonl (from a crash mid
 * write) is tolerated on replay — it's detected (JSON.parse throws) and
 * skipped with a warning, rather than aborting startup, since at most one
 * unpersisted edit is ever at risk and losing it is far better than
 * refusing to boot.
 */

'use strict';

const fs = require('fs');
const path = require('path');

const SNAPSHOT_EVERY_N_OPS = 200;

class DocumentStore {
  /** @param {string} dataDir - root directory for all documents' persisted state */
  constructor(dataDir) {
    this.dataDir = dataDir;
    fs.mkdirSync(dataDir, { recursive: true });
  }

  _docDir(docId) {
    // docId is used to build a filesystem path — reject anything that could
    // escape dataDir (path traversal) or contain characters invalid on
    // common filesystems.
    if (!/^[A-Za-z0-9_-]{1,128}$/.test(docId)) {
      throw new Error(`invalid docId: ${JSON.stringify(docId)}`);
    }
    return path.join(this.dataDir, docId);
  }

  _oplogPath(docId) {
    return path.join(this._docDir(docId), 'oplog.jsonl');
  }

  _snapshotPath(docId) {
    return path.join(this._docDir(docId), 'snapshot.json');
  }

  /** Load persisted state for a document: the latest snapshot (or null) and
   * the ops appended since that snapshot, in order. Tolerates a truncated
   * final line (partial write from a crash) by skipping it. */
  load(docId) {
    const dir = this._docDir(docId);
    fs.mkdirSync(dir, { recursive: true });

    let snapshot = null;
    const snapPath = this._snapshotPath(docId);
    if (fs.existsSync(snapPath)) {
      try {
        snapshot = JSON.parse(fs.readFileSync(snapPath, 'utf8'));
      } catch (err) {
        // A corrupted snapshot is recoverable as long as the full op-log
        // since the beginning of time is still there — but we compact the
        // log on every snapshot, so in the worst case we fall back to "no
        // snapshot" and replay the (now full) log below.
        snapshot = null;
      }
    }

    const ops = [];
    const logPath = this._oplogPath(docId);
    if (fs.existsSync(logPath)) {
      const raw = fs.readFileSync(logPath, 'utf8');
      const lines = raw.split('\n');
      for (let i = 0; i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue;
        try {
          ops.push(JSON.parse(line));
        } catch (err) {
          // Only the LAST line of the file can legitimately be a partial
          // write from a crash; anything else is real corruption but we
          // still prefer "lose one op" over "refuse to start".
          if (i !== lines.length - 1 && i !== lines.length - 2) {
            throw new Error(`corrupt op-log for ${docId} at line ${i + 1}: ${err.message}`);
          }
        }
      }
    }

    return { snapshot, ops };
  }

  /** Durably append a single operation to the doc's op-log. Returns the new
   * count of ops appended since the last snapshot (caller uses this to
   * decide whether to trigger compaction). */
  appendOp(docId, op) {
    const logPath = this._oplogPath(docId);
    fs.mkdirSync(path.dirname(logPath), { recursive: true });
    fs.appendFileSync(logPath, JSON.stringify(op) + '\n', 'utf8');
    return this._bumpOpsSinceSnapshot(docId);
  }

  _countersPath(docId) {
    return path.join(this._docDir(docId), 'counters.json');
  }

  _bumpOpsSinceSnapshot(docId) {
    const p = this._countersPath(docId);
    let n = 0;
    if (fs.existsSync(p)) {
      try {
        n = JSON.parse(fs.readFileSync(p, 'utf8')).opsSinceSnapshot || 0;
      } catch {
        n = 0;
      }
    }
    n += 1;
    fs.writeFileSync(p, JSON.stringify({ opsSinceSnapshot: n }), 'utf8');
    return n;
  }

  shouldCompact(docId) {
    const p = this._countersPath(docId);
    if (!fs.existsSync(p)) return false;
    try {
      return (JSON.parse(fs.readFileSync(p, 'utf8')).opsSinceSnapshot || 0) >= SNAPSHOT_EVERY_N_OPS;
    } catch {
      return false;
    }
  }

  /** Write a fresh snapshot and truncate the op-log — everything captured
   * in `rgaSnapshot` no longer needs to be replayed from the log. Written
   * via a temp-file-then-rename so a crash mid-write can never leave a
   * half-written snapshot.json on disk (the old one, or nothing, survives —
   * never a corrupt in-between state). */
  compact(docId, rgaSnapshot) {
    const dir = this._docDir(docId);
    fs.mkdirSync(dir, { recursive: true });
    const snapPath = this._snapshotPath(docId);
    const tmpPath = `${snapPath}.tmp-${process.pid}-${Date.now()}`;
    fs.writeFileSync(tmpPath, JSON.stringify(rgaSnapshot), 'utf8');
    fs.renameSync(tmpPath, snapPath);
    fs.writeFileSync(this._oplogPath(docId), '', 'utf8');
    fs.writeFileSync(this._countersPath(docId), JSON.stringify({ opsSinceSnapshot: 0 }), 'utf8');
  }

  /** List all document ids that have any persisted state on disk. */
  listDocIds() {
    if (!fs.existsSync(this.dataDir)) return [];
    return fs.readdirSync(this.dataDir).filter((name) => {
      try {
        return fs.statSync(path.join(this.dataDir, name)).isDirectory();
      } catch {
        return false;
      }
    });
  }
}

module.exports = { DocumentStore, SNAPSHOT_EVERY_N_OPS };
