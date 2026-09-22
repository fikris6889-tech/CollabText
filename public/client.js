/**
 * client.js — the browser side of the collaborative editor. Plain
 * vanilla JS, no build step, no framework: it's loaded straight as a
 * <script> tag and runs the EXACT SAME RGA implementation as the server
 * (served from /crdt.js, which is literally src/crdt.js — see server.js).
 *
 * HOW LOCAL EDITS BECOME OPERATIONS
 * ------------------------------------
 * A <textarea>'s native `input` event gives you the new full value, not a
 * clean "user typed 'x' at position 7" description. To recover that, on
 * every input event we diff the textarea's new value against the value we
 * last knew about: strip the longest common prefix and the longest common
 * suffix off both strings, and whatever's left in the middle is "delete
 * this many old characters here, then insert these new characters here".
 * That handles single keystrokes, paste, cut, and autocomplete/spellcheck
 * replacements uniformly, by turning all of them into a sequence of
 * per-character RGA insert/delete ops (RGA operates on individual
 * characters, so a multi-character paste becomes N insert ops — simple to
 * reason about and to test, at the cost of generating more ops than a
 * "batch splice" primitive would for very large pastes).
 *
 * HOW REMOTE EDITS ARE APPLIED WITHOUT FIGHTING THE USER'S CURSOR
 * -------------------------------------------------------------------
 * Applying a remote op can shift every character after it by one position.
 * If we just overwrote textarea.value, the browser would reset the cursor
 * to the start. Instead we compute the delta a remote op has on positions
 * at-or-after the local cursor and shift selectionStart/selectionEnd by
 * the same amount, so typing while a remote peer is also typing elsewhere
 * in the document doesn't relocate your caret.
 */

(function () {
  'use strict';

  const { RGA } = window.CollabCRDT;

  const editor = document.getElementById('editor');
  const statusEl = document.getElementById('status');
  const peersEl = document.getElementById('peers');
  const docIdInput = document.getElementById('doc-id');
  const loadBtn = document.getElementById('doc-load');

  let rga = null;
  let ws = null;
  let lastKnownValue = '';
  let applyingRemote = false;
  const peers = new Set();

  function setStatus(text, cls) {
    statusEl.textContent = text;
    statusEl.className = `status status-${cls}`;
  }

  function updatePeers() {
    peersEl.textContent = peers.size > 0 ? `${peers.size} other peer(s) connected` : '';
  }

  function docIdFromLocation() {
    const params = new URLSearchParams(window.location.search);
    return params.get('doc') || 'demo';
  }

  function connect(docId) {
    if (ws) {
      try { ws.close(); } catch { /* ignore */ }
    }
    peers.clear();
    updatePeers();
    setStatus('connecting…', 'connecting');

    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    ws = new WebSocket(`${protocol}//${window.location.host}/ws/${encodeURIComponent(docId)}`);

    ws.addEventListener('open', () => setStatus('connected', 'connected'));
    ws.addEventListener('close', () => setStatus('disconnected', 'error'));
    ws.addEventListener('error', () => setStatus('connection error', 'error'));

    ws.addEventListener('message', (event) => {
      const msg = JSON.parse(event.data);
      if (msg.type === 'init') {
        rga = new RGA(msg.siteId);
        // Bulk-load the initial text as a sequence of local inserts so this
        // replica's own tree is internally consistent; this does NOT
        // re-broadcast anything (it's not going through applyClientOp).
        for (const ch of msg.text) rga.localInsert([...rga.toText()].length, ch);
        lastKnownValue = rga.toText();
        setEditorValue(lastKnownValue, { preserveCursor: false });
      } else if (msg.type === 'op') {
        applyRemoteOp(msg.op);
      } else if (msg.type === 'presence') {
        if (msg.event === 'join') peers.add(msg.connId);
        else peers.delete(msg.connId);
        updatePeers();
      } else if (msg.type === 'cursor') {
        // Presence cursor info is available here (msg.connId, msg.pos) for
        // a richer UI (e.g. colored carets per peer); this minimal UI just
        // uses presence for the peer count and keeps cursor rendering out
        // of scope, documented as a known extension in the README.
      }
    });
  }

  function applyRemoteOp(op) {
    if (!rga) return;
    const posBefore = editor.selectionStart;
    rga.applyRemote(op);
    const newText = rga.toText();

    // Compute how much the local cursor should shift: a remote insert at or
    // before the cursor pushes it right by one; a remote delete at or
    // before the cursor pulls it left by one. We approximate "at or before"
    // using the character-count delta localized to where the change
    // happened, derived from a prefix/suffix diff against the text we
    // already had — the same technique used for local edits below, run in
    // reverse.
    const delta = diffStrings(lastKnownValue, newText);
    let shift = 0;
    if (delta.insertedLength > 0 && delta.start <= posBefore) shift += delta.insertedLength;
    if (delta.deletedLength > 0 && delta.start < posBefore) {
      shift -= Math.min(delta.deletedLength, posBefore - delta.start);
    }

    lastKnownValue = newText;
    setEditorValue(newText, { preserveCursor: true, cursorOverride: posBefore + shift });
  }

  function setEditorValue(value, { preserveCursor, cursorOverride }) {
    applyingRemote = true;
    const start = editor.selectionStart;
    const end = editor.selectionEnd;
    editor.value = value;
    if (preserveCursor) {
      const pos = Math.max(0, Math.min(value.length, cursorOverride ?? start));
      editor.setSelectionRange(pos, pos);
    } else {
      editor.setSelectionRange(end, end);
    }
    applyingRemote = false;
  }

  /** Longest-common-prefix / longest-common-suffix diff between two
   * strings. Returns { start, deletedLength, insertedLength, insertedText }
   * describing "starting at `start`, remove `deletedLength` old chars and
   * insert `insertedText`". */
  function diffStrings(oldStr, newStr) {
    let prefix = 0;
    const maxPrefix = Math.min(oldStr.length, newStr.length);
    while (prefix < maxPrefix && oldStr[prefix] === newStr[prefix]) prefix += 1;

    let suffix = 0;
    const maxSuffix = Math.min(oldStr.length, newStr.length) - prefix;
    while (
      suffix < maxSuffix &&
      oldStr[oldStr.length - 1 - suffix] === newStr[newStr.length - 1 - suffix]
    ) {
      suffix += 1;
    }

    const deletedLength = oldStr.length - prefix - suffix;
    const insertedText = newStr.slice(prefix, newStr.length - suffix);
    return { start: prefix, deletedLength, insertedLength: insertedText.length, insertedText };
  }

  editor.addEventListener('input', () => {
    if (applyingRemote || !rga || !ws || ws.readyState !== WebSocket.OPEN) return;

    const newValue = editor.value;
    const { start, deletedLength, insertedText } = diffStrings(lastKnownValue, newValue);

    const ops = [];
    // Delete first (from the end backwards isn't needed here because RGA
    // positions are visible-character positions and we always delete AT
    // `start`, which stays valid as each deletion shrinks the document).
    for (let i = 0; i < deletedLength; i++) {
      const op = rga.localDelete(start);
      if (op) ops.push(op);
    }
    // Then insert the new characters, left to right, each one after the
    // previous.
    for (let i = 0; i < insertedText.length; i++) {
      const op = rga.localInsert(start + i, insertedText[i]);
      ops.push(op);
    }

    lastKnownValue = rga.toText();
    for (const op of ops) ws.send(JSON.stringify({ type: 'op', op }));
  });

  loadBtn.addEventListener('click', () => {
    const docId = docIdInput.value.trim() || 'demo';
    const url = new URL(window.location.href);
    url.searchParams.set('doc', docId);
    window.history.replaceState(null, '', url);
    connect(docId);
  });

  const initialDoc = docIdFromLocation();
  docIdInput.value = initialDoc;
  connect(initialDoc);
})();
