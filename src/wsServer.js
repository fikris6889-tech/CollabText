/**
 * wsServer.js — a from-scratch WebSocket server implementing the parts of
 * RFC 6455 this project needs: the opening handshake, text-frame framing
 * (fragmented and unfragmented), masking, ping/pong, and the closing
 * handshake. No `ws` package, no `socket.io` — every byte on the wire is
 * assembled and parsed here.
 *
 * WHY BUILD THIS INSTEAD OF USING A LIBRARY
 * -------------------------------------------
 * In a real project you'd reach for `ws`. This project's whole point is
 * closing the gap between "I used a real-time library" and "I understand
 * what a real-time library does" — so the protocol itself, with its
 * genuinely fiddly bits (the masking XOR, the 7/16/64-bit length encoding,
 * a handshake response that's byte-exact or the browser silently refuses
 * the connection), is implemented and unit-tested here.
 *
 * SECURITY-RELEVANT DECISIONS
 * -----------------------------
 * - `MAX_FRAME_PAYLOAD` bounds any single frame's payload length, and
 *   `MAX_MESSAGE_BYTES` bounds the total size of a reassembled (possibly
 *   fragmented) message. Without either, a malicious or buggy client could
 *   claim a 64-bit length and exhaust server memory before a single byte of
 *   actual payload arrives.
 * - Per RFC 6455 section 5.1, every client->server frame MUST be masked;
 *   frames from a client that aren't masked are treated as a protocol
 *   violation and the connection is closed rather than tolerated, which
 *   would open the door to cross-protocol confusion attacks.
 * - Reserved opcodes and reserved (rsv1-3) bits are rejected rather than
 *   silently ignored, since silently accepting unknown protocol extensions
 *   is a classic source of desync between what a server thinks it agreed to
 *   and what a client does.
 */

'use strict';

const crypto = require('crypto');

const WS_MAGIC = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

const OPCODES = {
  CONTINUATION: 0x0,
  TEXT: 0x1,
  BINARY: 0x2,
  CLOSE: 0x8,
  PING: 0x9,
  PONG: 0xa,
};

const MAX_FRAME_PAYLOAD = 2 * 1024 * 1024; // 2 MiB per frame
const MAX_MESSAGE_BYTES = 4 * 1024 * 1024; // 4 MiB reassembled message cap

/** Compute the Sec-WebSocket-Accept header value from a client's
 * Sec-WebSocket-Key, per RFC 6455 section 1.3. */
function computeAcceptKey(clientKey) {
  return crypto.createHash('sha1').update(clientKey + WS_MAGIC, 'binary').digest('base64');
}

/** Given a Node `http` request + the raw socket handed to the server's
 * 'upgrade' event, validate it's a well-formed WebSocket upgrade request
 * and write the 101 Switching Protocols response. Returns true on success,
 * or writes an error response and returns false. */
function performHandshake(req, socket) {
  const upgradeHeader = (req.headers.upgrade || '').toLowerCase();
  const connectionHeader = (req.headers.connection || '').toLowerCase();
  const key = req.headers['sec-websocket-key'];
  const version = req.headers['sec-websocket-version'];

  if (upgradeHeader !== 'websocket' || !connectionHeader.includes('upgrade') || !key || version !== '13') {
    socket.write('HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n');
    socket.destroy();
    return false;
  }

  const acceptKey = computeAcceptKey(key);
  const responseHeaders = [
    'HTTP/1.1 101 Switching Protocols',
    'Upgrade: websocket',
    'Connection: Upgrade',
    `Sec-WebSocket-Accept: ${acceptKey}`,
    '\r\n',
  ].join('\r\n');
  socket.write(responseHeaders);
  return true;
}

/** Encode a text message as one (unfragmented) WebSocket frame. Server
 * frames are never masked (RFC 6455 5.1: only client->server frames must
 * be masked). */
function encodeTextFrame(str) {
  const payload = Buffer.from(str, 'utf8');
  const len = payload.length;
  let header;
  if (len < 126) {
    header = Buffer.alloc(2);
    header[0] = 0x80 | OPCODES.TEXT; // FIN=1, opcode=text
    header[1] = len; // MASK=0
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = 0x80 | OPCODES.TEXT;
    header[1] = 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = 0x80 | OPCODES.TEXT;
    header[1] = 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  return Buffer.concat([header, payload]);
}

function encodeCloseFrame(code = 1000, reason = '') {
  const reasonBuf = Buffer.from(reason, 'utf8');
  const payload = Buffer.alloc(2 + reasonBuf.length);
  payload.writeUInt16BE(code, 0);
  reasonBuf.copy(payload, 2);
  const header = Buffer.alloc(2);
  header[0] = 0x80 | OPCODES.CLOSE;
  header[1] = payload.length;
  return Buffer.concat([header, payload]);
}

function encodePongFrame(payload = Buffer.alloc(0)) {
  const header = Buffer.alloc(2);
  header[0] = 0x80 | OPCODES.PONG;
  header[1] = payload.length;
  return Buffer.concat([header, payload]);
}

/**
 * A minimal incremental WebSocket frame parser. Feed it raw bytes as they
 * arrive on the socket via `push(buffer)`; it emits complete, unmasked
 * application messages via the `onMessage` callback, and reports protocol
 * errors via `onError` (caller should then close the connection).
 *
 * This is deliberately a small state machine rather than "wait for the
 * whole message and parse it in one go" — a real socket can deliver a
 * frame's header and payload split across arbitrarily many TCP packets,
 * and the frame parser has to be correct no matter how the stream happens
 * to be chunked. That's exercised directly in test/wsServer.test.js by
 * feeding a single frame to `push()` one byte at a time.
 */
class FrameParser {
  constructor({ onMessage, onClose, onPing, onError }) {
    this.onMessage = onMessage;
    this.onClose = onClose;
    this.onPing = onPing;
    this.onError = onError;
    this._buffer = Buffer.alloc(0);
    this._fragments = []; // Buffers accumulated for a fragmented text message
    this._fragmentedOpcode = null;
    this._fragmentedBytes = 0;
  }

  push(chunk) {
    this._buffer = this._buffer.length ? Buffer.concat([this._buffer, chunk]) : chunk;
    // Keep parsing as many complete frames as are already buffered.
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const consumed = this._tryParseOneFrame();
      if (consumed === 0) return; // need more bytes
      if (consumed < 0) return; // fatal error already reported
    }
  }

  /** Returns bytes consumed (>0), 0 if more data is needed, or -1 on a
   * fatal protocol error (already reported via onError). */
  _tryParseOneFrame() {
    const buf = this._buffer;
    if (buf.length < 2) return 0;

    const b0 = buf[0];
    const b1 = buf[1];
    const fin = (b0 & 0x80) !== 0;
    const rsv = b0 & 0x70;
    const opcode = b0 & 0x0f;
    const masked = (b1 & 0x80) !== 0;
    let payloadLen = b1 & 0x7f;
    let offset = 2;

    if (rsv !== 0) {
      this._fail(1002, 'reserved bits must be zero');
      return -1;
    }
    if (!masked) {
      // RFC 6455 5.1: "a client MUST mask all frames ... A server MUST
      // close the connection upon receiving a frame that is not masked."
      this._fail(1002, 'client frames must be masked');
      return -1;
    }

    if (payloadLen === 126) {
      if (buf.length < offset + 2) return 0;
      payloadLen = buf.readUInt16BE(offset);
      offset += 2;
    } else if (payloadLen === 127) {
      if (buf.length < offset + 8) return 0;
      const big = buf.readBigUInt64BE(offset);
      if (big > BigInt(Number.MAX_SAFE_INTEGER)) {
        this._fail(1009, 'frame too large');
        return -1;
      }
      payloadLen = Number(big);
      offset += 8;
    }

    if (payloadLen > MAX_FRAME_PAYLOAD) {
      this._fail(1009, 'frame exceeds maximum allowed size');
      return -1;
    }

    if (buf.length < offset + 4) return 0; // need masking key
    const maskKey = buf.subarray(offset, offset + 4);
    offset += 4;

    if (buf.length < offset + payloadLen) return 0; // need full payload

    const maskedPayload = buf.subarray(offset, offset + payloadLen);
    const payload = Buffer.alloc(payloadLen);
    for (let i = 0; i < payloadLen; i++) payload[i] = maskedPayload[i] ^ maskKey[i % 4];

    const totalFrameLen = offset + payloadLen;
    this._buffer = buf.subarray(totalFrameLen);

    this._handleFrame(opcode, fin, payload);
    return totalFrameLen;
  }

  _handleFrame(opcode, fin, payload) {
    if (opcode === OPCODES.CLOSE) {
      this.onClose(payload);
      return;
    }
    if (opcode === OPCODES.PING) {
      this.onPing(payload);
      return;
    }
    if (opcode === OPCODES.PONG) {
      return; // nothing to do; presence of a pong is enough
    }

    if (opcode === OPCODES.TEXT || opcode === OPCODES.BINARY) {
      if (this._fragmentedOpcode !== null) {
        this._fail(1002, 'expected continuation frame');
        return;
      }
      if (fin) {
        this.onMessage(payload);
        return;
      }
      this._fragmentedOpcode = opcode;
      this._fragments = [payload];
      this._fragmentedBytes = payload.length;
      return;
    }

    if (opcode === OPCODES.CONTINUATION) {
      if (this._fragmentedOpcode === null) {
        this._fail(1002, 'unexpected continuation frame');
        return;
      }
      this._fragmentedBytes += payload.length;
      if (this._fragmentedBytes > MAX_MESSAGE_BYTES) {
        this._fail(1009, 'fragmented message exceeds maximum allowed size');
        return;
      }
      this._fragments.push(payload);
      if (fin) {
        const full = Buffer.concat(this._fragments);
        this._fragmentedOpcode = null;
        this._fragments = [];
        this._fragmentedBytes = 0;
        this.onMessage(full);
      }
      return;
    }

    this._fail(1002, `unknown opcode 0x${opcode.toString(16)}`);
  }

  _fail(code, reason) {
    this.onError({ code, reason });
  }
}

module.exports = {
  performHandshake,
  computeAcceptKey,
  encodeTextFrame,
  encodeCloseFrame,
  encodePongFrame,
  FrameParser,
  OPCODES,
  MAX_FRAME_PAYLOAD,
  MAX_MESSAGE_BYTES,
};
