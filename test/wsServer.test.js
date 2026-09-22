'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const crypto = require('crypto');
const {
  computeAcceptKey,
  FrameParser,
  encodeTextFrame,
  MAX_FRAME_PAYLOAD,
} = require('../src/wsServer');

/** Build a masked client->server text frame exactly as a real browser
 * would, so we can feed it to FrameParser and prove the masking/length
 * encoding is decoded correctly. This mirrors RFC 6455 section 5.2. */
function buildMaskedFrame(payloadStr, { fin = true, opcode = 0x1 } = {}) {
  const payload = Buffer.from(payloadStr, 'utf8');
  const maskKey = crypto.randomBytes(4);
  const masked = Buffer.alloc(payload.length);
  for (let i = 0; i < payload.length; i++) masked[i] = payload[i] ^ maskKey[i % 4];

  let header;
  const len = payload.length;
  if (len < 126) {
    header = Buffer.alloc(2);
    header[0] = (fin ? 0x80 : 0x00) | opcode;
    header[1] = 0x80 | len;
  } else if (len < 65536) {
    header = Buffer.alloc(4);
    header[0] = (fin ? 0x80 : 0x00) | opcode;
    header[1] = 0x80 | 126;
    header.writeUInt16BE(len, 2);
  } else {
    header = Buffer.alloc(10);
    header[0] = (fin ? 0x80 : 0x00) | opcode;
    header[1] = 0x80 | 127;
    header.writeBigUInt64BE(BigInt(len), 2);
  }
  return Buffer.concat([header, maskKey, masked]);
}

test('computeAcceptKey matches the RFC 6455 worked example', () => {
  // This exact key/answer pair is the worked example from RFC 6455 section 1.3.
  const key = 'dGhlIHNhbXBsZSBub25jZQ==';
  assert.equal(computeAcceptKey(key), 's3pPLMBiTxaQ9kYGzzhZRbK+xOo=');
});

test('FrameParser decodes a small unfragmented masked text frame', () => {
  const messages = [];
  const parser = new FrameParser({
    onMessage: (buf) => messages.push(buf.toString('utf8')),
    onClose: () => {},
    onPing: () => {},
    onError: (e) => assert.fail(`unexpected error: ${JSON.stringify(e)}`),
  });
  parser.push(buildMaskedFrame('hello world'));
  assert.deepEqual(messages, ['hello world']);
});

test('FrameParser handles the frame arriving one byte at a time', () => {
  // Real TCP sockets do not deliver a frame atomically. The parser must be
  // correct no matter how the bytes are chunked across push() calls.
  const messages = [];
  const parser = new FrameParser({
    onMessage: (buf) => messages.push(buf.toString('utf8')),
    onClose: () => {},
    onPing: () => {},
    onError: (e) => assert.fail(`unexpected error: ${JSON.stringify(e)}`),
  });
  const frame = buildMaskedFrame('byte-at-a-time delivery test 🎉');
  for (let i = 0; i < frame.length; i++) parser.push(frame.subarray(i, i + 1));
  assert.deepEqual(messages, ['byte-at-a-time delivery test 🎉']);
});

test('FrameParser handles two frames arriving in a single chunk', () => {
  const messages = [];
  const parser = new FrameParser({
    onMessage: (buf) => messages.push(buf.toString('utf8')),
    onClose: () => {},
    onPing: () => {},
    onError: (e) => assert.fail(`unexpected error: ${JSON.stringify(e)}`),
  });
  const combined = Buffer.concat([buildMaskedFrame('first'), buildMaskedFrame('second')]);
  parser.push(combined);
  assert.deepEqual(messages, ['first', 'second']);
});

test('FrameParser reassembles a fragmented message across continuation frames', () => {
  const messages = [];
  const parser = new FrameParser({
    onMessage: (buf) => messages.push(buf.toString('utf8')),
    onClose: () => {},
    onPing: () => {},
    onError: (e) => assert.fail(`unexpected error: ${JSON.stringify(e)}`),
  });
  parser.push(buildMaskedFrame('Hello, ', { fin: false, opcode: 0x1 }));
  parser.push(buildMaskedFrame('fragmented ', { fin: false, opcode: 0x0 }));
  parser.push(buildMaskedFrame('world!', { fin: true, opcode: 0x0 }));
  assert.deepEqual(messages, ['Hello, fragmented world!']);
});

test('FrameParser correctly decodes the 16-bit extended length encoding', () => {
  const longStr = 'x'.repeat(1000); // > 125, triggers the 126 extended-length path
  const messages = [];
  const parser = new FrameParser({
    onMessage: (buf) => messages.push(buf.toString('utf8')),
    onClose: () => {},
    onPing: () => {},
    onError: (e) => assert.fail(`unexpected error: ${JSON.stringify(e)}`),
  });
  parser.push(buildMaskedFrame(longStr));
  assert.equal(messages[0].length, 1000);
  assert.equal(messages[0], longStr);
});

test('FrameParser rejects an unmasked client frame per RFC 6455 5.1', () => {
  let error = null;
  const parser = new FrameParser({
    onMessage: () => assert.fail('should not deliver an unmasked frame'),
    onClose: () => {},
    onPing: () => {},
    onError: (e) => { error = e; },
  });
  // Hand-build an UNMASKED frame (mask bit = 0) — something a compliant
  // browser would never send, but a malicious or buggy client might.
  const payload = Buffer.from('sneaky', 'utf8');
  const header = Buffer.from([0x81, payload.length]); // FIN=1,opcode=text, MASK=0
  parser.push(Buffer.concat([header, payload]));
  assert.ok(error, 'expected a protocol error');
  assert.equal(error.code, 1002);
});

test('FrameParser rejects a frame that claims a payload larger than the configured maximum', () => {
  let error = null;
  const parser = new FrameParser({
    onMessage: () => assert.fail('should not deliver an oversized frame'),
    onClose: () => {},
    onPing: () => {},
    onError: (e) => { error = e; },
  });
  const header = Buffer.alloc(10);
  header[0] = 0x81; // FIN + text
  header[1] = 0x80 | 127; // masked, 64-bit length follows
  header.writeBigUInt64BE(BigInt(MAX_FRAME_PAYLOAD) + 1n, 2);
  const maskKey = Buffer.from([1, 2, 3, 4]);
  // We don't need to actually send that many bytes — the parser must reject
  // based on the declared length alone, before waiting for a payload that
  // (if this were a real attack) would never fully arrive.
  parser.push(Buffer.concat([header, maskKey]));
  assert.ok(error, 'expected a protocol error for an oversized declared length');
  assert.equal(error.code, 1009);
});

test('FrameParser rejects a nonzero reserved bit', () => {
  let error = null;
  const parser = new FrameParser({
    onMessage: () => assert.fail('should not deliver'),
    onClose: () => {},
    onPing: () => {},
    onError: (e) => { error = e; },
  });
  const frame = buildMaskedFrame('x');
  frame[0] |= 0x40; // set RSV1
  parser.push(frame);
  assert.ok(error);
  assert.equal(error.code, 1002);
});

test('encodeTextFrame round-trips through a compliant unmask (server frames are unmasked, per spec)', () => {
  const frame = encodeTextFrame('hello from the server');
  assert.equal(frame[0], 0x81); // FIN=1, opcode=text
  assert.equal(frame[1] & 0x80, 0); // server frames MUST NOT be masked
  const len = frame[1] & 0x7f;
  const payload = frame.subarray(2, 2 + len);
  assert.equal(payload.toString('utf8'), 'hello from the server');
});

test('encodeTextFrame uses the 16-bit length form above 125 bytes', () => {
  const s = 'y'.repeat(200);
  const frame = encodeTextFrame(s);
  assert.equal(frame[1], 126);
  const len = frame.readUInt16BE(2);
  assert.equal(len, 200);
  assert.equal(frame.subarray(4, 4 + len).toString('utf8'), s);
});
