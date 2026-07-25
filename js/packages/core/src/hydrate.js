'use strict';

// hydrate.js — a single, self-contained implementation of the httpt JS SDK core
// (incubation-js.md §1 + §2). No local imports; usable via
//   const { hydrate, parse } = require('./hydrate');
//
// hydrate(templateStream, data, streams) returns SYNCHRONOUSLY:
//   - resolvedStream : the request head, text chunks   (AsyncIterable<string>, async function*)
//   - mapStream      : one index-shift-map entry per tag (AsyncIterable<object>, async function*)
//   - bodyStream     : the request body, bytes          (ReadableStream<Uint8Array>, BYOB byte-stream)
// A detached background worker fills them; any error fails all three (fail-fast).

// ---------------------------------------------------------------------------
// Async channel: a background worker push()es values in; a `for await` consumer
// pulls them out. end() finishes cleanly, fail() makes the consumer throw.
// ---------------------------------------------------------------------------
function createChannel() {
  const queue = [];   // buffered { value } | { done: true } | { err }
  let waiter = null;  // pending { resolve, reject } from an awaiting next()
  let closed = false;

  const settle = () => {
    if (!waiter || queue.length === 0) return;
    const item = queue.shift();
    const w = waiter;
    waiter = null;
    if (item.err) w.reject(item.err);
    else if (item.done) w.resolve({ value: undefined, done: true });
    else w.resolve({ value: item.value, done: false });
  };

  return {
    push: (value) => { if (!closed) { queue.push({ value }); settle(); } },
    end: () => { if (!closed) { closed = true; queue.push({ done: true }); settle(); } },
    fail: (err) => { if (!closed) { closed = true; queue.push({ err }); settle(); } },
    next: () => new Promise((resolve, reject) => { waiter = { resolve, reject }; settle(); }),
  };
}

// The resolved/map outputs are real async generators (§2.1), draining a channel.
async function* channelGenerator(channel) {
  for (;;) {
    const { value, done } = await channel.next();
    if (done) return;
    yield value;
  }
}

// Accept a ReadableStream, a plain string, a byte array, or any async iterable
// and yield decoded text chunks; a streaming TextDecoder keeps multi-byte UTF-8
// intact across chunk boundaries.
async function* toTextChunks(template) {
  if (template == null) return;
  if (typeof template === 'string') { yield template; return; }

  const decoder = new TextDecoder('utf-8');
  const decode = (c) => (typeof c === 'string' ? c : decoder.decode(c, { stream: true }));

  if (typeof template.getReader === 'function') {          // Web ReadableStream
    const reader = template.getReader();
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        if (value !== undefined) { const t = decode(value); if (t) yield t; }
      }
    } finally { reader.releaseLock(); }
    const tail = decoder.decode(); if (tail) yield tail;
    return;
  }
  if (typeof template[Symbol.asyncIterator] === 'function') { // any async iterable
    for await (const c of template) { const t = decode(c); if (t) yield t; }
    const tail = decoder.decode(); if (tail) yield tail;
    return;
  }
  if (ArrayBuffer.isView(template)) { yield decoder.decode(template); return; }
  throw new TypeError('Unsupported template input');
}

const toU8 = (c) => {
  const enc = new TextEncoder();
  return typeof c === 'string' ? enc.encode(c)
    : c instanceof Uint8Array ? c
    : ArrayBuffer.isView(c) ? new Uint8Array(c.buffer, c.byteOffset, c.byteLength)
    : c instanceof ArrayBuffer ? new Uint8Array(c)
    : new Uint8Array(c);
};

// Normalize a provided body source into byte chunks, for piping into bodyStream.
async function* toByteChunks(source) {
  if (source == null) return;
  if (typeof source === 'string' || ArrayBuffer.isView(source)) { yield toU8(source); return; }
  if (typeof source.arrayBuffer === 'function' && typeof source.stream !== 'function') {
    yield new Uint8Array(await source.arrayBuffer()); return;                 // Blob / File
  }
  const s = typeof source.stream === 'function' ? source.stream() : source;
  if (typeof s.getReader === 'function') {
    const reader = s.getReader();
    try { for (;;) { const { value, done } = await reader.read(); if (done) break; if (value !== undefined) yield toU8(value); } }
    finally { reader.releaseLock(); }
    return;
  }
  if (typeof s[Symbol.asyncIterator] === 'function') { for await (const c of s) yield toU8(c); return; }
  throw new TypeError('Unsupported provided-stream source');
}

// ---------------------------------------------------------------------------
// Filters: {{ name | filter }}
// ---------------------------------------------------------------------------
const spacedJson = (v) => JSON.stringify(v, null, 1).replace(/\n */g, ' ');
const jsonEscape = (v) => JSON.stringify(String(v)).slice(1, -1); // escaped, without the quotes
const FILTERS = {
  raw: (v) => (v == null ? '' : String(v)),
  url: (v) => (v == null ? '' : encodeURIComponent(String(v))),
  'json-value': (v) => spacedJson(v),
  'json-string': (v) => jsonEscape(v),
  'json-key': (v) => jsonEscape(v),
};
const STREAM_FILTERS = new Set(['stream-as-base64', 'stream-as-utf8', 'stream-as-is']);

function renderTag(inner, data, materialized) {
  const parts = inner.split('|').map((s) => s.trim());
  const value = data ? data[parts[0]] : undefined;

  if (parts.length === 2 && STREAM_FILTERS.has(parts[1])) {   // §1.3.2.3 streaming transforms
    const { index } = resolveRef(value);
    const bytes = materialized.get(index) || new Uint8Array(0);
    if (parts[1] === 'stream-as-base64') return Buffer.from(bytes).toString('base64');
    if (parts[1] === 'stream-as-utf8') return new TextDecoder('utf-8').decode(bytes);
    return Buffer.from(bytes).toString('latin1'); // stream-as-is (inline, raw bytes)
  }

  if (parts.length === 1) return FILTERS.raw(value);
  let out = value;
  for (let i = 1; i < parts.length; i++) {
    const fn = FILTERS[parts[i]];
    if (!fn) { const e = new Error(`Unknown template filter: '${parts[i]}'`); e.name = 'TemplateSyntaxError'; throw e; }
    out = fn(out);
  }
  return String(out);
}

// ---------------------------------------------------------------------------
// Stream reference rules (§1.4.4 / §1.4.5)
// ---------------------------------------------------------------------------
const STREAM_TAG_RE = /\{\{\s*([^|}]+?)\s*\|\s*(stream-as-base64|stream-as-utf8|stream-as-is)\s*\}\}/g;
const PSEUDO_PROVIDED_RE = /(^|\n)[ \t]*:httpt-body-type[ \t]*:[ \t]*provided/i;

function resolveRef(def) {
  if (def && typeof def === 'object' && Number.isInteger(def.content)) return { index: def.content, explicit: true };
  if (Number.isInteger(def)) return { index: def, explicit: true };
  return { index: 0, explicit: false }; // Implicit Default (§1.4.4)
}

function collectStreamReferences(template, data) {
  const refs = [];
  const body = data && typeof data.body === 'object' && data.body ? data.body : null;
  if (body && body.type === 'provided') {
    const { index, explicit } = resolveRef(body);
    refs.push({ index, explicit, where: 'body' });
  } else if (typeof template === 'string' && PSEUDO_PROVIDED_RE.test(template)) {
    refs.push({ index: 0, explicit: false, where: 'body' });
  }
  if (typeof template === 'string') {
    STREAM_TAG_RE.lastIndex = 0;
    let m;
    while ((m = STREAM_TAG_RE.exec(template)) !== null) {
      const { index, explicit } = resolveRef(data ? data[m[1].trim()] : undefined);
      refs.push({ index, explicit, where: `filter:${m[1].trim()}` });
    }
  }
  return refs;
}

function validateStreamReferences(refs) {
  if (refs.length <= 1) return;
  if (refs.some((r) => !r.explicit)) {
    const e = new Error('Multiple provided streams referenced but one omits an explicit index');
    e.name = 'AmbiguousStreamReferenceError'; throw e;
  }
  const seen = new Set();
  for (const r of refs) {
    if (seen.has(r.index)) { const e = new Error(`Duplicate provided-stream index ${r.index}`); e.name = 'DuplicateStreamReferenceError'; throw e; }
    seen.add(r.index);
  }
}

async function materializeStream(source) {
  const chunks = [];
  let total = 0;
  for await (const chunk of toByteChunks(source)) { chunks.push(chunk); total += chunk.byteLength; }
  const out = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) { out.set(c, off); off += c.byteLength; }
  return out;
}

// Find the first head/body separator: a blank line, \n\n or \r\n\r\n.
function findBoundary(s) {
  const lf = s.indexOf('\n\n');
  const crlf = s.indexOf('\r\n\r\n');
  if (lf === -1 && crlf === -1) return null;
  if (crlf !== -1 && (lf === -1 || crlf <= lf)) return { index: crlf, len: 4 };
  return { index: lf, len: 2 };
}
const pseudoIsProvided = (head) => PSEUDO_PROVIDED_RE.test(head);

/**
 * @param {ReadableStream<Uint8Array | string>} templateStream
 * @param {Object} [data={}]
 * @param {Array<ReadableStream<Uint8Array> | Blob | any>} [streams=[]]
 * @returns {{
 * resolvedStream: AsyncIterable<string>,
 * mapStream: AsyncIterable<object>,
 * bodyStream: ReadableStream<Uint8Array>
 * }}
 */
function hydrate(templateStream, data = {}, streams = []) {
  const resolvedChannel = createChannel();
  const mapChannel = createChannel();

  // bodyStream is a BYOB byte-stream (§2.1): backpressure + zero-copy BYOB reads.
  let bodyController;
  let resolvePull = null;
  const bodyStream = new ReadableStream(
    {
      type: 'bytes',
      start: (c) => { bodyController = c; },
      pull: () => { if (resolvePull) { const r = resolvePull; resolvePull = null; r(); } },
    },
    { highWaterMark: 65536 }
  );
  const awaitPull = () => new Promise((r) => { resolvePull = r; });
  // enqueue a fresh copy: a byte-stream detaches the enqueued buffer, so never
  // hand it a view over a caller-owned (or pooled Buffer) ArrayBuffer.
  const enqueueBytes = (u8) => { if (u8 && u8.byteLength) bodyController.enqueue(u8.slice()); };

  const encoder = new TextEncoder();
  const materialized = new Map();
  const dynamicHeaders = Array.isArray(data && data.headers) ? data.headers : [];
  const dynamicBody = data && typeof data.body === 'object' && data.body ? data.body : undefined;

  // --- state machine bookkeeping ---
  let buf = '';
  let phase = 'head';
  let headBuf = '';
  let resolvedIndex = 0;
  let sourceIndex = 0;
  let headDone = false;
  let bodyMode = dynamicBody ? 'dynamic' : null;

  const emitHead = (s) => { if (s) { headBuf += s; resolvedIndex += s.length; } };
  const emitBody = (s) => { if (s) { enqueueBytes(encoder.encode(s)); resolvedIndex += s.length; } };
  const emit = (s) => (phase === 'head' ? emitHead(s) : emitBody(s));

  function recordTag(origStart, origLen, value) {
    const hydratedStart = resolvedIndex;
    emit(value);
    mapChannel.push({
      'hydrated-start': hydratedStart, 'original-start': origStart,
      'hydrated-length': value.length, 'original-length': origLen,
    });
  }

  function finalizeHead() {
    if (dynamicBody) bodyMode = 'dynamic';
    else if (pseudoIsProvided(headBuf)) bodyMode = 'provided-shadow';
    else bodyMode = 'inline';

    const lines = [];
    for (const h of dynamicHeaders) lines.push(`${h.name}: ${h.value}`);
    if (dynamicBody) lines.push(`:httpt-body-type: ${dynamicBody.type}`);
    if (lines.length) {
      emitHead(headBuf.endsWith('\n') ? lines.map((l) => l + '\n').join('') : lines.map((l) => '\n' + l).join(''));
    }
    resolvedChannel.push(headBuf);
    resolvedChannel.end();
    headDone = true;
  }

  function stepHead(isFinal) {
    for (;;) {
      const tagIdx = buf.indexOf('{{');
      const boundary = findBoundary(buf);
      let ev = null;
      if (tagIdx !== -1 && (!boundary || tagIdx < boundary.index)) ev = { type: 'tag', index: tagIdx };
      else if (boundary) ev = { type: 'boundary', index: boundary.index, len: boundary.len };

      if (!ev) {
        if (isFinal) { emitHead(buf); sourceIndex += buf.length; buf = ''; return; }
        let hold = 0;
        if (buf.endsWith('\r\n\r')) hold = 3;
        else if (buf.endsWith('\r\n')) hold = 2;
        else if (buf.endsWith('\r') || buf.endsWith('\n')) hold = 1;
        if (buf.endsWith('{')) hold = Math.max(hold, 1);
        const safe = hold ? buf.slice(0, -hold) : buf;
        if (safe) { emitHead(safe); sourceIndex += safe.length; buf = buf.slice(safe.length); }
        return;
      }

      const literal = buf.slice(0, ev.index);
      emitHead(literal); sourceIndex += literal.length; buf = buf.slice(ev.index);

      if (ev.type === 'tag') {
        const close = buf.indexOf('}}', 2);
        if (close === -1) { if (isFinal) { emitHead(buf); sourceIndex += buf.length; buf = ''; } return; }
        const tagLen = close + 2;
        recordTag(sourceIndex, tagLen, renderTag(buf.slice(2, close), data, materialized));
        sourceIndex += tagLen; buf = buf.slice(tagLen);
      } else {
        if (!dynamicBody) finalizeHead();
        resolvedIndex += ev.len; sourceIndex += ev.len; buf = buf.slice(ev.len);
        phase = 'body';
        return;
      }
    }
  }

  function stepBody(isFinal) {
    if (bodyMode === 'dynamic') {
      if (/\S/.test(buf)) { const e = new Error('Template defines a body but data.body was also provided'); e.name = 'BodyConflictError'; throw e; }
      buf = ''; return;
    }
    if (bodyMode === 'provided-shadow') { buf = ''; return; }

    for (;;) {
      const tagIdx = buf.indexOf('{{');
      if (tagIdx === -1) {
        if (isFinal) { emitBody(buf); sourceIndex += buf.length; buf = ''; return; }
        const hold = buf.endsWith('{') ? 1 : 0;
        const safe = hold ? buf.slice(0, -hold) : buf;
        if (safe) { emitBody(safe); sourceIndex += safe.length; buf = buf.slice(safe.length); }
        return;
      }
      const literal = buf.slice(0, tagIdx);
      emitBody(literal); sourceIndex += literal.length; buf = buf.slice(tagIdx);
      const close = buf.indexOf('}}', 2);
      if (close === -1) { if (isFinal) { emitBody(buf); sourceIndex += buf.length; buf = ''; } return; }
      const tagLen = close + 2;
      recordTag(sourceIndex, tagLen, renderTag(buf.slice(2, close), data, materialized));
      sourceIndex += tagLen; buf = buf.slice(tagLen);
    }
  }

  const step = (isFinal) => { if (phase === 'head') stepHead(isFinal); if (phase === 'body') stepBody(isFinal); };

  async function run() {
    // Validate references (§1.4.4) + materialize inline-filter streams (§1.4.5).
    const refs = collectStreamReferences(templateStream, data);
    validateStreamReferences(refs);
    for (const ref of refs) {
      if (ref.where.startsWith('filter:') && !materialized.has(ref.index)) {
        materialized.set(ref.index, await materializeStream(streams ? streams[ref.index] : undefined));
      }
    }

    for await (const chunk of toTextChunks(templateStream)) { buf += chunk; step(false); }
    step(true);
    if (!headDone) finalizeHead();

    mapChannel.end();

    if (bodyMode === 'dynamic' || bodyMode === 'provided-shadow') {
      const idx = dynamicBody && Number.isInteger(dynamicBody.content) ? dynamicBody.content : 0;
      const src = streams ? streams[idx] : undefined;
      if (src != null) {
        for await (const chunk of toByteChunks(src)) {
          while (bodyController.desiredSize !== null && bodyController.desiredSize <= 0) await awaitPull();
          if (bodyController.desiredSize === null) break;
          enqueueBytes(chunk);
        }
      }
    }
    try { bodyController.close(); } catch { /* already closed */ }
  }

  run().catch((err) => {
    resolvedChannel.fail(err);
    mapChannel.fail(err);
    try { bodyController.error(err); } catch { /* already settled */ }
  });

  return {
    resolvedStream: channelGenerator(resolvedChannel),
    mapStream: channelGenerator(mapChannel),
    bodyStream,
  };
}

// ---------------------------------------------------------------------------
// parse (§2.2): consume the resolved head, build the IR, hand off / materialize
// the body.
// ---------------------------------------------------------------------------
const PSEUDO_BODY_TYPE = /^[ \t]*:httpt-body-type[ \t]*:/i;

function parseRequestLine(line) {
  const parts = line.trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) return { method: '', uri: '', version: '' };
  if (parts.length === 1) return { method: parts[0], uri: '', version: '' };
  if (parts.length === 2) return { method: parts[0], uri: parts[1], version: '' };
  return { method: parts[0], version: parts[parts.length - 1], uri: parts.slice(1, -1).join(' ') };
}

async function drainReadable(readable) {
  if (!readable) return new Uint8Array(0);
  const reader = readable.getReader();
  const chunks = [];
  let total = 0;
  try { for (;;) { const { value, done } = await reader.read(); if (done) break; if (value) { const u = toU8(value); chunks.push(u); total += u.byteLength; } } }
  finally { reader.releaseLock(); }
  const out = new Uint8Array(total);
  let off = 0;
  for (const c of chunks) { out.set(c, off); off += c.byteLength; }
  return out;
}

/**
 * @param {AsyncIterable<string>} resolvedIterable
 * @param {ReadableStream<Uint8Array> | null} [optionalBodyStream=null]
 * @returns {Promise<{ ir: object, bodyStream: ReadableStream<Uint8Array> | null }>}
 */
async function parse(resolvedIterable, optionalBodyStream = null) {
  let head = '';
  if (resolvedIterable) for await (const chunk of resolvedIterable) head += chunk;

  const lines = head.split('\n');
  while (lines.length && lines[lines.length - 1] === '') lines.pop();

  const { method, uri, version } = parseRequestLine(lines.shift() || '');
  let host = '';
  let bodyType = null;
  const headers = [];

  for (const raw of lines) {
    const line = raw.endsWith('\r') ? raw.slice(0, -1) : raw;
    if (line === '') continue;
    if (PSEUDO_BODY_TYPE.test(line)) {
      const first = line.indexOf(':');
      const second = line.indexOf(':', first + 1);
      bodyType = line.slice(second + 1).trim().toLowerCase();
      continue;
    }
    const searchStart = line.startsWith(':') ? 1 : 0; // ":authority" etc.
    const colon = line.indexOf(':', searchStart);
    if (colon === -1) { headers.push({ name: line, value: '' }); continue; }
    const name = line.slice(0, colon);
    const value = line.slice(colon + 1).replace(/^[ \t]+/, '');
    const lname = name.toLowerCase();
    if (lname === 'host' || lname === ':authority') { host = value; continue; }
    headers.push({ name, value });
  }

  const ir = { 'schema-version': '1.0', method, host, uri, version, headers };
  const decode = (b) => new TextDecoder('utf-8').decode(b);
  const trimNl = (s) => s.replace(/\r?\n$/, '');

  if (bodyType === 'provided') {
    ir.body = { type: 'provided', content: 0 };
  } else if (bodyType === 'json' || bodyType === 'text' || bodyType === 'base64') {
    const text = trimNl(decode(await drainReadable(optionalBodyStream)));
    ir.body = bodyType === 'json' ? { type: 'json', content: JSON.parse(text) } : { type: bodyType, content: text };
  } else {
    const bytes = await drainReadable(optionalBodyStream);
    if (bytes.byteLength > 0) ir.body = { type: 'text', content: trimNl(decode(bytes)) };
  }

  return { ir, bodyStream: optionalBodyStream };
}

module.exports = { hydrate, parse };
