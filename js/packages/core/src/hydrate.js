'use strict';

const { createChannel, toTextChunks, toByteChunks } = require('./streams');
const { applyFilter, FILTERS } = require('./filters');

/**
 * @typedef {import('./types').HttptIR} HttptIR
 */

/**
 * Looks up a template parameter in the data context.
 * @param {object} data
 * @param {string} name
 * @returns {*}
 */
function lookup(data, name) {
  if (data == null) return undefined;
  return data[name];
}

/**
 * Renders a single `{{ name | filter | ... }}` tag body (text between the
 * braces) to its substituted string value.
 * @param {string} inner
 * @param {object} data
 * @returns {string}
 */
function renderTag(inner, data) {
  const parts = inner.split('|').map((s) => s.trim());
  const name = parts[0];
  let value = lookup(data, name);
  if (parts.length === 1) {
    return FILTERS.raw(value);
  }
  for (let i = 1; i < parts.length; i++) {
    value = applyFilter(parts[i], value);
  }
  return String(value);
}

/**
 * Finds the first head/body boundary (`\n\n` or `\r\n\r\n`) in `s`.
 * @param {string} s
 * @returns {{ index: number, len: number } | null}
 */
function findBoundary(s) {
  const lf = s.indexOf('\n\n');
  const crlf = s.indexOf('\r\n\r\n');
  if (lf === -1 && crlf === -1) return null;
  if (crlf !== -1 && (lf === -1 || crlf <= lf)) return { index: crlf, len: 4 };
  return { index: lf, len: 2 };
}

/**
 * Extracts the `:httpt-body-type` value from a resolved head string, if present.
 * @param {string} head
 * @returns {string | null}
 */
function pseudoBodyType(head) {
  const m = head.match(/(?:^|\n)[ \t]*:httpt-body-type[ \t]*:[ \t]*([^\r\n]*)/i);
  return m ? m[1].trim().toLowerCase() : null;
}

/**
 * @param {*} v
 * @returns {number}
 */
function streamIndexOf(v) {
  return Number.isInteger(v) ? v : 0;
}

/**
 * Single-pass, chunk-safe hydration of an httpt template into three streams.
 *
 * Returns SYNCHRONOUSLY (incubation-js.md §1): the stream objects are wired up
 * immediately and a detached background worker fills them. `resolvedStream`
 * (head) and `mapStream` are `AsyncIterable`s; `bodyStream` is a Web
 * `ReadableStream<Uint8Array>`. Errors from the worker propagate to whichever
 * outputs are still open via fail-fast.
 *
 * @param {string | ReadableStream | AsyncIterable | Uint8Array} template
 * @param {object} [data={}]
 * @param {Array<*>} [streams=[]]
 * @returns {{ resolvedStream: AsyncIterable<string>, mapStream: AsyncIterable<object>, bodyStream: ReadableStream<Uint8Array> }}
 */
function hydrate(template, data = {}, streams = []) {
  const resolvedChannel = createChannel();
  const mapChannel = createChannel();

  let bodyController;
  let resolvePull = null;
  const bodyStream = new ReadableStream({
    start(controller) {
      bodyController = controller;
    },
    pull() {
      // The consumer wants more: release the worker if it is parked on backpressure.
      if (resolvePull) {
        const resolve = resolvePull;
        resolvePull = null;
        resolve();
      }
    },
  });
  const awaitPull = () => new Promise((resolve) => {
    resolvePull = resolve;
  });

  const encoder = new TextEncoder();
  const dynamicHeaders = Array.isArray(data && data.headers) ? data.headers : [];
  const dynamicBody = data && typeof data.body === 'object' && data.body !== null ? data.body : undefined;

  // --- state machine state ---
  let buf = ''; // unconsumed input tail (may hold a partial tag / boundary)
  let phase = 'head'; // 'head' | 'body'
  let headBuf = ''; // resolved head accumulator (flushed at finalizeHead)
  let resolvedIndex = 0; // offset into the FULL resolved string (head + boundary + body)
  let sourceIndex = 0; // offset into the original template
  let headFinalized = false;
  // 'dynamic' is known up front (data.body supplied); 'inline'/'provided-shadow'
  // are resolved from the template head at finalizeHead().
  let bodyMode = dynamicBody ? 'dynamic' : null;

  function emitHead(str) {
    if (!str) return;
    headBuf += str;
    resolvedIndex += str.length;
  }

  function emitBody(str) {
    if (!str) return;
    bodyController.enqueue(encoder.encode(str));
    resolvedIndex += str.length;
  }

  function emitResolved(str) {
    if (phase === 'head') emitHead(str);
    else emitBody(str);
  }

  /**
   * Records a resolved tag in the map stream and emits its value.
   * @param {number} originalStart
   * @param {number} originalLength
   * @param {string} value
   */
  function recordTag(originalStart, originalLength, value) {
    const hydratedStart = resolvedIndex;
    emitResolved(value);
    mapChannel.push({
      'hydrated-start': hydratedStart,
      'original-start': originalStart,
      'hydrated-length': value.length,
      'original-length': originalLength,
    });
  }

  function finalizeHead() {
    // Determine body routing from the template head (before injection).
    const pseudo = pseudoBodyType(headBuf);
    if (dynamicBody) bodyMode = 'dynamic';
    else if (pseudo === 'provided') bodyMode = 'provided-shadow';
    else bodyMode = 'inline';

    // Dynamic injection (§1.3.4 / §1.3.5): headers first, then the body pseudo-header.
    const lines = [];
    for (const h of dynamicHeaders) lines.push(`${h.name}: ${h.value}`);
    if (dynamicBody) lines.push(`:httpt-body-type: ${dynamicBody.type}`);
    if (lines.length) {
      const inj = headBuf.endsWith('\n')
        ? lines.map((l) => l + '\n').join('')
        : lines.map((l) => '\n' + l).join('');
      emitHead(inj);
    }

    resolvedChannel.push(headBuf);
    resolvedChannel.end();
    headFinalized = true;
  }

  /**
   * Advances the head state machine over `buf`, emitting resolved head text and
   * map entries, until it either exhausts safely-processable input or crosses
   * the head/body boundary.
   * @param {boolean} isFinal
   */
  function stepHead(isFinal) {
    for (;;) {
      const tagIdx = buf.indexOf('{{');
      const boundary = findBoundary(buf);

      let event = null;
      if (tagIdx !== -1 && (!boundary || tagIdx < boundary.index)) {
        event = { type: 'tag', index: tagIdx };
      } else if (boundary) {
        event = { type: 'boundary', index: boundary.index, len: boundary.len };
      }

      if (!event) {
        // No complete tag or boundary: emit safe literal, hold ambiguous tail.
        emitSafeLiteralHead(isFinal);
        return;
      }

      // Literal text before the event is definite head content.
      const literal = buf.slice(0, event.index);
      emitHead(literal);
      sourceIndex += literal.length;
      buf = buf.slice(event.index);

      if (event.type === 'tag') {
        const close = buf.indexOf('}}', 2);
        if (close === -1) {
          if (isFinal) {
            emitHead(buf); // unterminated tag → emit verbatim
            sourceIndex += buf.length;
            buf = '';
            return;
          }
          return; // hold: wait for the closing braces
        }
        const inner = buf.slice(2, close);
        const tagLen = close + 2;
        recordTag(sourceIndex, tagLen, renderTag(inner, data));
        sourceIndex += tagLen;
        buf = buf.slice(tagLen);
      } else {
        // Boundary: consume it and switch to body phase.
        //
        // When data.body was supplied, DEFER finalizing the head: a template
        // body here is a BodyConflictError, and if we finalized (ending the
        // resolved stream) the error would fire after parse already read the
        // head. Keeping the resolved stream open lets the error reach parse.
        if (!dynamicBody) finalizeHead();
        resolvedIndex += event.len;
        sourceIndex += event.len;
        buf = buf.slice(event.len);
        phase = 'body';
        return;
      }
    }
  }

  /**
   * Emits as much of `buf` as is unambiguously head literal, holding back a tail
   * that could be the start of a tag (`{`) or a boundary (`\n`, `\r\n\r`, ...).
   * @param {boolean} isFinal
   */
  function emitSafeLiteralHead(isFinal) {
    if (isFinal) {
      emitHead(buf);
      sourceIndex += buf.length;
      buf = '';
      return;
    }
    let hold = 0;
    if (buf.endsWith('\r\n\r')) hold = 3;
    else if (buf.endsWith('\r\n')) hold = 2;
    else if (buf.endsWith('\r')) hold = 1;
    else if (buf.endsWith('\n')) hold = 1;
    if (buf.endsWith('{')) hold = Math.max(hold, 1);
    const safe = hold ? buf.slice(0, buf.length - hold) : buf;
    if (safe) {
      emitHead(safe);
      sourceIndex += safe.length;
      buf = buf.slice(safe.length);
    }
  }

  /**
   * Advances the body state machine over `buf` per the resolved `bodyMode`.
   * @param {boolean} isFinal
   */
  function stepBody(isFinal) {
    if (bodyMode === 'dynamic') {
      // data.body was supplied: the template must have an (at most whitespace) body.
      if (/\S/.test(buf)) {
        const err = new Error('Template defines a body but data.body was also provided');
        err.name = 'BodyConflictError';
        throw err;
      }
      buf = '';
      return;
    }
    if (bodyMode === 'provided-shadow') {
      buf = ''; // shadow body is ignored
      return;
    }
    // inline body: substitute tags, stream bytes.
    for (;;) {
      const tagIdx = buf.indexOf('{{');
      if (tagIdx === -1) {
        if (isFinal) {
          emitBody(buf);
          sourceIndex += buf.length;
          buf = '';
          return;
        }
        const hold = buf.endsWith('{') ? 1 : 0;
        const safe = hold ? buf.slice(0, buf.length - hold) : buf;
        if (safe) {
          emitBody(safe);
          sourceIndex += safe.length;
          buf = buf.slice(safe.length);
        }
        return;
      }
      const literal = buf.slice(0, tagIdx);
      emitBody(literal);
      sourceIndex += literal.length;
      buf = buf.slice(tagIdx);
      const close = buf.indexOf('}}', 2);
      if (close === -1) {
        if (isFinal) {
          emitBody(buf);
          buf = '';
          return;
        }
        return;
      }
      const inner = buf.slice(2, close);
      const tagLen = close + 2;
      recordTag(sourceIndex, tagLen, renderTag(inner, data));
      sourceIndex += tagLen;
      buf = buf.slice(tagLen);
    }
  }

  function step(isFinal) {
    if (phase === 'head') stepHead(isFinal);
    if (phase === 'body') stepBody(isFinal);
  }

  async function processTemplate() {
    for await (const chunk of toTextChunks(template)) {
      buf += chunk;
      step(false);
    }
    step(true);
    if (!headFinalized) finalizeHead(); // head-only template (EOF before any boundary)

    // The map is complete once the template (head + inline body tags) is fully
    // processed. End it BEFORE the out-of-band body drain so map consumers (e.g.
    // build()) never block on body backpressure.
    mapChannel.end();

    // Out-of-band body: stream the referenced native stream into the body with
    // backpressure so large/live payloads stay O(1) in memory.
    if (bodyMode === 'dynamic' || bodyMode === 'provided-shadow') {
      const idx = dynamicBody ? streamIndexOf(dynamicBody.content) : 0;
      const src = streams && streams[idx];
      if (src != null) {
        for await (const chunk of toByteChunks(src)) {
          while (bodyController.desiredSize !== null && bodyController.desiredSize <= 0) {
            await awaitPull();
          }
          if (bodyController.desiredSize === null) break; // consumer cancelled
          bodyController.enqueue(chunk);
        }
      }
    }

    try {
      bodyController.close();
    } catch (_) {
      /* already closed or cancelled */
    }
  }

  // Detached background worker (never awaited); fail-fast on error.
  processTemplate().catch((err) => {
    resolvedChannel.fail(err);
    mapChannel.fail(err);
    try {
      bodyController.error(err);
    } catch (_) {
      /* already closed/errored */
    }
  });

  return {
    resolvedStream: resolvedChannel.iterable,
    mapStream: mapChannel.iterable,
    bodyStream,
  };
}

module.exports = { hydrate };
