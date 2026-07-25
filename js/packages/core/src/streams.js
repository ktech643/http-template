'use strict';

/**
 * Streaming primitives for the httpt pipeline.
 *
 * The public `hydrate` signature (incubation-js.md §1) is a hybrid: the
 * `resolvedStream` and `mapStream` are `AsyncIterable`s (cheap, native async
 * generators) while `bodyStream` is a Web `ReadableStream<Uint8Array>` (real
 * backpressure for large payloads). A `Channel` is the single-producer /
 * single-consumer queue that backs the two async-iterable outputs and lets the
 * detached background worker push values, signal completion, or fail-fast an
 * error that surfaces at the consumer's `for await`.
 */

/**
 * @template T
 * @typedef {Object} Channel
 * @property {(value: T) => void} push
 * @property {() => void} end
 * @property {(err: Error) => void} fail
 * @property {AsyncIterable<T>} iterable
 */

/**
 * Creates a single-consumer async channel.
 * @template T
 * @returns {Channel<T>}
 */
function createChannel() {
  /** @type {Array<{ value?: T, err?: Error, done?: boolean }>} */
  const queue = [];
  /** @type {{ resolve: Function, reject: Function } | null} */
  let waiter = null;
  let closed = false;

  function settle() {
    if (!waiter || queue.length === 0) return;
    const item = queue.shift();
    const w = waiter;
    waiter = null;
    if (item.err) w.reject(item.err);
    else if (item.done) w.resolve({ value: undefined, done: true });
    else w.resolve({ value: item.value, done: false });
  }

  const iterable = {
    [Symbol.asyncIterator]() {
      return {
        next() {
          return new Promise((resolve, reject) => {
            waiter = { resolve, reject };
            settle();
          });
        },
        [Symbol.asyncIterator]() {
          return this;
        },
      };
    },
  };

  return {
    push(value) {
      if (closed) return;
      queue.push({ value });
      settle();
    },
    end() {
      if (closed) return;
      closed = true;
      queue.push({ done: true });
      settle();
    },
    fail(err) {
      if (closed) return;
      closed = true;
      queue.push({ err });
      settle();
    },
    iterable,
  };
}

/**
 * Normalizes a polymorphic template input into an async iterable of decoded
 * text chunks. Supports `string`, `Uint8Array`/`Buffer`, Web `ReadableStream`,
 * Node `Readable`, and any `AsyncIterable` of strings or byte chunks. UTF-8 is
 * decoded with a streaming `TextDecoder` so multi-byte characters split across
 * chunk boundaries are handled correctly.
 * @param {*} template
 * @returns {AsyncIterable<string>}
 */
async function* toTextChunks(template) {
  if (template == null) return;

  if (typeof template === 'string') {
    yield template;
    return;
  }

  const decoder = new TextDecoder('utf-8');
  const decode = (chunk) => {
    if (typeof chunk === 'string') return chunk;
    return decoder.decode(chunk, { stream: true });
  };

  // Web ReadableStream (has getReader) — read via a reader.
  if (typeof template.getReader === 'function') {
    const reader = template.getReader();
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        if (value !== undefined) {
          const text = decode(value);
          if (text) yield text;
        }
      }
    } finally {
      reader.releaseLock();
    }
    const tail = decoder.decode();
    if (tail) yield tail;
    return;
  }

  // Anything async-iterable (Node Readable, async generators, etc.).
  if (typeof template[Symbol.asyncIterator] === 'function') {
    for await (const chunk of template) {
      const text = decode(chunk);
      if (text) yield text;
    }
    const tail = decoder.decode();
    if (tail) yield tail;
    return;
  }

  // A single byte chunk.
  if (ArrayBuffer.isView(template)) {
    yield decoder.decode(template);
    return;
  }

  throw new TypeError('Unsupported template input type');
}

/**
 * Normalizes a provided body source into an async iterable of `Uint8Array`
 * chunks, so it can be piped into the body `ReadableStream` with O(1) memory.
 * @param {*} source - string, Uint8Array/Buffer, Web ReadableStream, Node Readable, Blob, or AsyncIterable
 * @returns {AsyncIterable<Uint8Array>}
 */
async function* toByteChunks(source) {
  if (source == null) return;

  if (typeof source === 'string') {
    yield new TextEncoder().encode(source);
    return;
  }

  if (ArrayBuffer.isView(source)) {
    yield source instanceof Uint8Array ? source : new Uint8Array(source.buffer, source.byteOffset, source.byteLength);
    return;
  }

  // Blob / File (has arrayBuffer()).
  if (typeof source.arrayBuffer === 'function' && typeof source.stream !== 'function') {
    yield new Uint8Array(await source.arrayBuffer());
    return;
  }

  const asStream = typeof source.stream === 'function' ? source.stream() : source;

  if (typeof asStream.getReader === 'function') {
    const reader = asStream.getReader();
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        if (value !== undefined) yield toUint8(value);
      }
    } finally {
      reader.releaseLock();
    }
    return;
  }

  if (typeof asStream[Symbol.asyncIterator] === 'function') {
    for await (const chunk of asStream) yield toUint8(chunk);
    return;
  }

  throw new TypeError('Unsupported provided-stream source type');
}

/**
 * @param {*} chunk
 * @returns {Uint8Array}
 */
function toUint8(chunk) {
  if (typeof chunk === 'string') return new TextEncoder().encode(chunk);
  if (chunk instanceof Uint8Array) return chunk;
  if (ArrayBuffer.isView(chunk)) return new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength);
  if (chunk instanceof ArrayBuffer) return new Uint8Array(chunk);
  return new Uint8Array(chunk);
}

/**
 * Fully drains a Web `ReadableStream<Uint8Array>` into a single `Uint8Array`.
 * Used by `parse` when it must materialize an inline body (text/json/base64).
 * @param {ReadableStream<Uint8Array>} readable
 * @returns {Promise<Uint8Array>}
 */
async function drainReadable(readable) {
  if (!readable) return new Uint8Array(0);
  const reader = readable.getReader();
  const chunks = [];
  let total = 0;
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      if (value !== undefined) {
        const bytes = toUint8(value);
        chunks.push(bytes);
        total += bytes.byteLength;
      }
    }
  } finally {
    reader.releaseLock();
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const c of chunks) {
    out.set(c, offset);
    offset += c.byteLength;
  }
  return out;
}

module.exports = { createChannel, toTextChunks, toByteChunks, toUint8, drainReadable };
