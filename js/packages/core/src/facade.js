'use strict';

const { hydrate, parse } = require('./pipeline');
const { dispatchFetch } = require('./dispatch');

/**
 * @typedef {import('./types').HttptIR} HttptIR
 */

/**
 * Runs the full hydrate -> parse pipeline and returns the structured result:
 * the Intermediate Representation, the collected Index Shift Map, and the
 * (possibly unread, for `provided` bodies) body stream ready for execution.
 *
 * @param {string | ReadableStream | AsyncIterable | Uint8Array} template
 * @param {object} [data={}]
 * @param {Array<*>} [streams=[]]
 * @returns {Promise<{ ir: HttptIR, map: object[], bodyStream: ReadableStream<Uint8Array> | null }>}
 */
async function build(template, data = {}, streams = []) {
  const { resolvedStream, mapStream, bodyStream } = hydrate(template, data, streams);

  // Collect the map concurrently with parse so neither stalls the worker.
  const mapPromise = (async () => {
    const entries = [];
    for await (const entry of mapStream) entries.push(entry);
    return entries;
  })();

  // Await both together. A worker failure fails BOTH the resolved and map
  // channels; if we awaited parse first and it rejected, mapPromise's rejection
  // would be orphaned (an unhandledRejection that aborts the process). Promise.all
  // attaches a handler to each promise up front, so the map rejection is always
  // observed while the caller still sees parse's error first.
  const [{ ir, bodyStream: outBody }, map] = await Promise.all([
    parse(resolvedStream, bodyStream),
    mapPromise,
  ]);

  return { ir, map, bodyStream: outBody };
}

/**
 * Builds and dispatches a request via the native fetch executor.
 * @param {string | ReadableStream | AsyncIterable | Uint8Array} template
 * @param {object} [data={}]
 * @param {Array<*>} [streams=[]]
 * @param {{ scheme?: string }} [config={}]
 * @returns {Promise<Response>}
 */
async function execute(template, data = {}, streams = [], config = {}) {
  const { ir, bodyStream } = await build(template, data, streams);
  const scheme = config.scheme || 'https';
  return dispatchFetch(ir, scheme, bodyStream);
}

module.exports = { build, execute };
