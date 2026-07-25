'use strict';

const { toByteChunks } = require('./streams');

/**
 * Stream-reference validation and materialization (incubation.md §1.3.2.3,
 * §1.4.4, §1.4.5).
 *
 * A "reference" is a `provided` StreamDefinition used in the hydration context:
 * the body (`data.body` or a template `:httpt-body-type: provided`) and each
 * streaming-filter tag (`{{ name | stream-as-* }}`). No official fixtures cover
 * these paths; semantics here follow the spec text.
 */

const STREAM_FILTERS = new Set(['stream-as-base64', 'stream-as-utf8', 'stream-as-is']);
const STREAM_TAG_RE = /\{\{\s*([^|}]+?)\s*\|\s*(stream-as-base64|stream-as-utf8|stream-as-is)\s*\}\}/g;
const PSEUDO_PROVIDED_RE = /(^|\n)[ \t]*:httpt-body-type[ \t]*:[ \t]*provided/i;

/**
 * @typedef {Object} StreamRef
 * @property {number} index    - resolved stream index
 * @property {boolean} explicit - whether an index was explicitly given
 * @property {string} where    - 'body' | `filter:<name>`
 */

/**
 * Resolves a StreamDefinition-ish value to a { index, explicit } pair.
 * @param {*} def
 * @returns {{ index: number, explicit: boolean }}
 */
function resolveRef(def) {
  if (def && typeof def === 'object' && Number.isInteger(def.content)) {
    return { index: def.content, explicit: true };
  }
  if (Number.isInteger(def)) return { index: def, explicit: true };
  return { index: 0, explicit: false }; // Implicit Default (§1.4.4)
}

/**
 * Collects every provided-stream reference in the hydration context.
 * @param {*} template
 * @param {object} data
 * @returns {StreamRef[]}
 */
function collectStreamReferences(template, data) {
  /** @type {StreamRef[]} */
  const refs = [];

  const body = data && typeof data.body === 'object' && data.body !== null ? data.body : null;
  if (body && body.type === 'provided') {
    const { index, explicit } = resolveRef(body);
    refs.push({ index, explicit, where: 'body' });
  } else if (typeof template === 'string' && PSEUDO_PROVIDED_RE.test(template)) {
    refs.push({ index: 0, explicit: false, where: 'body' });
  }

  // Streaming-filter references require a scannable (string) template.
  if (typeof template === 'string') {
    STREAM_TAG_RE.lastIndex = 0;
    let m;
    while ((m = STREAM_TAG_RE.exec(template)) !== null) {
      const name = m[1].trim();
      const { index, explicit } = resolveRef(data ? data[name] : undefined);
      refs.push({ index, explicit, where: `filter:${name}` });
    }
  }

  return refs;
}

/**
 * Enforces the §1.4.4 rules on a set of references.
 * @param {StreamRef[]} refs
 * @throws {Error} AmbiguousStreamReferenceError | DuplicateStreamReferenceError
 */
function validateStreamReferences(refs) {
  if (refs.length <= 1) return; // Implicit default is fine for a single reference.

  // Ambiguity Error: with >1 reference, all MUST be explicit.
  if (refs.some((r) => !r.explicit)) {
    const err = new Error(
      'Multiple provided streams are referenced but at least one omits an explicit content index'
    );
    err.name = 'AmbiguousStreamReferenceError';
    throw err;
  }

  // Uniqueness Error: every index MUST be unique.
  const seen = new Set();
  for (const r of refs) {
    if (seen.has(r.index)) {
      const err = new Error(`Duplicate provided-stream index ${r.index}`);
      err.name = 'DuplicateStreamReferenceError';
      throw err;
    }
    seen.add(r.index);
  }
}

/**
 * Buffers a native stream source into a single Uint8Array (§1.4.5
 * materialization for streams used inline in the head/body).
 * @param {*} source
 * @returns {Promise<Uint8Array>}
 */
async function materializeStream(source) {
  const chunks = [];
  let total = 0;
  for await (const chunk of toByteChunks(source)) {
    chunks.push(chunk);
    total += chunk.byteLength;
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const c of chunks) {
    out.set(c, offset);
    offset += c.byteLength;
  }
  return out;
}

module.exports = {
  STREAM_FILTERS,
  collectStreamReferences,
  validateStreamReferences,
  materializeStream,
  resolveRef,
};
