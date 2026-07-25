'use strict';

const fs = require('node:fs');
const path = require('node:path');

/**
 * Reads the template file as a string.
 * @param {string} file
 * @returns {string}
 */
function loadTemplate(file) {
  return fs.readFileSync(file, 'utf8');
}

/**
 * Resolves the data context for a template. Precedence: `--data <path>` flag,
 * then a sibling `<base>.data.json`, else `{}`.
 * @param {string} file
 * @param {Record<string, string | boolean>} flags
 * @returns {object}
 */
function loadData(file, flags) {
  const explicit = typeof flags.data === 'string' ? flags.data : null;
  const sibling = file.endsWith('.httpt') ? file.slice(0, -'.httpt'.length) + '.data.json' : null;
  const dataPath = explicit || (sibling && fs.existsSync(sibling) ? sibling : null);
  if (!dataPath) return {};
  return JSON.parse(fs.readFileSync(dataPath, 'utf8'));
}

/**
 * Fully reads a Web ReadableStream into a Buffer.
 * @param {ReadableStream<Uint8Array> | null} stream
 * @returns {Promise<Buffer>}
 */
async function readStream(stream) {
  if (!stream) return Buffer.alloc(0);
  const reader = stream.getReader();
  const chunks = [];
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      if (value !== undefined) chunks.push(Buffer.from(value));
    }
  } finally {
    reader.releaseLock();
  }
  return Buffer.concat(chunks);
}

module.exports = { loadTemplate, loadData, readStream, path };
