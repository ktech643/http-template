'use strict';

const { drainReadable } = require('./streams');

/**
 * @typedef {import('./types').HttptIR} HttptIR
 */

/**
 * Splits a request line into method / uri / version, tolerating extra internal
 * whitespace. The first token is the method, the last is the version, and
 * everything between is the URI.
 * @param {string} line
 * @returns {{ method: string, uri: string, version: string }}
 */
function parseRequestLine(line) {
  const parts = line.trim().split(/\s+/).filter(Boolean);
  if (parts.length === 0) return { method: '', uri: '', version: '' };
  if (parts.length === 1) return { method: parts[0], uri: '', version: '' };
  if (parts.length === 2) return { method: parts[0], uri: parts[1], version: '' };
  return {
    method: parts[0],
    version: parts[parts.length - 1],
    uri: parts.slice(1, -1).join(' '),
  };
}

const PSEUDO_BODY_TYPE = /^[ \t]*:httpt-body-type[ \t]*:/i;

/**
 * Deconstructs a hydrated request into its Intermediate Representation.
 *
 * Consumes the resolved HEAD (an `AsyncIterable<string>`) to build the request
 * line, headers, host, and body type, then materializes or hands off the body
 * from `bodyStream` depending on the body type.
 *
 * @param {AsyncIterable<string>} resolvedIterable - resolved head text
 * @param {ReadableStream<Uint8Array> | null} [bodyStream=null] - resolved body bytes
 * @returns {Promise<{ ir: HttptIR, bodyStream: ReadableStream<Uint8Array> | null }>}
 */
async function parse(resolvedIterable, bodyStream = null) {
  let head = '';
  if (resolvedIterable) {
    for await (const chunk of resolvedIterable) head += chunk;
  }

  const lines = head.split('\n');
  while (lines.length && lines[lines.length - 1] === '') lines.pop();

  const requestLine = lines.shift() || '';
  const { method, uri, version } = parseRequestLine(requestLine);

  let host = '';
  let bodyType = null;
  const headers = [];

  for (const rawLine of lines) {
    const line = rawLine.endsWith('\r') ? rawLine.slice(0, -1) : rawLine;
    if (line === '') continue;

    // Pseudo-header: consumed to set body type, never emitted to IR headers.
    if (PSEUDO_BODY_TYPE.test(line)) {
      const firstColon = line.indexOf(':');
      const secondColon = line.indexOf(':', firstColon + 1);
      bodyType = line.slice(secondColon + 1).trim().toLowerCase();
      continue;
    }

    // A leading colon marks a pseudo-header (e.g. ":authority"); the separator
    // colon is the NEXT one, not the leading one.
    const searchStart = line.startsWith(':') ? 1 : 0;
    const colon = line.indexOf(':', searchStart);
    if (colon === -1) {
      headers.push({ name: line, value: '' });
      continue;
    }
    const name = line.slice(0, colon);
    const value = line.slice(colon + 1).replace(/^[ \t]+/, '');

    // Lift the authority (Host header or :authority pseudo-header) into the root
    // host field (§1.4.6); it is never emitted as an IR header.
    const lname = name.toLowerCase();
    if (lname === 'host' || lname === ':authority') {
      host = value;
      continue;
    }
    headers.push({ name, value });
  }

  /** @type {HttptIR} */
  const ir = {
    'schema-version': '1.0',
    method,
    host,
    uri,
    version,
    headers,
  };

  if (bodyType === 'provided') {
    // Out-of-band body: record the stream index, hand off the stream unread.
    ir.body = { type: 'provided', content: 0 };
  } else if (bodyType === 'json' || bodyType === 'text' || bodyType === 'base64') {
    const text = await readBodyText(bodyStream);
    if (bodyType === 'json') ir.body = { type: 'json', content: JSON.parse(text) };
    else ir.body = { type: bodyType, content: text };
  } else {
    // No pseudo-header: default to text iff the body carries any bytes.
    const bytes = await drainReadable(bodyStream);
    if (bytes.byteLength > 0) {
      ir.body = { type: 'text', content: trimTrailingNewline(decode(bytes)) };
    }
  }

  return { ir, bodyStream };
}

/**
 * @param {ReadableStream<Uint8Array> | null} bodyStream
 * @returns {Promise<string>}
 */
async function readBodyText(bodyStream) {
  const bytes = await drainReadable(bodyStream);
  return trimTrailingNewline(decode(bytes));
}

/**
 * @param {Uint8Array} bytes
 * @returns {string}
 */
function decode(bytes) {
  return new TextDecoder('utf-8').decode(bytes);
}

/**
 * Strips exactly one trailing line terminator (`\n` or `\r\n`); internal and
 * leading newlines are preserved (fixture 014).
 * @param {string} s
 * @returns {string}
 */
function trimTrailingNewline(s) {
  return s.replace(/\r?\n$/, '');
}

module.exports = { parse };
