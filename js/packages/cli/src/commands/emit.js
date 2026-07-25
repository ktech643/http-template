const { spawn } = require('node:child_process');
const { Readable } = require('node:stream');
const { dispatchFetch } = require('@httpt/core');

/**
 * @typedef {import('@httpt/core/src/types').HttptIR} HttptIR
 */

/**
 * Dispatches an httpt IR via curl.
 * @param {HttptIR} ir
 * @param {string} scheme - e.g., "https", "http"
 * @param {import('node:stream').Readable | ReadableStream | null} [bodyStream=null]
 * @returns {Promise<void>}
 */
function dispatchCurl(ir, scheme, bodyStream = null) {
  return new Promise((resolve, reject) => {
    const url = `${scheme}://${ir.host}${ir.uri}`;
    const args = ['-s', '-v', '-X', ir.method];

    if (ir.version === 'HTTP/1.0') args.push('--http1.0');
    else if (ir.version === 'HTTP/1.1') args.push('--http1.1');
    else if (ir.version === 'HTTP/2' || ir.version === 'HTTP/2.0') args.push('--http2');
    else if (ir.version === 'HTTP/3') args.push('--http3');

    for (const { name, value } of ir.headers) {
      args.push('-H', value ? `${name}: ${value}` : `${name};`);
    }

    let buffer = null;
    let hasBodyData = false;

    if (ir.body) {
      hasBodyData = true;
      if (ir.body.type === 'text') {
        buffer = Buffer.from(ir.body.content, 'utf-8');
      } else if (ir.body.type === 'json') {
        buffer = Buffer.from(JSON.stringify(ir.body.content), 'utf-8');
      } else if (ir.body.type === 'base64') {
        buffer = Buffer.from(ir.body.content, 'base64');
      } else if (ir.body.type !== 'provided') {
        throw new Error(`Unsupported httpt-ir body type: ${ir.body.type}`);
      }
      args.push('--data-binary', '@-');
    }
    args.push(url);

    const curlProc = spawn('curl', args, { stdio: ['pipe', 'inherit', 'inherit'] });
    curlProc.on('close', (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`curl process exited with code ${code}`));
      }
    });
    curlProc.on('error', reject);

    if (hasBodyData) {
      if (buffer) {
        curlProc.stdin.end(buffer);
        if (bodyStream) {
          if (typeof bodyStream.cancel === 'function') {
            bodyStream.cancel(); // Web Stream
          } else if (typeof bodyStream.destroy === 'function') {
            bodyStream.destroy(); // Node Stream
          }
        }
      } else if (bodyStream) {
        if (typeof bodyStream.cancel === 'function') {
          // Web stream
          Readable.fromWeb(bodyStream).pipe(curlProc.stdin);
        } else {
          // Node stream
          bodyStream.pipe(curlProc.stdin);
        }
      } else {
        curlProc.stdin.end();
      }
    } else {
      curlProc.stdin.end();
      if (bodyStream) {
        if (typeof bodyStream.cancel === 'function') {
          bodyStream.cancel(); // Web Stream
        } else if (typeof bodyStream.destroy === 'function') {
          bodyStream.destroy(); // Node Stream
        }
      }
    }
  });
}

const { build } = require('@httpt/core');
const { loadTemplate, loadData } = require('../io');

/**
 * `httpt emit <file> --target <curl|fetch>` — build the IR and dispatch it via
 * the chosen executor. Defaults to the fetch executor.
 * @param {string} file
 * @param {Record<string, string | boolean>} flags
 */
async function emitCommand(file, flags) {
  const template = loadTemplate(file);
  const data = loadData(file, flags);
  const target = typeof flags.target === 'string' ? flags.target : 'fetch';
  const scheme = typeof flags.scheme === 'string' ? flags.scheme : 'https';

  const { ir, bodyStream } = await build(template, data);

  if (target === 'curl') {
    await dispatchCurl(ir, scheme, bodyStream);
    return;
  }
  if (target === 'fetch') {
    const res = await dispatchFetch(ir, scheme, bodyStream);
    const text = await res.text();
    console.log(`HTTP ${res.status} ${res.statusText}`);
    console.log(text);
    return;
  }
  throw new Error(`Unknown --target '${target}' (expected 'curl' or 'fetch')`);
}

module.exports = { emitCommand, dispatchCurl };
