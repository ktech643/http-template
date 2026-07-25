'use strict';

const { hydrate } = require('@httpt/core');
const { loadTemplate, loadData, readStream } = require('../io');

/**
 * `httpt hydrate <file>` — resolve the template and print the hydrated request.
 * With `--shift-map`, also print the Index Shift Map as JSON.
 * @param {string} file
 * @param {Record<string, string | boolean>} flags
 */
async function hydrateCommand(file, flags) {
  const template = loadTemplate(file);
  const data = loadData(file, flags);

  const { resolvedStream, mapStream, bodyStream } = hydrate(template, data);

  let head = '';
  for await (const chunk of resolvedStream) head += chunk;

  const map = [];
  for await (const entry of mapStream) map.push(entry);

  const body = await readStream(bodyStream);
  const resolved = body.length > 0 ? `${head}\n\n${body.toString('utf8')}` : head;

  process.stdout.write(resolved);
  if (!resolved.endsWith('\n')) process.stdout.write('\n');

  if (flags['shift-map']) {
    console.log(JSON.stringify(map, null, 2));
  }
}

module.exports = hydrateCommand;
