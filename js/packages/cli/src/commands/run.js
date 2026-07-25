'use strict';

const { build, execute } = require('@httpt/core');
const { loadTemplate, loadData } = require('../io');

/**
 * `httpt run <file>` — orchestrate the full pipeline. With `--dry-run`, print
 * the resulting IR without dispatching; otherwise execute the request over the
 * network and print the response status and body.
 * @param {string} file
 * @param {Record<string, string | boolean>} flags
 */
async function runCommand(file, flags) {
  const template = loadTemplate(file);
  const data = loadData(file, flags);

  if (flags['dry-run']) {
    const { ir } = await build(template, data);
    console.log(JSON.stringify(ir, null, 2));
    return;
  }

  const scheme = typeof flags.scheme === 'string' ? flags.scheme : 'https';
  const res = await execute(template, data, [], { scheme });
  const text = await res.text();
  console.log(`HTTP ${res.status} ${res.statusText}`);
  console.log(text);
}

module.exports = runCommand;
