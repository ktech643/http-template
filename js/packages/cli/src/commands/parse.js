'use strict';

const { build } = require('@httpt/core');
const { loadTemplate, loadData } = require('../io');

/**
 * `httpt parse <file>` — hydrate then parse, printing the Intermediate
 * Representation as pretty JSON.
 * @param {string} file
 * @param {Record<string, string | boolean>} flags
 */
async function parseCommand(file, flags) {
  const template = loadTemplate(file);
  const data = loadData(file, flags);
  const { ir } = await build(template, data);
  console.log(JSON.stringify(ir, null, 2));
}

module.exports = parseCommand;
