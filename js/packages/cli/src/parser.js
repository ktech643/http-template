'use strict';

/**
 * Parses `httpt <command> <file> [--flag] [--flag=value] [--flag value] ...`.
 *
 * A `--flag` immediately followed by a non-flag token is treated as a value
 * pair once the positional file has already been captured; otherwise the token
 * is taken as the file and `--flag` is a boolean.
 *
 * @param {string[]} argv - typically `process.argv`
 * @returns {{ command: string, file: string | null, flags: Record<string, string | boolean> }}
 */
function parseArgs(argv) {
  const args = argv.slice(2);
  const command = args[0] || 'help';
  let file = null;
  /** @type {Record<string, string | boolean>} */
  const flags = {};

  for (let i = 1; i < args.length; i++) {
    const arg = args[i];
    if (!arg.startsWith('--')) {
      if (file === null) file = arg;
      continue;
    }

    const body = arg.slice(2);
    const eq = body.indexOf('=');
    if (eq !== -1) {
      flags[body.slice(0, eq)] = body.slice(eq + 1);
      continue;
    }

    const next = args[i + 1];
    if (file !== null && next !== undefined && !next.startsWith('--')) {
      flags[body] = next;
      i++;
    } else {
      flags[body] = true;
    }
  }

  return { command, file, flags };
}

module.exports = { parseArgs };
