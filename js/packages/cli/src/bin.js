#!/usr/bin/env node
'use strict';

const { parseArgs } = require('./parser');
const hydrateCommand = require('./commands/hydrate');
const parseCommand = require('./commands/parse');
const { emitCommand } = require('./commands/emit');
const runCommand = require('./commands/run');

const USAGE = `httpt <command> <file> [options]

Commands:
  hydrate <file> [--shift-map]           Resolve the template; print the hydrated request
  parse   <file>                         Print the parsed Intermediate Representation (IR)
  run     <file> [--dry-run]             Full pipeline; --dry-run prints the IR without sending
  emit    <file> [--target curl|fetch]   Build the IR and dispatch it

Options:
  --data <path>     Data context JSON (defaults to a sibling <base>.data.json)
  --scheme <s>      URL scheme for dispatch (default: https)`;

const COMMANDS = {
  hydrate: hydrateCommand,
  parse: parseCommand,
  emit: emitCommand,
  run: runCommand,
};

async function main() {
  const { command, file, flags } = parseArgs(process.argv);

  if (command === 'help' || flags.help) {
    console.log(USAGE);
    return;
  }

  const handler = COMMANDS[command];
  if (!handler) {
    console.error(`Unknown command: ${command}\n`);
    console.error(USAGE);
    process.exitCode = 1;
    return;
  }

  if (!file) {
    console.error(`Command '${command}' requires a <file> argument.`);
    process.exitCode = 1;
    return;
  }

  await handler(file, flags);
}

main().catch((err) => {
  console.error(`${err.name || 'Error'}: ${err.message}`);
  process.exitCode = 1;
});
