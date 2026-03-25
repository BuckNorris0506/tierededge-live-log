#!/usr/bin/env node
import { dispatchOperatorCommand } from './operator-dispatcher.mjs';

function main() {
  const command = process.argv.slice(2).join(' ');
  const result = dispatchOperatorCommand(command);
  if (!result.ok && result.response_type === 'unsupported_command') {
    console.error(result.text);
    process.exit(1);
  }
  console.log(result.text);
}

main();
