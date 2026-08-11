'use strict';

const Writer = require('./lib/Writer.js');
const UserError = require('./lib/UserError.js');
const ApplicationError = require('./lib/ApplicationError.js');
const {flushAndExit} = require('./lib/processExit.js');

async function main() {
  const writer = new Writer();
  await writer.write();
}

main().then(() => {
  // The process is not left to exit on its own: the event hub client can keep a handle open after
  // the connection is closed, and the parent process waits for this process to exit.
  flushAndExit(0);
}).catch((error) => {
  // User error
  if (error instanceof UserError) {
    console.error(error.message);
    process.exit(1);
  }

  // Application error
  console.error(error instanceof ApplicationError ? error.message : error);
  process.exit(2);
});
