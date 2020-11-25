'use strict';

const Writer = require('./lib/Writer.js');
const UserError = require('./lib/UserError.js');
const ApplicationError = require('./lib/ApplicationError.js');

async function main() {
  const writer = new Writer();
  await writer.write();
}

main().catch((error) => {
  // User error
  if (error instanceof UserError) {
    console.error(error.message);
    process.exit(1);
  }

  // Application error
  console.error(error instanceof ApplicationError ? error.message : error);
  process.exit(2);
});
