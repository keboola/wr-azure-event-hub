'use strict';

const fs = require('fs');

// We are using separated file descriptor to read JSON documents.
// Number of the custom file descriptor is loaded from ENV (set by PHP), ... fallback is STDIN.
let messageStreamFd;
if (process.env.MESSAGE_STREAM_FD !== undefined) {
  messageStreamFd = parseInt(process.env.MESSAGE_STREAM_FD, 10);
} else {
  console.error('Please, set env variable "MESSAGE_STREAM_FD". Using STDIN as fallback.');
  messageStreamFd = process.stdin.fd;
}
module.exports = fs.createReadStream(null, { fd: messageStreamFd });
