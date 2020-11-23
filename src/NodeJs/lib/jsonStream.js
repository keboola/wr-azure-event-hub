'use strict';

const fs = require('fs');

// We are using separated file descriptor to read JSON documents.
// Number of the custom file descriptor is loaded from ENV (set by PHP), ... fallback is STDIN.
let jsonStreamFd;
if (process.env.JSON_STREAM_FD !== undefined) {
  jsonStreamFd = parseInt(process.env.JSON_STREAM_FD, 10);
} else {
  console.error('Please, set env variable "JSON_STREAM_FD". Using STDIN as fallback.');
  jsonStreamFd = process.stdin.fd;
}
module.exports = fs.createReadStream(null, { fd: jsonStreamFd });
