'use strict';

// Reproduces the failure that flushAndExit() guards against: all the work is done and the output
// has been written, but a handle is still keeping the event loop alive. Without a deterministic
// exit this process would run until something kills it.
const {flushAndExit} = require('../../../../src/NodeJs/lib/processExit.js');

console.log('stdout1');
console.log('stdout2');
console.error('stderr1');
console.log('stdout3');
console.log('stdout4');

// A referenced timer, the shape of the handle the event hub client can leave behind after close().
setInterval(() => {}, 60 * 1000);

flushAndExit(0);
