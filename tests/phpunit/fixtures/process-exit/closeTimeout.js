'use strict';

// Drives the close-timeout branch of Writer.closeProducer(): the connection never finishes
// closing, so the writer has to report it and still finish successfully, because every message has
// already been delivered by then.
//
// Two timers are armed on purpose. One stands in for the handle the event hub client leaves behind,
// the other is the writer's own progress timer - the report must name only the first, so the count
// in "open handles" is what proves the writer excludes its own timer.
process.env.MESSAGE_DELIMITER = JSON.stringify('\n');
process.env.CONNECTION_STRING = 'placeholder';
process.env.EVENT_HUB_NAME = 'test';
process.env.BATCH_SIZE = '1';

const Writer = require('../../../../src/NodeJs/lib/Writer.js');
const {flushAndExit} = require('../../../../src/NodeJs/lib/processExit.js');

const writer = new Writer();
writer.producer = {close: () => new Promise(() => {})}; // never finishes closing
writer.startProgressTimer();
const leakedHandle = setInterval(() => {}, 60 * 1000);

writer.closeProducer(100).then(() => {
  clearInterval(leakedHandle);
  writer.stopProgressTimer();
  writer.logFinalState();
  flushAndExit(0);
});
