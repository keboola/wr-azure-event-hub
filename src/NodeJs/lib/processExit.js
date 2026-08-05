'use strict';

// Writes to a pipe are asynchronous on POSIX and process.exit() does not wait for them, so the
// output is flushed before exiting. If a flush never completes, the exit is committed anyway after
// this timeout.
const FLUSH_TIMEOUT_MS = 2 * 1000;

/**
 * Describe the handles that are currently keeping the event loop alive, as a "name xN" summary.
 *
 * Every resource is reported. An unreferenced timer cannot hold the loop open by itself, but a
 * leaked interval - the shape of the handle the event hub client can leave behind - is reported
 * as "Timeout", so filtering timers out would hide the most likely cause.
 */
function describeActiveResources() {
  if (typeof process.getActiveResourcesInfo !== 'function') {
    return 'unknown';
  }

  const counts = new Map();
  process.getActiveResourcesInfo().forEach((name) => {
    counts.set(name, (counts.get(name) || 0) + 1);
  });

  if (counts.size === 0) {
    return 'nothing';
  }

  return Array.from(counts.entries())
    .map(([name, count]) => (count > 1 ? `${name} x${count}` : name))
    .join(', ');
}

/**
 * Exit with the given code, once stdout and stderr have been flushed.
 *
 * The process must not be left to exit by itself when the work is done: the event hub client can
 * keep a handle open after close(), and the parent process waits for this process to exit, so a
 * leaked handle stalls the whole job until the platform timeout kills it - even though all
 * messages have already been delivered.
 */
function flushAndExit(code) {
  let pending = 2;
  let exited = false;

  const exit = () => {
    if (!exited) {
      exited = true;
      process.exit(code);
    }
  };

  const flushed = () => {
    pending -= 1;
    if (pending === 0) {
      exit();
    }
  };

  // An empty chunk is flushed after everything already queued on the stream, so its callback tells
  // us the previous output has been written out.
  process.stdout.write('', flushed);
  process.stderr.write('', flushed);

  // Unreferenced, so it can never delay an exit that is otherwise ready.
  setTimeout(exit, FLUSH_TIMEOUT_MS).unref();
}

module.exports = {describeActiveResources, flushAndExit};
