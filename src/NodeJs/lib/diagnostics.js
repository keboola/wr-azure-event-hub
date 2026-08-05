'use strict';

// Namespaces limited to connection/link lifecycle.
// Raw frame and SASL namespaces are intentionally omitted, they contain credentials.
const DEBUG_NAMESPACES = [
  'rhea:events',
  'rhea:flow',
  'azure:core-amqp:info',
  'azure:core-amqp:error',
  'azure:event-hubs:info',
  'azure:event-hubs:error',
].join(',');

const SECRET_PATTERNS = [
  [/SharedAccessKey=[^;\s"']+/gi, 'SharedAccessKey=***'],
  [/SharedAccessSignature\s+\S+/gi, 'SharedAccessSignature ***'],
  [/\b(sig|skn|se|token|access_token|password|secret)=[^&;\s"']+/gi, '$1=***'],
  [/Bearer\s+[\w\-.~+/]+=*/gi, 'Bearer ***'],
];

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Removes credentials from a diagnostic message.
 * The connection string is redacted as a whole, patterns cover values derived from it.
 */
function redact(value) {
  let output = typeof value === 'string' ? value : require('util').inspect(value);

  const connectionString = process.env.CONNECTION_STRING;
  if (connectionString) {
    output = output.replace(new RegExp(escapeRegExp(connectionString), 'g'), '***');
  }

  SECRET_PATTERNS.forEach(([pattern, replacement]) => {
    output = output.replace(pattern, replacement);
  });

  return output;
}

// Stdio streams are always present and do not block the exit
const IGNORED_RESOURCES = ['PipeWrap', 'TTYWrap', 'FileHandle'];

const OPEN_RESOURCES_REPORT_INTERVAL_MS = 60 * 1000;

/**
 * Reports resources that keep the process alive after the work is done.
 * The timer is unreferenced, so it never delays the exit and fires only if the process is stuck.
 */
function reportOpenResourcesIfStuck() {
  if (typeof process.getActiveResourcesInfo !== 'function') {
    return;
  }

  const startedAt = Date.now();
  const timer = setInterval(() => {
    const resources = process.getActiveResourcesInfo().filter((name) => !IGNORED_RESOURCES.includes(name));
    const seconds = Math.round((Date.now() - startedAt) / 1000);
    console.log(
      `Warning: all messages were sent, but the process has not finished after ${seconds}s. `
      + `It is kept alive by: ${resources.length ? resources.join(', ') : 'unknown resource'}.`
    );
  }, OPEN_RESOURCES_REPORT_INTERVAL_MS);
  timer.unref();
}

/**
 * Opt-in Azure SDK diagnostics, enabled by the AZURE_DIAGNOSTICS env variable.
 * All output goes through redaction, so it is safe to enable in production.
 */
function enableAzureDiagnostics() {
  if (!process.env.AZURE_DIAGNOSTICS) {
    return;
  }

  try {
    /* eslint global-require: "off" */
    const debug = require('debug');
    debug.enable(DEBUG_NAMESPACES);
    debug.log = (...args) => console.log(redact(args.map(String).join(' ')));
  } catch (e) {
    console.error(`Cannot enable AMQP diagnostics: ${e.message}`);
  }

  try {
    const {AzureLogger, setLogLevel} = require('@azure/logger');
    setLogLevel('info');
    AzureLogger.log = (...args) => console.log(redact(args.map(String).join(' ')));
  } catch (e) {
    console.error(`Cannot enable Azure SDK diagnostics: ${e.message}`);
  }

  console.log('Azure diagnostics are enabled, secrets are redacted from the output.');
}

module.exports = {redact, reportOpenResourcesIfStuck, enableAzureDiagnostics};
