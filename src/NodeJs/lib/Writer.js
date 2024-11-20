'use strict';

const {EventHubProducerClient} = require('@azure/event-hubs');
const {MessagingError} = require('@azure/core-amqp');
const {Transform} = require('stream');
const binarySplit = require('binary-split');
const UserError = require('./UserError.js');
const ApplicationError = require('./ApplicationError.js');
const messageStream = require('./messageStream');

const PROGRESS_OUTPUT_INTERVAL_MS = 30 * 1000; // log progress each 30 seconds

class Writer {
  constructor() {
    // Check environment variables
    ['MESSAGE_DELIMITER', 'CONNECTION_STRING', 'EVENT_HUB_NAME'].forEach((key) => {
      if (!process.env[key]) {
        throw new ApplicationError(`Missing "${key}" environment variable.`);
      }
    });

    this.delimiter = JSON.parse(process.env.MESSAGE_DELIMITER);
    this.connectionString = process.env.CONNECTION_STRING;
    this.eventHubName = process.env.EVENT_HUB_NAME;
    this.progressTimer = null;
    this.producer = null;
    this.batch = null;
    this.batchSendPromise = null;
    this.batchSize = parseInt(process.env.BATCH_SIZE, 10);

    // Stats
    this.messagesQueuedCount = 0;
    this.messagesSendingCount = 0;
    this.messagesSentCount = 0;
    this.batchesSentCount = 0;
  }

  async testConnection() {
    this.producer = await this.createProducerClient();
    await this.producer.close();
  }

  async write() {
    this.startProgressTimer();
    this.producer = await this.createProducerClient();
    await this.writeMessages();
    await this.producer.close();
    this.stopProgressTimer();
    this.logFinalState();
  }

  async writeMessages() {
    const messages = this.getMessages();
    await this.sendBatchAndCreateNew();

    let loadNext = true;
    let message = null;
    while (true) {
      // If the message was not added in the previous step (loadNext = false) => try again.
      message = loadNext ? (await messages.next()).value : message;
      if (!message) {
        // No message left => all done => send batch and wait for it
        await this.sendBatch(true);
        break;
      }

      // Try to add
      const eventData = this.messageToEventData(message);

      const isAdded = this.batch.tryAdd(eventData);

      // Message was not added + batch is empty => message is too large
      if (!isAdded && this.batch.count === 0) {
        throw new UserError(`Message number="${this.messagesQueuedCount + 1}" is too large.`);
      }

      if (isAdded) {
        // Message was added to the batch => continue => load next
        this.messagesQueuedCount += 1;
        loadNext = true;
      } else {
        // Message was not added => batch was full => try again with the new batch
        loadNext = false;
      }

      // Crate the new batch, if:
      // 1. Batch is full (in terms of items count) OR
      // 2. Message was not added + batch is not empty => batch is full (in terms of size)
      if (
        (isAdded && this.batch.count === this.batchSize)
        || (!isAdded && this.batch.count > 0)
      ) {
        await this.sendBatchAndCreateNew();
      }
    }
  }

  messageToEventData(message) {
    let eventData = {
      body: message.message,
    }
    if (message.properties) {
      if (message.properties.correlationId) {
        eventData.correlationId = message.properties.correlationId;
        delete message.properties.correlationId;
      }
      if (message.properties.messageId) {
        eventData.messageId = message.properties.messageId;
        delete message.properties.messageId;
      }
      if (message.properties) {
        eventData.properties = message.properties;
      }
    }
    return eventData;
  }

  async sendBatchAndCreateNew() {
    // wait = false => non-blocking
    // - Only one batch is sent at a time => We are waiting only for the previous batch.
    // - But we are not waiting for current batch, we are already preparing another one during sending.
    await this.sendBatch(false);
    this.batch = await this.producer.createBatch();
  }

  async sendBatch(wait = false) {
    await this.batchSendPromise; // Wait for the previous batch to be sent
    this.batchSendPromise = this.doSendBatch(); // Send current batch
    if (wait) {
      await this.batchSendPromise;
    }
  }

  async doSendBatch() {
    this.messagesSendingCount = this.messagesQueuedCount;
    this.messagesQueuedCount = 0;

    if (this.batch === null || this.batch.count === 0) {
      // Empty batch => ignore
      return;
    }

    await this.producer.sendBatch(this.batch);
    this.batchesSentCount += 1;
    this.messagesSentCount += this.messagesSendingCount;
    this.messagesSendingCount = 0;
  }

  async* getMessages() {
    const messages = messageStream
      .pipe(binarySplit(this.delimiter))
      .pipe(new Transform({
        objectMode: true,
        transform(json, encoding, callback) {
          callback(null, JSON.parse(json));
        },
      }));

    /* eslint no-restricted-syntax: "off" */
    for await (const message of messages) {
      yield message;
    }
  }

  async createProducerClient() {
    try {
      console.log(`Connecting to the event hub "${this.eventHubName}" ...`);
      const producer = new EventHubProducerClient(this.connectionString, this.eventHubName);
      const properties = await producer.getEventHubProperties();
      console.log(`Connected to the event hub "${properties.name}".`);
      return producer;
    } catch (e) {
      switch (true) {
        case e instanceof TypeError && e.message.includes('doesn\'t match with eventHubName:'):
          throw new UserError(
            'Connection error: The entity path in connection string doesn\'t match with the configured event hub name.'
          );

        case e instanceof TypeError && e.message.includes('AccessKey='):
          // Hide access key from the output
          throw new UserError('Connection error. Please, check connection string.');

        case e instanceof TypeError:
          throw new UserError(
            `Connection error: ${e.message.replace(/\\.\\s*$/, '')}. Please, check connection string.`
          );

        case e instanceof MessagingError:
          throw new UserError(e.message);

        default:
          throw e;
      }
    }
  }

  logFinalState() {
    if (this.messagesSentCount) {
      console.log(
        `Done: Sent "${this.messagesSentCount}" messages / "${this.batchesSentCount}" batches `
        + `to the event hub "${this.eventHubName}".`
      );
    } else {
      console.log('Done: No message was sent.');
    }
  }

  logProgress() {
    console.log(`Progress: Sent "${this.messagesSentCount}" messages / "${this.batchesSentCount}" batches ...`);
  }

  startProgressTimer() {
    this.progressTimer = setInterval(() => this.logProgress(), PROGRESS_OUTPUT_INTERVAL_MS);
  }

  stopProgressTimer() {
    clearInterval(this.progressTimer);
  }
}

module.exports = Writer;
