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
    this.batchSize = parseInt(process.env.BATCH_SIZE, 10);

    // Active batches by partition key - only keep current batches in memory
    this.activeBatches = new Map();
    this.batchSendPromises = new Map();

    // Stats
    this.messagesQueuedCount = 0;
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
    
    let message = null;
    let shouldLoadNext = true;
    
    while (true) {
      // Load next message or retry with current message
      if (shouldLoadNext) {
        message = (await messages.next()).value;
        if (!message) {
          // No more messages, send all remaining batches
          await this.sendAllActiveBatches();
          break;
        }
      }

      const eventData = this.messageToEventData(message);
      const partitionKey = eventData.partitionKey || null;

      // Get or create batch for this partition key
      let batch = await this.getOrCreateBatch(partitionKey);
      const isAdded = batch.tryAdd(eventData);

      // Message was not added + batch is empty => message is too large
      if (!isAdded && batch.count === 0) {
        throw new UserError(`Message number="${this.messagesQueuedCount + 1}" is too large.`);
      }

      if (isAdded) {
        // Message added successfully
        this.messagesQueuedCount += 1;
        shouldLoadNext = true;
      } else {
        // Message was not added => batch was full => try again with new batch
        shouldLoadNext = false;
      }

      // Send batch if:
      // 1. Message was added and batch is full (by count), OR
      // 2. Message was not added and batch is not empty (full by size)
      if (
        (isAdded && batch.count >= this.batchSize)
        || (!isAdded && batch.count > 0)
      ) {
        await this.sendBatchAndCreateNew(partitionKey);
      }
    }
  }

  async getOrCreateBatch(partitionKey) {
    if (!this.activeBatches.has(partitionKey)) {
      await this.createBatchForPartition(partitionKey);
    }
    return this.activeBatches.get(partitionKey);
  }

  async createBatchForPartition(partitionKey) {
    const batchOptions = {};
    if (partitionKey !== null) {
      batchOptions.partitionKey = partitionKey;
    }

    const batch = await this.producer.createBatch(batchOptions);
    this.activeBatches.set(partitionKey, batch);
    return batch;
  }

  async sendBatchAndCreateNew(partitionKey) {
    await this.sendBatch(partitionKey, true); // Wait for the batch to be sent
    await this.createBatchForPartition(partitionKey);
  }

  async sendAllActiveBatches() {
    const sendPromises = [];
    for (const partitionKey of this.activeBatches.keys()) {
      sendPromises.push(this.sendBatch(partitionKey, true));
    }
    await Promise.all(sendPromises);
  }

  async sendBatch(partitionKey, wait = false) {
    // Wait for any previous send operation for this partition
    const previousPromise = this.batchSendPromises.get(partitionKey);
    if (previousPromise) {
      await previousPromise;
    }

    // Start sending current batch
    const sendPromise = this.doSendBatch(partitionKey);
    this.batchSendPromises.set(partitionKey, sendPromise);

    if (wait) {
      await sendPromise;
    }
  }

  async doSendBatch(partitionKey) {
    const batch = this.activeBatches.get(partitionKey);

    if (!batch || batch.count === 0) {
      return; // No batch or empty batch
    }

    const batchMessageCount = batch.count;

    await this.producer.sendBatch(batch);
    this.batchesSentCount += 1;
    this.messagesSentCount += batchMessageCount;

    // Remove the batch from active batches
    this.activeBatches.delete(partitionKey);
    this.batchSendPromises.delete(partitionKey);
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
      if (Object.keys(message.properties).length > 0) {
        eventData.properties = message.properties;
      }
    }
    if (message.partitionKey) {
      eventData.partitionKey = message.partitionKey;
      delete message.partitionKey;
    }
    eventData.contentType = 'application/json'

    return eventData;
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
