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

    // Partitioning support - Map of partition key to message queue
    this.messagesByPartition = new Map();

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
    // Read all messages and group them by partition key
    await this.readAndGroupMessages();

    // Send messages in batches for each partition key
    await this.sendPartitionedBatches();
  }

  async readAndGroupMessages() {
    const messages = this.getMessages();
    let message = null;

    // Read all messages and group by partition key
    while (true) {
      message = (await messages.next()).value;
      if (!message) {
        break; // No more messages, done reading
      }

      const eventData = this.messageToEventData(message);
      // Use null for messages without a partition key to let Azure assign a partition
      const partitionKey = eventData.partitionKey || null;

      // Initialize array for this partition if needed
      if (!this.messagesByPartition.has(partitionKey)) {
        this.messagesByPartition.set(partitionKey, []);
      }

      // Add message to its partition group
      this.messagesByPartition.get(partitionKey).push(eventData);
      this.messagesQueuedCount++;
    }
  }

  async sendPartitionedBatches() {
    // Process each partition's messages
    for (const [partitionKey, messages] of this.messagesByPartition.entries()) {
      await this.sendBatchesForPartition(partitionKey, messages);
    }
  }

  async sendBatchesForPartition(partitionKey, messages) {
    const batchOptions = {};

    // Only set partitionKey option if it's not null
    if (partitionKey !== null) {
      batchOptions.partitionKey = partitionKey;
    }

    // Create first batch for this partition
    let batch = await this.producer.createBatch(batchOptions);
    let messageIndex = 0;

    // Process all messages for this partition
    while (messageIndex < messages.length) {
      const message = messages[messageIndex];
      const isAdded = batch.tryAdd(message);

      if (isAdded) {
        // Message added successfully, move to next message
        messageIndex++;

        // If batch is full by count, send it and create a new one
        if (batch.count === this.batchSize) {
          await this.sendBatch(batch);
          batch = await this.producer.createBatch(batchOptions);
        }
      } else {
        // Couldn't add message to batch

        // If batch is empty, message is too large
        if (batch.count === 0) {
          throw new UserError(`Message number="${this.messagesSentCount + messageIndex + 1}" is too large.`);
        }

        // Batch is full by size, send it and create a new one
        await this.sendBatch(batch);
        batch = await this.producer.createBatch(batchOptions);
      }
    }

    // Send final batch if it has any messages
    if (batch.count > 0) {
      await this.sendBatch(batch);
    }
  }

  async sendBatch(batch) {
    await this.producer.sendBatch(batch);
    this.batchesSentCount++;
    this.messagesSentCount += batch.count;
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
