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
    
    // Fetch available partitions for direct routing
    const partitionIds = await this.producer.getPartitionIds();

    let loadNext = true;
    let message = null;
    let partitionBatches = {};
    
    // Initialize a batch for each partition
    for (const partitionId of partitionIds) {
      partitionBatches[partitionId] = await this.producer.createBatch({ partitionId });
    }

    while (true) {
      // If the message was not added in the previous step (loadNext = false) => try again.
      message = loadNext ? (await messages.next()).value : message;
      if (!message) {
        // No message left => all done => send all batches and wait for them
        for (const partitionId of partitionIds) {
          if (partitionBatches[partitionId].count > 0) {
            await this.producer.sendBatch(partitionBatches[partitionId]);
            this.batchesSentCount += 1;
            this.messagesSentCount += partitionBatches[partitionId].count;
          }
        }
        break;
      }

      // Determine which partition to use
      let targetPartitionId;
      if (message.partitionKey) {
        // Try to use the partition key directly as a partition ID
        const partitionKey = message.partitionKey.toString();
        
        if (partitionIds.includes(partitionKey)) {
          // The partition key is a valid partition ID, use it directly
          targetPartitionId = partitionKey;
        } else {
          // The partition key is not a valid partition ID, use it to determine a partition
          // We'll use the modulo operation for simple, deterministic mapping
          const partitionIndex = Math.abs(parseInt(partitionKey, 10) || 0) % partitionIds.length;
          targetPartitionId = partitionIds[partitionIndex];
        }
      } else {
        // Round-robin distribution if no partition key
        const nextPartitionIndex = this.messagesQueuedCount % partitionIds.length;
        targetPartitionId = partitionIds[nextPartitionIndex];
      }

      // Prepare event data (without partitionKey which isn't needed anymore)
      const eventData = this.messageToEventData(message);
      
      // Try to add to the target partition's batch
      const targetBatch = partitionBatches[targetPartitionId];
      const isAdded = targetBatch.tryAdd(eventData);

      if (!isAdded) {
        // Batch is full, send it and create a new one
        await this.producer.sendBatch(targetBatch);
        this.batchesSentCount += 1;
        this.messagesSentCount += targetBatch.count;
        
        // Create a new batch for this partition
        partitionBatches[targetPartitionId] = await this.producer.createBatch({ partitionId: targetPartitionId });
        
        // Try again with the new batch
        const newBatch = partitionBatches[targetPartitionId];
        const addedToNewBatch = newBatch.tryAdd(eventData);
        
        if (!addedToNewBatch) {
          throw new UserError(`Message number="${this.messagesQueuedCount + 1}" is too large for partition ${targetPartitionId}.`);
        }
      }
      
      // Message was successfully added
      this.messagesQueuedCount += 1;
      loadNext = true;
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
    eventData.contentType = 'application/json'
    
    // We don't need to set partitionKey here anymore as we're using direct partition routing
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
