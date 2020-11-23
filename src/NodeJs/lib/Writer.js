'use strict';

const { EventHubProducerClient } = require('@azure/event-hubs');
const ApplicationError = require('./ApplicationError.js');

class Writer {
  constructor() {
    // Check environment variables
    ['JSON_DELIMITER', 'CONNECTION_STRING', 'EVENT_HUB_NAME'].forEach((key) => {
      if (!process.env[key]) {
        throw new ApplicationError(`Missing "${key}" environment variable.`);
      }
    });

    this.delimiter = JSON.parse(process.env.JSON_DELIMITER);
    this.connectionString = process.env.CONNECTION_STRING;
    this.eventHubName = process.env.EVENT_HUB_NAME;
    this.producer = this.createProducerClient();
  }

  async testConnection() {
    await this.producer.getEventHubProperties();
    await this.producer.close();
  }

  async write() {
    this.todo = 'todo';
  }

  createProducerClient() {
    return new EventHubProducerClient(this.connectionString, this.eventHubName);
  }
}

module.exports = Writer;
