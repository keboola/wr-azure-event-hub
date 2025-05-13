# Azure Event Hub Writer

[![Build Status](https://travis-ci.com/keboola/wr-azure-event-hub.svg?branch=master)](https://travis-ci.com/keboola/wr-azure-event-hub)

[Azure Event Hub](https://azure.microsoft.com/en-us/services/event-hubs/) writer for the [Keboola Connection](https://www.keboola.com).

## Configuration

The configuration `config.json` contains following properties in `parameters` key: 
- `hub` - object (required): Configuration of the connection.
    - `#connectionString` - string (required): [Event Hubs connection string](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-get-connection-string) eg. `Endpoint=sb://....`.
    - `eventHubName` - string (required): [Event Hubs name](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-create#create-an-event-hub).
- `tableId` - string (optional):
  - Name of the table from the input mapping (source).
  - If the input mapping contains only one table, it is used by default.
- `batchSize` int (optional):
  - Default `1000`.
  - Number of messages to be sent at once.
  - If the messages exceed the maximum batch size, a smaller number can be sent.
- `mode` - enum (optional): Specifies how the CSV row is mapped to the message.
    - `row_as_json` (default) - Message is row in the JSON format, eg. `{"id": 1, "name": "John"}`.
    - `column_value` - Message is value of the defined `column`, eg. `John`.
- `column` - string (optional): Name of the column for `mode` = `column_value`.
- `propertiesColumns` - string (optional): Name of the column with properties (correlationId, messageId, ...) in JSON format for `mode` = `column_value`.
- `partitionKeyColumn` - string (optional): Name of the column containing the partition key values. If not specified, Azure Event Hub will handle partition assignment automatically.


## Development
 
Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/wr-azure-event-hub
cd wr-azure-event-hub
docker-compose build
docker-compose run --rm dev composer install --no-scripts
docker-compose run --rm dev yarn install
```

Create `.env` file with following variables:
```env
CONNECTION_STRING=
EVENT_HUB_NAME=
CONSUMER_GROUP_NAME="$Default"
```


Run the test suite using this command:

```
docker-compose run --rm dev composer tests
```
 
### Event Hub Consumer

To read back messages sent to the Event Hub, run:
```
nodejs ./tests/functional/hubConsumer.js 
```

This script prints to STDOUT all new messages from the launch time. 

It is useful for development and debugging.
 
# Integration

For information about deployment and integration with KBC, please refer to the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/) 
