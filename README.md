# Azure Event Hub Writer

[![Build Status](https://travis-ci.com/keboola/wr-azure-event-hub.svg?branch=master)](https://travis-ci.com/keboola/wr-azure-event-hub)

[Azure Event Hub](https://azure.microsoft.com/en-us/services/event-hubs/) writer for the [Keboola Connection](https://www.keboola.com).

## Configuration

The configuration `config.json` contains following properties in `parameters` key: 
- `hub` - object (required): Configuration of the connection.
    - `#connectionString` - string (required): [Event Hubs connection string](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-get-connection-string) eg. `Endpoint=sb://....`.
    - `eventHubName` - string (required): [Event Hubs name](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-create#create-an-event-hub).
- `tableId` - string (required): Name of the table from the input mapping (source).
- `mode` - enum (optional): Specifies how the CSV row is mapped to the message.
    - `row_as_json` (default) - Message is row in the JSON format, eg. `{"id": 1, "name": "John"}`.
    - `column_value` - Message is value of the defined `column`, eg. `John`.
- `column` - string (optional): Name of the column for `mode` = `column_value`.


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
