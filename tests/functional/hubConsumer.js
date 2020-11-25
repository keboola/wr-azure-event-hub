const { EventHubConsumerClient, latestEventPosition } = require("@azure/event-hubs");

const connectionString = process.env["CONNECTION_STRING"];
const eventHubName = process.env["EVENT_HUB_NAME"];
const consumerGroup = process.env["CONSUMER_GROUP_NAME"];

let consumerClient;
let subscription;

async function main() {
    let messageCount = 0;
    consumerClient = new EventHubConsumerClient(consumerGroup, connectionString, eventHubName);
    subscription = consumerClient.subscribe(
        {
            processInitialize: async(context) => {
                console.error(`Subscribed partition=${context.partitionId}.`)
            },
            processEvents: async (events, context) => {
                for (const event of events) {
                    const number = (messageCount+1).toString().padStart(3, '0');
                    console.log(`Message ${number}, content: ${JSON.stringify(event.body)}`)
                }
                messageCount++;
            },
            processError: async (err, context) => {
                console.error(`Error on partition "${context.partitionId}": ${err}`);
                process.exit(1);
            }
        },
        { startPosition: latestEventPosition }
    );

    // Log to STDERR, so STDOUT contains only messages.
    console.error(`Listening for the messages from the event hub "${eventHubName}"/"${consumerGroup}".`);

    // Graceful exit
    process.on('SIGINT', () => onExit());
    process.on('SIGTERM', () => onExit());
}

async function onExit() {
    await subscription.close();
    await consumerClient.close();
    console.error(`Disconnected.`);
    process.exit(0);
}

main().catch((error) => {
    console.error("Error:", error);
});
