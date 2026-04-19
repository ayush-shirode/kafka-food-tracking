const { kafka } = require("../kafka/kafka")

const consumer = kafka.consumer({ groupId: "dlq-group" });

async function run() {
    await consumer.connect();
    await consumer.subscribe({ topic: "dead-letter-order-status" });

    await consumer.run({
        eachMessage: async ({ message }) => {
            console.log("DLQ message:", message.value.toString());
        }
    });    
}

run();