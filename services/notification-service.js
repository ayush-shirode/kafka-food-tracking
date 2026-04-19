const { kafka } = require("../kafka/kafka");

const consumer = kafka.consumer({ groupId: "notification-group-v2" });

async function run() {
    await consumer.connect();

    await consumer.subscribe({ topic: "orders" });
    await consumer.subscribe({ topic: "order-status" });
    await consumer.subscribe({ topic: "delivery-updates" });

    await consumer.run({
        eachMessage: async ({ topic, message }) => {
            const data = JSON.parse(message.value.toString());

            console.log(`[${topic}]`, data);
        },
    });
}

run();