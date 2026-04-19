const { kafka } = require("../kafka/kafka");

const consumer = kafka.consumer({ groupId: "restaurant-group-v2" });

const producer = kafka.producer();

async function run() {
    await consumer.connect();
    await producer.connect();

    await consumer.subscribe({ topic: "orders", fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ message }) => {
            const data = JSON.parse(message.value.toString());

            console.log("Restaurant received order:", data);

            // simulate decision
            const accepted = Math.random() > 0.2;

            const statusUpdate = {
                orderId: data.orderId,
                status: accepted ? "ACCEPTED" : "REJECTED",
            };

            await producer.send({
                topic: "order-status",
                messages: [
                    {
                        key: data.orderId,
                        value: JSON.stringify(statusUpdate),
                    },
                ],
            });

            console.log("Restaurant sent status:", statusUpdate);
        },
    });
}

run();