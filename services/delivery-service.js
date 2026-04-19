const { kafka } = require("../kafka/kafka")

const consumer = kafka.consumer({ groupId: "delivery-group-v2" });
const producer = kafka.producer();

async function run() {
    await consumer.connect();
    await producer.connect();

    await consumer.subscribe({topic : "order-status", fromBeginning: true});

    await consumer.run({
        eachMessage: async ({message}) => {
            try {
                const data = JSON.parse(message.value.toString());

                console.log("Received Order Status: ", data);

                if (data.status !== "ACCEPTED") return;

                const agentData = {
                    orderId: data.orderId,
                    status: "OUT_FOR_DELIVERY", // fix naming
                    agent: "Agent-1",
                };

                await producer.send({
                    topic: "delivery-updates",
                    messages: [
                        {
                            key: data.orderId,
                            value: JSON.stringify(agentData),
                        }
                    ]
                });

                console.log(`Agent assigned to order ${data.orderId}`);
            } catch (err) {
                console.error("Error processing message:", err);
            }
        }
    })
}

run();