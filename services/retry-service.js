const { kafka } = require("../kafka/kafka");

const consumer = kafka.consumer({ groupId: "retry-group" });
const producer = kafka.producer();

async function run() {
    await consumer.connect();
    await producer.connect();

    await consumer.subscribe({ topic : "retry-order-status"});

    await consumer.run({
        eachMessage: async ({message}) => {
            const raw = message.value.toString();

            try {
                const data = JSON.parse(raw);

                console.log("Retrying Data: ", data);

                // simulate retry success

                if (!data.status) {
                    throw new Error("Retry failed: Missing status");
                }
                                
                await producer.send({
                    topic: "delivery-updates",
                    messages: [
                        {
                            key: data.orderId,
                            value: JSON.stringify({
                                ...data,
                                status: "OUT_FOR_DELIVERY",
                            }),
                        },
                    ],
                });
            }
            catch (err) {
                console.log("Retry Failed -> DLQ ");

                await producer.send({
                    topic: "dead-letter-order-status",
                    messages: [
                        {
                            value: raw,
                        }
                    ]
                })
            }
        }
    })
}

run();