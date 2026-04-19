const { kafka } = require("./kafka");

const consumer = kafka.consumer({groupId: 'restuarent-group'});

async function run() {
    await consumer.connect();

    await consumer.subscribe({topic: 'orders', fromBeginning: true});

    await consumer.run({
        eachMessage: async ({ message }) => {
            const data = JSON.parse(message.value.toString());
            console.log("Received:", data);
        }
    });
};

async function getStatusUpdate() {
    await consumer.connect();

    await consumer.subscribe({topic: 'order-status', fromBeginning: false});

    await consumer.run({
        eachMessage: async ({message}) => {
            const raw = message.value.toString();
            try {
                const data = JSON.parse(raw);
                console.log("Parsed:", data);
            } catch (err) {
                console.log("Skipping invalid JSON:", raw);
            }

        }
    })
}

// run();
getStatusUpdate();