const express = require('express');
const { kafka } = require('../kafka/kafka');

const app = express();
app.use(express.json());

const producer = kafka.producer();

app.post('/order', async (req, res) => {
    const order = req.body;

    await producer.connect();

    await producer.send({
        topic: "orders",
        messages: [
            {
                key: order.orderId,
                value: JSON.stringify(order),
            },
        ]
    });

    res.send("order placed");
});

app.post('/order-status', async (req, res) => {
    const status = req.body;

    await producer.connect();

    await producer.send({
        topic: "order-status",
        messages: [
            {
                key: status.orderId,
                value: JSON.stringify(status),
            },
        ]
    });

    res.send("Status Update Request sent");
});

app.post('/delivery-update', async (req, res) => {
    const status = req.body;

    await producer.connect();

    await producer.send({
        topic: "order-status",
        messages: [
            {
                key: status.orderId,
                value: JSON.stringify(status),
            },
        ]
    });

    res.send("Status Update Request sent");
});


app.listen(3000, () => {console.log("Order service running")});