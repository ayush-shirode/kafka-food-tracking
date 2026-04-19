const { kafka } = require("./kafka");

const producer = kafka.producer();

// async function sendOrder() {
//     console.log("Connecting Producer....");
//     await producer.connect();
//     console.log("Producer Connected");

//     await producer.send({
//         topic: "orders",
//         messages: [{
//             key: "order-1",
//             value: JSON.stringify({
//                 orderId: "1",
//                 status: "Placed",
//             })
//         }]
//     })

//     console.log("Order sent");

//     await producer.disconnect();
// }

// sendOrder();

async function init() {
    await producer.connect();
}
init();