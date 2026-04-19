const { Kafka } = require("kafkajs");

const kafka = new Kafka({
    clientId: 'food-app',
    brokers: ['localhost:9092'],
})

module.exports = { kafka };