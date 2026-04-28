// test/kafkajs/kafka.js
const { Kafka, logLevel } = require('kafkajs')

const BROKER = process.env.KAFKA_BROKER || 'localhost:10000'

function createKafkaClient(clientId = 'gokafk-test') {
  return new Kafka({
    clientId,
    brokers: [BROKER],
    logLevel: logLevel.WARN,
    retry: {
      initialRetryTime: 100,
      retries: 3,
    },
    connectionTimeout: 5000,
    requestTimeout: 10000,
  })
}

module.exports = { createKafkaClient }
