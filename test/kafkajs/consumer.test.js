// test/kafkajs/consumer.test.js
const { createKafkaClient } = require('./kafka')

const TOPIC = 'pizza-orders'
const GROUP_ID = 'pizza-oven-group'

describe('Consumer — gokafk Compatibility', () => {
  let kafka
  let producer
  let consumer

  beforeAll(async () => {
    kafka = createKafkaClient('consumer-test')

    // Produce a message first so the consumer has something to consume
    producer = kafka.producer()
    await producer.connect()
    await producer.send({
      topic: TOPIC,
      messages: [{ key: 'order-123', value: 'Pepperoni Pizza' }],
    })
  })

  afterAll(async () => {
    try { await consumer?.disconnect() } catch (_) {}
    try { await producer?.disconnect() } catch (_) {}
  })

  it('TC-04: consumer.connect() succeeds', async () => {
    consumer = kafka.consumer({ groupId: GROUP_ID })
    await consumer.connect()
  })

  it('TC-05: consumer.subscribe() to topic', async () => {
    await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
  })

  it('TC-06: consumer receives the produced message end-to-end', async () => {
    await new Promise((resolve, reject) => {
      const timeout = setTimeout(
        () => reject(new Error('TC-06 timeout: no message received in 10s')),
        10000
      )
      consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          clearTimeout(timeout)
          expect(topic).toBe(TOPIC)
          expect(message.value.toString()).toBe('Pepperoni Pizza')
          resolve()
        },
      }).catch(reject)
    })
    await consumer.stop()
  })
})
