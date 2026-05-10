// test/kafkajs/producer.test.js
const { createKafkaClient } = require('./kafka')

const TOPIC = 'pizza-status'

describe('Producer — gokafk Compatibility', () => {
  let kafka
  let producer

  beforeAll(() => {
    kafka = createKafkaClient('producer-test')
  })

  afterAll(async () => {
    try { await producer?.disconnect() } catch (_) {}
  })

  it('TC-01: kafka client can be instantiated', () => {
    expect(kafka).toBeDefined()
    expect(typeof kafka.producer).toBe('function')
    expect(typeof kafka.consumer).toBe('function')
  })

  it('TC-02: producer.connect() succeeds', async () => {
    producer = kafka.producer()
    await producer.connect()
  })

  it('TC-03: producer.send() delivers one message', async () => {
    const result = await producer.send({
      topic: TOPIC,
      messages: [{ key: 'status', value: 'Pizza is in the oven' }],
    })
    expect(result).toBeDefined()
    expect(result[0].errorCode).toBe(0)
  })
})
