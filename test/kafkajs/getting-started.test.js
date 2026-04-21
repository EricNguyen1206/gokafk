// test/kafkajs/getting-started.test.js
const { createKafkaClient } = require('./helpers/kafka')
const { logFailure } = require('./helpers/failureLogger')

const TOPIC = 'test-topic'
const GROUP_ID = 'test-group'

describe('KafkaJS Getting Started — gokafk Compatibility', () => {
  let kafka
  let producer
  let consumer

  beforeAll(() => {
    kafka = createKafkaClient('getting-started-test')
  })

  afterAll(async () => {
    try { await producer?.disconnect() } catch (_) {}
    try { await consumer?.disconnect() } catch (_) {}
  })

  // TC-01: Pure JS — always passes
  it('TC-01: kafka client can be instantiated', () => {
    expect(kafka).toBeDefined()
    expect(typeof kafka.producer).toBe('function')
    expect(typeof kafka.consumer).toBe('function')
  })

  // TC-02: triggers ApiVersions + Metadata handshake
  it('TC-02: producer.connect() succeeds', async () => {
    producer = kafka.producer()
    try {
      await producer.connect()
    } catch (err) {
      logFailure({ testCase: 'TC-02', phase: 'producer.connect', error: err })
      throw err
    }
  })

  // TC-03: Produce API
  it('TC-03: producer.send() delivers one message', async () => {
    if (!producer) {
      logFailure({ testCase: 'TC-03', phase: 'precondition', error: new Error('producer not connected') })
      return
    }
    try {
      const result = await producer.send({
        topic: TOPIC,
        messages: [{ key: 'hello', value: 'Hello KafkaJS user!' }],
      })
      expect(result).toBeDefined()
      expect(result[0].errorCode).toBe(0)
    } catch (err) {
      logFailure({ testCase: 'TC-03', phase: 'producer.send', error: err })
      throw err
    }
  })

  // TC-04: JoinGroup + SyncGroup
  it('TC-04: consumer.connect() succeeds', async () => {
    consumer = kafka.consumer({ groupId: GROUP_ID })
    try {
      await consumer.connect()
    } catch (err) {
      logFailure({ testCase: 'TC-04', phase: 'consumer.connect', error: err })
      throw err
    }
  })

  // TC-05: OffsetFetch + ListOffsets
  it('TC-05: consumer.subscribe() to topic', async () => {
    if (!consumer) {
      logFailure({ testCase: 'TC-05', phase: 'precondition', error: new Error('consumer not connected') })
      return
    }
    try {
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true })
    } catch (err) {
      logFailure({ testCase: 'TC-05', phase: 'consumer.subscribe', error: err })
      throw err
    }
  })

  // TC-06: Full end-to-end Fetch
  it('TC-06: consumer receives the produced message end-to-end', async () => {
    if (!consumer) {
      logFailure({ testCase: 'TC-06', phase: 'precondition', error: new Error('consumer not ready') })
      return
    }
    try {
      await new Promise((resolve, reject) => {
        const timeout = setTimeout(
          () => reject(new Error('TC-06 timeout: no message received in 10s')),
          10000
        )
        consumer.run({
          eachMessage: async ({ topic, partition, message }) => {
            clearTimeout(timeout)
            expect(topic).toBe(TOPIC)
            expect(message.value.toString()).toBe('Hello KafkaJS user!')
            resolve()
          },
        }).catch(reject)
      })
    } catch (err) {
      logFailure({ testCase: 'TC-06', phase: 'consumer.run/eachMessage', error: err })
      throw err
    }
  })
})
