const { spawn, execSync } = require('child_process');
const path = require('path');
const fs = require('fs');
const { createKafkaClient } = require('./kafka');

// Configuration Constants
const BROKER_PORT = 10000;
const STARTUP_TIMEOUT_MS = 30000;
const SHUTDOWN_DELAY_MS = 2000;
const SHORT_TEST_TIMEOUT_MS = 30000;
const LONG_TEST_TIMEOUT_MS = 180000;

// Path Constants
const DATA_DIR = path.join(__dirname, '../../data/logs');
const BINARY_PATH = path.join(__dirname, '../../gokafk_bin');
const ROOT_DIR = path.join(__dirname, '../../');

// Test Domain Constants
const TOPICS = {
  PIZZA: 'pizza-orders',
  PERSISTENCE: 'persistence-orders',
  RECOVERY: 'recovery-orders',
  BALANCED: 'balanced-orders',
  LOYAL: 'loyal-orders',
  TIMETRAVEL: 'time-travel-orders',
};

const GROUPS = {
  ORDER_SERVICE: 'order-service-group',
  PERSISTENCE: 'persistence-group',
  CHEF_TEAM: 'chef-team',
  BALANCED_TEAM: 'balanced-chef-team',
};

const MESSAGES = {
  PEPPERONI: 'Pepperoni Pizza',
  MARGHERITA: 'Margherita Pizza',
  PERSISTENT: 'Persistent Pizza',
  CHEF_ORDER_PREFIX: 'Chef Order #',
};

describe('Gokafk Integration Tests — Pizza Shop Scenarios', () => {
  let brokerProcess;
  let kafka;

  /**
   * Spawns the gokafk broker process and waits for the readiness signal.
   * Supports environment variables via the config.LoadConfig() mechanism.
   */
  const startBroker = (env = {}) => {
    return new Promise((resolve, reject) => {
      console.log('Starting broker with env:', env);
      brokerProcess = spawn(BINARY_PATH, ['server'], { 
        cwd: ROOT_DIR,
        env: { ...process.env, ...env }
      });

      let resolved = false;
      let stderrOutput = '';

      const onData = (data) => {
        const msg = data.toString();
        stderrOutput += msg;
        if (msg.includes('broker started') && !resolved) {
          console.log('Broker is ready');
          resolved = true;
          resolve();
        }
      };

      brokerProcess.stderr.on('data', onData);
      brokerProcess.stdout.on('data', onData);

      brokerProcess.on('error', (err) => {
        console.error('Broker process error:', err);
        if (!resolved) {
          resolved = true;
          reject(err);
        }
      });

      brokerProcess.on('exit', (code) => {
        if (!resolved && code !== 0 && code !== null) {
          resolved = true;
          const errorMsg = `Broker exited with code ${code}. Stderr: ${stderrOutput}`;
          console.error(errorMsg);
          reject(new Error(errorMsg));
        }
      });

      setTimeout(() => {
        if (!resolved) {
          resolved = true;
          reject(new Error(`Broker start timeout after ${STARTUP_TIMEOUT_MS}ms`));
        }
      }, STARTUP_TIMEOUT_MS);
    });
  };

  /**
   * Gracefully shuts down the broker
   */
  const stopBroker = () => {
    return new Promise((resolve) => {
      if (!brokerProcess || brokerProcess.killed) return resolve();
      console.log('Stopping broker...');
      brokerProcess.on('exit', () => {
        console.log('Broker stopped');
        setTimeout(resolve, SHUTDOWN_DELAY_MS);
      });
      brokerProcess.kill('SIGTERM');
    });
  };

  beforeAll(async () => {
    // 1. Build the Go binary
    console.log('Building gokafk binary...');
    execSync(`go build -o ${BINARY_PATH} cmd/gokafk/main.go`, { cwd: ROOT_DIR });

    // 2. Clear old data logs
    if (fs.existsSync(DATA_DIR)) {
      console.log('Clearing data directory:', DATA_DIR);
      fs.rmSync(DATA_DIR, { recursive: true, force: true });
    }

    // 3. Ensure the broker port is clean
    try {
      execSync(`lsof -ti :${BROKER_PORT} | xargs kill -9`);
    } catch (_) {}

    await startBroker();
    kafka = createKafkaClient('integration-test');
  }, LONG_TEST_TIMEOUT_MS);

  afterAll(async () => {
    await stopBroker();
    if (fs.existsSync(BINARY_PATH)) {
      fs.unlinkSync(BINARY_PATH);
    }
  });

  it('Scenario 1: Basic End-to-End Order Flow', async () => {
    const producer = kafka.producer();
    const consumer = kafka.consumer({ groupId: GROUPS.ORDER_SERVICE });

    await producer.connect();
    await producer.send({
      topic: TOPICS.PIZZA,
      messages: [{ key: 'order-1', value: MESSAGES.PEPPERONI }],
    });
    await producer.disconnect();

    await consumer.connect();
    await consumer.subscribe({ topic: TOPICS.PIZZA, fromBeginning: true });

    await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('Scenario 1 Timeout')), 15000);
      consumer.run({
        eachMessage: async ({ message }) => {
          console.log('Scenario 1 received:', message.value.toString());
          clearTimeout(timeout);
          expect(message.value.toString()).toBe(MESSAGES.PEPPERONI);
          resolve();
        },
      });
    });

    await consumer.stop();
    await consumer.disconnect();
  }, SHORT_TEST_TIMEOUT_MS);

  it('Scenario 2: Broker Persistence Recovery', async () => {
    const producer = kafka.producer();
    await producer.connect();
    await producer.send({
      topic: TOPICS.PERSISTENCE,
      messages: [{ key: 'p-1', value: MESSAGES.PERSISTENT }],
    });
    await producer.disconnect();

    await stopBroker();
    await startBroker();

    const consumer = kafka.consumer({ groupId: GROUPS.PERSISTENCE });
    await consumer.connect();
    await consumer.subscribe({ topic: TOPICS.PERSISTENCE, fromBeginning: true });

    await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('Scenario 2 Timeout')), 20000);
      consumer.run({
        eachMessage: async ({ message }) => {
          if (message.value.toString() === MESSAGES.PERSISTENT) {
            console.log('Scenario 2 recovered message:', MESSAGES.PERSISTENT);
            clearTimeout(timeout);
            resolve();
          }
        },
      });
    });

    await consumer.stop();
    await consumer.disconnect();
  }, LONG_TEST_TIMEOUT_MS);

  it('Scenario 3: Chef Recovery (Offset Persistence)', async () => {
    const topic = TOPICS.RECOVERY;
    const group = GROUPS.CHEF_TEAM;
    const orders = [1, 2, 3, 4, 5].map(i => ({ key: `o${i}`, value: `${MESSAGES.CHEF_ORDER_PREFIX}${i}` }));

    const producer = kafka.producer();
    await producer.connect();
    await producer.send({ topic, messages: orders });
    await producer.disconnect();

    let chef1 = kafka.consumer({ groupId: group });
    await chef1.connect();
    await chef1.subscribe({ topic, fromBeginning: true });

    let count = 0;
    await new Promise((resolve) => {
      chef1.run({
        autoCommitInterval: 50,
        eachMessage: async ({ message }) => {
          count++;
          console.log(`Chef 1 processing: ${message.value.toString()}`);
          // Simulated work time
          await new Promise(r => setTimeout(r, 1000));
          if (count === 2) {
            resolve();
          }
        },
      });
    });
    
    await chef1.stop();
    await chef1.disconnect();

    await stopBroker();
    await startBroker();

    let chef2 = kafka.consumer({ groupId: group });
    await chef2.connect();
    await chef2.subscribe({ topic });

    await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('Scenario 3 Timeout')), 40000);
      chef2.run({
        eachMessage: async ({ message }) => {
          const val = message.value.toString();
          console.log(`Chef 2 resumed at: ${val}`);
          expect(val).toBe(`${MESSAGES.CHEF_ORDER_PREFIX}3`);
          clearTimeout(timeout);
          resolve();
        },
      });
    });

    await chef2.stop();
    await chef2.disconnect();
  }, LONG_TEST_TIMEOUT_MS);

  it('Scenario 4: Pizza Load Balancing (Rebalancing)', async () => {
    console.log('Running Scenario 4...');
    // Restart broker with 2 partitions for auto-creation
    await stopBroker();
    await startBroker({ GOKAFK_PARTITIONS: '2' });

    const topic = TOPICS.BALANCED;
    const group = GROUPS.BALANCED_TEAM;

    const chef1 = kafka.consumer({ groupId: group });
    const chef2 = kafka.consumer({ groupId: group });
    await Promise.all([chef1.connect(), chef2.connect()]);
    await Promise.all([
      chef1.subscribe({ topic, fromBeginning: true }),
      chef2.subscribe({ topic, fromBeginning: true }),
    ]);

    let chef1Count = 0;
    let chef2Count = 0;
    chef1.run({ eachMessage: async () => { chef1Count++; } });
    chef2.run({ eachMessage: async () => { chef2Count++; } });

    // Wait for group stabilization and assignment
    await new Promise(r => setTimeout(r, 10000));

    const producer = kafka.producer();
    await producer.connect();
    // 10 pizzas without keys (round-robin distribution across partitions)
    const messages = Array.from({ length: 10 }).map((_, i) => ({ value: `Pizza ${i}` }));
    await producer.send({ topic, messages });
    
    await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error(`Load balance failed. Chef1: ${chef1Count}, Chef2: ${chef2Count}`)), 20000);
      const interval = setInterval(() => {
        if (chef1Count > 0 && chef2Count > 0) {
          clearInterval(interval);
          clearTimeout(timeout);
          resolve();
        }
      }, 1000);
    });

    await Promise.all([chef1.stop(), chef2.stop()]);
    await Promise.all([chef1.disconnect(), chef2.disconnect()]);
  }, LONG_TEST_TIMEOUT_MS);

  it('Scenario 5: Loyal Customers (Key Partitioning)', async () => {
    const topic = TOPICS.LOYAL;
    const producer = kafka.producer();
    await producer.connect();

    // Customer A and B land in different partitions (assuming FNV-32a hashes differently)
    await producer.send({
      topic,
      messages: [
        { key: 'Customer-A', value: 'Pizza 1' },
        { key: 'Customer-B', value: 'Pizza 2' },
        { key: 'Customer-A', value: 'Pizza 3' },
        { key: 'Customer-B', value: 'Pizza 4' },
      ],
    });

    const admin = kafka.admin();
    await admin.connect();
    const offsets = await admin.fetchTopicOffsets(topic);
    
    // With 2 partitions (set in Scenario 4), both should have data
    const hasDataInBoth = offsets.some(o => o.partition === 0 && o.offset !== '0') &&
                          offsets.some(o => o.partition === 1 && o.offset !== '0');
    
    expect(hasDataInBoth).toBe(true);
    await admin.disconnect();
  }, SHORT_TEST_TIMEOUT_MS);

  it('Scenario 6: Historical Orders (Timestamp Search)', async () => {
    const topic = TOPICS.TIMETRAVEL;
    const producer = kafka.producer();
    await producer.connect();

    await producer.send({ topic, messages: [{ value: 'Early Morning Order' }] });
    await new Promise(r => setTimeout(r, 2000));
    const midPoint = Date.now();
    await new Promise(r => setTimeout(r, 2000));
    await producer.send({ topic, messages: [{ value: 'Late Night Order' }] });

    const admin = kafka.admin();
    await admin.connect();
    const offsets = await admin.fetchTopicOffsetsByTimestamp(topic, midPoint);
    
    // We expect offset 1 (the second message)
    console.log('Offsets for timestamp:', offsets);
    expect(offsets[0].offset).toBe('1');
    await admin.disconnect();
  }, SHORT_TEST_TIMEOUT_MS);
});
