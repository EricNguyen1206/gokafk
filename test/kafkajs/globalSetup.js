// test/kafkajs/globalSetup.js
const { execSync } = require('child_process')
const net = require('net')

const BROKER_HOST = process.env.KAFKA_HOST || 'localhost'
const BROKER_PORT = parseInt(process.env.KAFKA_PORT || '10000', 10)
const IS_DOCKER = !!process.env.KAFKA_BROKER  // set by docker-compose

async function waitForPort(host, port, timeoutMs = 20000) {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    const ok = await new Promise((resolve) => {
      const socket = new net.Socket()
      socket.setTimeout(500)
      socket
        .once('connect', () => { socket.destroy(); resolve(true) })
        .once('error',   () => { socket.destroy(); resolve(false) })
        .once('timeout', () => { socket.destroy(); resolve(false) })
        .connect(port, host)
    })
    if (ok) return
    await new Promise((r) => setTimeout(r, 500))
  }
  throw new Error(`Broker at ${host}:${port} did not become ready within ${timeoutMs}ms`)
}

module.exports = async function globalSetup() {
  if (IS_DOCKER) {
    const [host, port] = process.env.KAFKA_BROKER.split(':')
    await waitForPort(host, parseInt(port, 10))
    console.log(`[setup] broker ready at ${process.env.KAFKA_BROKER}`)
    return
  }

  // Local dev: start broker via docker
  console.log('[setup] starting gokafk broker container...')
  try {
    execSync(
      'docker run -d --name gokafk-test -p 10000:10000 gokafk:dev',
      { stdio: 'inherit' }
    )
    global.__GOKAFK_CONTAINER__ = 'gokafk-test'
    await waitForPort(BROKER_HOST, BROKER_PORT)
    console.log(`[setup] broker ready at ${BROKER_HOST}:${BROKER_PORT}`)
  } catch (err) {
    console.warn(`[setup] failed to start broker container: ${err.message}`)
    console.warn('[setup] assuming broker is already running or being started elsewhere')
  }
}
