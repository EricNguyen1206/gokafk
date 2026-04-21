// test/kafkajs/globalTeardown.js
const { execSync } = require('child_process')

module.exports = async function globalTeardown() {
  if (global.__GOKAFK_CONTAINER__) {
    console.log('[teardown] stopping gokafk broker container...')
    try {
      execSync(`docker rm -f ${global.__GOKAFK_CONTAINER__}`, { stdio: 'inherit' })
    } catch (err) {
      console.warn(`[teardown] failed to stop broker container: ${err.message}`)
    }
  }
}
