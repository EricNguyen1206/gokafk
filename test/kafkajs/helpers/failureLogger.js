// test/kafkajs/helpers/failureLogger.js
const fs = require('fs')
const path = require('path')

const OUT = path.join(__dirname, '..', 'results', 'failures.json')

function logFailure({ testCase, phase, error }) {
  let records = []
  if (fs.existsSync(OUT)) {
    try { records = JSON.parse(fs.readFileSync(OUT, 'utf8')) } catch (_) {}
  }
  records.push({
    timestamp: new Date().toISOString(),
    testCase,
    phase,
    error: {
      message: error?.message || String(error),
      type: error?.constructor?.name,
      stack: error?.stack,
    },
  })
  fs.writeFileSync(OUT, JSON.stringify(records, null, 2))
}

module.exports = { logFailure }
