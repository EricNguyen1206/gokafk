// test/kafkajs/jest.config.js
/** @type {import('jest').Config} */
module.exports = {
  testEnvironment: 'node',
  globalSetup: './globalSetup.js',
  globalTeardown: './globalTeardown.js',
  testTimeout: 30000,
  verbose: true,
  reporters: ['default'],
}
