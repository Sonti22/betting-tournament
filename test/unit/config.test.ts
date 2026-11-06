import assert from 'node:assert/strict';
import { test } from 'node:test';
import { loadConfig } from '../../src/config';

test('configuration supplies documented defaults and exact FX values', () => {
  const config = loadConfig({});
  assert.equal(config.port, 3000);
  assert.equal(config.fxRates.USD, '1');
  assert.ok(config.prefetch > 0);
  assert.ok(config.maxRetries >= 0);
  assert.ok(config.retryDelayMs > 0);
});

test('configuration reads environment and validates the USD basis', () => {
  const config = loadConfig({
    PORT: '3100', PREFETCH: '4', MAX_RETRIES: '2', RETRY_DELAY_MS: '150',
    QUEUE_PREFIX: 'test.integration', FX_RATES: '{"USD":"1","EUR":"1.12345678"}',
  });
  assert.equal(config.port, 3100);
  assert.equal(config.prefetch, 4);
  assert.equal(config.maxRetries, 2);
  assert.equal(config.retryDelayMs, 150);
  assert.equal(config.queuePrefix, 'test.integration');
  assert.equal(config.fxRates.EUR, '1.12345678');
});

for (const [name, env] of Object.entries({
  'invalid JSON rates': { FX_RATES: '{oops' },
  'missing USD': { FX_RATES: '{"EUR":"1.1"}' },
  'non-unit USD': { FX_RATES: '{"USD":"2"}' },
  'zero rate': { FX_RATES: '{"USD":"1","EUR":"0"}' },
  'negative rate': { FX_RATES: '{"USD":"1","EUR":"-1"}' },
  'excess rate precision': { FX_RATES: '{"USD":"1","EUR":"1.000000001"}' },
  'rate overflow': { FX_RATES: '{"USD":"1","EUR":"1000000"}' },
  'invalid port': { PORT: '0' },
  'invalid prefetch': { PREFETCH: '0' },
  'negative retries': { MAX_RETRIES: '-1' },
  'invalid retry interval': { RETRY_DELAY_MS: '0' },
})) {
  test(`configuration rejects ${name}`, () => {
    assert.throws(() => loadConfig(env));
  });
}
