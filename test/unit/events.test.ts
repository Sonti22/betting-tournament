import assert from 'node:assert/strict';
import { test } from 'node:test';
import { parseEvent, PermanentEventError } from '../../src/bets/events';
import { calculatePoints } from '../../src/bets/scoring';

const valid = {
  betId: 'bet-1', playerId: 'player-1', currency: 'USD',
  stake: '2.50', winAmount: '10.10', settledAt: '2026-11-06T12:00:00.000Z',
};

test('event normalization preserves exact money and canonicalizes equivalent payloads', () => {
  const event = parseEvent(valid);
  assert.equal(event.stake, '2.5');
  assert.equal(event.winAmount, '10.1');
  assert.deepEqual(parseEvent({ ...valid, stake: 2.5, winAmount: 10.1 }), event);
});

test('fractional points are exact, including the smallest supported product', () => {
  assert.equal(calculatePoints('0.1', '0.2'), '0.02');
  assert.equal(calculatePoints('1.99', '1'), '1.99');
  assert.equal(calculatePoints('0.00000001', '0.00000001'), '0.0000000000000001');
  assert.equal(calculatePoints('999999999999.99999999', '999999.99999999'),
    '999999999999989999.9900000000000001');
  assert.equal(calculatePoints('0', '1.2'), '0');
});

test('valid leap-day timestamps and zero winnings are accepted', () => {
  assert.equal(parseEvent({ ...valid, winAmount: '0', settledAt: '2024-02-29T12:34:56Z' }).winAmount, '0');
});

for (const [name, change] of Object.entries({
  'negative winnings': { winAmount: '-1' },
  'negative stake': { stake: -1 },
  'excess money precision': { winAmount: '0.000000001' },
  'amount overflow': { winAmount: '1000000000000' },
  'unsafe numeric amount': { winAmount: Number.MAX_SAFE_INTEGER + 1 },
  'non-finite amount': { winAmount: Number.POSITIVE_INFINITY },
  'exponent string': { winAmount: '1e3' },
  'empty player': { playerId: '' },
  'oversize identifier': { betId: 'a'.repeat(129) },
  'malformed identifier': { playerId: "player'; DROP TABLE bets;--" },
  'lowercase currency': { currency: 'usd' },
  'impossible date': { settledAt: '2026-02-30T12:00:00Z' },
  'timestamp without zone': { settledAt: '2026-11-06T12:00:00' },
  'unexpected property': { extra: 'unrecognized' },
  'missing required amount': { winAmount: undefined },
})) {
  test(`rejects ${name} as a permanent event error`, () => {
    assert.throws(() => parseEvent({ ...valid, ...change }), PermanentEventError);
  });
}

test('rejects null, arrays and non-object payloads', () => {
  for (const value of [null, [], 'event', 42]) {
    assert.throws(() => parseEvent(value), PermanentEventError);
  }
});
