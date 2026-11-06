import 'reflect-metadata';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { after, before, test } from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import type { INestApplication } from '@nestjs/common';
import amqp, { type ChannelModel, type ConfirmChannel, type GetMessage } from 'amqplib';
import { Pool } from 'pg';
import { createApp } from '../../src/bootstrap';
import { loadConfig, type AppConfig } from '../../src/config';
import { Database } from '../../src/database';
import { BetsService } from '../../src/bets/bets.service';
import { PermanentEventError, type EventType } from '../../src/bets/events';
import { LeaderboardService } from '../../src/leaderboard/leaderboard.service';
import { queueNames } from '../../src/messaging/topology';
import { publishConfirmed } from '../../src/messaging/publish';
import { RabbitMqService } from '../../src/messaging/rabbitmq.service';

const schema = `test_${randomUUID().replaceAll('-', '')}`;
const queuePrefix = `test.${randomUUID()}`;
const baseConfig = loadConfig(process.env);
const admin = new Pool({ connectionString: baseConfig.databaseUrl });
const url = new URL(baseConfig.databaseUrl);
url.searchParams.set('options', `-c search_path=${schema}`);
const config: AppConfig = {
  ...baseConfig, databaseUrl: url.toString(), queuePrefix,
  fxRates: { USD: '1', EUR: '1.1', GBP: '1.25', TST: '0.00000001' },
  maxRetries: 2, retryDelayMs: 100, prefetch: 8,
};
const names = queueNames(queuePrefix);
let app: INestApplication | undefined;
let connection: ChannelModel | undefined;
let channel: ConfirmChannel;
let bets: BetsService;
let board: LeaderboardService;
let database: Database;
let baseUrl: string;

function event(betId: string, playerId = betId, winAmount = '10', currency = 'USD') {
  return { betId, playerId, currency, stake: '1', winAmount, settledAt: '2026-11-06T12:00:00.000Z' };
}

async function eventually<T>(read: () => Promise<T>, accept: (value: T) => boolean, description: string): Promise<T> {
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    const value = await read();
    if (accept(value)) return value;
    await delay(30);
  }
  assert.fail(`Timed out: ${description}`);
}

async function publish(type: EventType, value: unknown) {
  await publishConfirmed(channel, names.exchange, type, Buffer.from(JSON.stringify(value)), { type });
}

async function deadMessage(): Promise<GetMessage> {
  const message = await eventually(() => channel.get(names.dead, { noAck: true }), Boolean, 'message in DLQ');
  assert.ok(message);
  return message;
}

before(async () => {
  // Only this randomly named schema is created/dropped; existing application data is never touched.
  await admin.query(`CREATE SCHEMA "${schema}"`);
  app = await createApp(config);
  await app.listen(0, '127.0.0.1');
  await eventually(async () => app!.get(RabbitMqService).isReady(), Boolean, 'RabbitMQ consumer ready');
  baseUrl = await app.getUrl();
  bets = app.get(BetsService);
  board = app.get(LeaderboardService);
  database = app.get(Database);
  connection = await amqp.connect(config.rabbitmqUrl);
  connection.on('error', () => undefined);
  channel = await connection.createConfirmChannel();
}, { timeout: 30_000 });

after(async () => {
  await app?.close();
  if (connection) {
    const cleanup = await connection.createChannel();
    for (const queue of [names.queue, names.retry, names.dead]) await cleanup.deleteQueue(queue);
    await cleanup.deleteExchange(names.exchange);
    await cleanup.close();
    await connection.close();
  }
  try {
    await admin.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
  } finally {
    await admin.end();
  }
}, { timeout: 30_000 });

test('parallel redelivery of the same bet awards its points exactly once', async () => {
  const payload = event('duplicate-bet', 'duplicate-player', '12.3456789', 'EUR');
  await Promise.all(Array.from({ length: 16 }, () => bets.process('bet.settled', payload)));
  assert.equal((await board.get('duplicate-player')).player.points, '13.58024679');
});

test('concurrent independent bets for one player do not lose increments', async () => {
  await Promise.all(Array.from({ length: 30 }, (_, index) =>
    bets.process('bet.settled', event(`increment-${index}`, 'increment-player', '0.1'))));
  assert.equal((await board.get('increment-player')).player.points, '3');
});

test('rollback subtracts the original award exactly once and remains terminal', async () => {
  const payload = event('rollback-bet', 'rollback-player', '20', 'EUR');
  await bets.process('bet.settled', payload);
  const changedRates = new BetsService(database, { ...config, fxRates: { USD: '1', EUR: '2' } });
  await Promise.all(Array.from({ length: 10 }, () => changedRates.process('bet.rollback', payload)));
  await changedRates.process('bet.settled', payload);
  const player = (await board.get('rollback-player')).player;
  assert.equal(player.points, '0');
  assert.notEqual(player.position, null);
});

test('rollback arriving before settlement creates a terminal tombstone', async () => {
  const payload = event('early-rollback-bet', 'early-rollback-player');
  await bets.process('bet.rollback', payload);
  await Promise.all(Array.from({ length: 5 }, () => bets.process('bet.settled', payload)));
  assert.equal((await board.get('early-rollback-player')).player.points, '0');
});

test('settlement and rollback races always finish without awarded points', async () => {
  await Promise.all(Array.from({ length: 8 }, async (_, index) => {
    const payload = event(`race-${index}`, `race-player-${index}`, '2.5');
    await Promise.all([
      bets.process('bet.settled', payload),
      bets.process('bet.rollback', payload),
      bets.process('bet.settled', payload),
      bets.process('bet.rollback', payload),
    ]);
    assert.equal((await board.get(payload.playerId)).player.points, '0');
  }));
});

test('conflicting settlement and foreign-player rollback cannot modify an existing award', async () => {
  const payload = event('conflict-bet', 'conflict-player', '17');
  await bets.process('bet.settled', payload);
  await assert.rejects(bets.process('bet.settled', { ...payload, winAmount: '18' }), PermanentEventError);
  await assert.rejects(bets.process('bet.rollback', { ...payload, playerId: 'impostor' }), PermanentEventError);
  assert.equal((await board.get('conflict-player')).player.points, '17');
  assert.equal((await board.get('impostor')).player.points, '0');
});

test('duplicates and rollback work after their currency is removed from configuration', async () => {
  const payload = event('frozen-fx-bet', 'frozen-fx-player', '2', 'GBP');
  await bets.process('bet.settled', payload);
  const newService = new BetsService(database, { ...config, fxRates: { USD: '1' } });
  await newService.process('bet.settled', payload);
  assert.equal((await board.get('frozen-fx-player')).player.points, '2.5');
  await newService.process('bet.rollback', payload);
  assert.equal((await board.get('frozen-fx-player')).player.points, '0');
});

test('unknown settlement currency fails atomically, while rollback does not require its FX rate', async () => {
  const payload = event('unknown-currency-bet', 'unknown-currency-player', '5', 'XYZ');
  await assert.rejects(bets.process('bet.settled', payload), PermanentEventError);
  assert.equal((await board.get(payload.playerId)).player.position, null);
  await bets.process('bet.rollback', payload);
  assert.equal((await board.get(payload.playerId)).player.points, '0');
});

test('database and API retain sixteen fractional decimal places', async () => {
  const payload = event('smallest-bet', 'smallest-player', '0.00000001', 'TST');
  await bets.process('bet.settled', payload);
  assert.equal((await board.get(payload.playerId)).player.points, '0.0000000000000001');
});

test('real broker delivers duplicate settlements without duplicate awards', async () => {
  const payload = event('broker-duplicate-bet', 'broker-duplicate-player', '7.5');
  const original = bets.process.bind(bets);
  let processed = 0;
  bets.process = async (type, value) => {
    const result = await original(type, value);
    if ((value as { betId?: string })?.betId === payload.betId) processed += 1;
    return result;
  };
  try {
    await Promise.all(Array.from({ length: 6 }, () => publish('bet.settled', payload)));
    await eventually(async () => processed, count => count === 6, 'all redeliveries processed');
    assert.equal((await board.get(payload.playerId)).player.points, '7.5');
  } finally {
    bets.process = original;
  }
});

test('publisher rejects an unroutable message even when broker confirms it', async () => {
  await assert.rejects(publishConfirmed(channel, names.exchange, 'unknown.event', Buffer.from('{}')));
});

test('consumer restores readiness and processing after its AMQP channel closes', async () => {
  const consumer = app!.get(RabbitMqService);
  // Deliberate transport failure on this test application's private channel only.
  const currentChannel = (consumer as unknown as { channel?: ConfirmChannel }).channel;
  assert.ok(currentChannel);
  assert.equal(consumer.isReady(), true);
  await currentChannel.close();
  await eventually(async () => consumer.isReady(), ready => !ready, 'consumer becomes unready');
  const payload = event('channel-recovery-bet', 'channel-recovery-player', '8.25');
  await publish('bet.settled', payload);
  await eventually(async () => consumer.isReady(), Boolean, 'consumer reconnects after channel closure');
  await eventually(() => board.get(payload.playerId), value => value.player.points === '8.25', 'recovered consumer processes event');
});

test('failure after COMMIT is retried without adding the committed award twice', async () => {
  const payload = event('after-commit-bet', 'after-commit-player', '6.75');
  const original = bets.process.bind(bets);
  let completedAttempts = 0;
  let retrySucceeded = false;
  bets.process = async (type, value) => {
    const result = await original(type, value);
    if ((value as { betId?: string })?.betId === payload.betId) {
      completedAttempts += 1;
      // Simulates interruption after database commit, not an actual process kill.
      if (completedAttempts === 1) throw new Error('Synthetic failure after COMMIT');
      retrySucceeded = true;
    }
    return result;
  };
  try {
    await publish('bet.settled', payload);
    await eventually(async () => retrySucceeded, Boolean, 'committed event retried successfully');
    assert.equal(completedAttempts, 2);
    assert.equal((await board.get(payload.playerId)).player.points, '6.75');
  } finally {
    bets.process = original;
  }
});

test('real broker honors rollback delivered before settlement', async () => {
  const payload = event('broker-early-bet', 'broker-early-player');
  const original = bets.process.bind(bets);
  let processed = 0;
  bets.process = async (type, value) => {
    const result = await original(type, value);
    if ((value as { betId?: string })?.betId === payload.betId) processed += 1;
    return result;
  };
  try {
    await publish('bet.rollback', payload);
    await eventually(async () => processed, count => count === 1, 'rollback consumed');
    await publish('bet.settled', payload);
    await eventually(async () => processed, count => count === 2, 'late settlement consumed');
    assert.equal((await board.get(payload.playerId)).player.points, '0');
  } finally {
    bets.process = original;
  }
});

test('invalid JSON and conflicting events reach DLQ instead of retrying indefinitely', async () => {
  await publishConfirmed(channel, names.exchange, 'bet.settled', Buffer.from('{broken'), { type: 'bet.settled' });
  const malformed = await deadMessage();
  assert.equal(malformed.content.toString(), '{broken');
  assert.equal(malformed.properties.headers?.['x-error-code'], 'invalid-json');
  const payload = event('dlq-conflict-bet', 'dlq-conflict-player', '2');
  await bets.process('bet.settled', payload);
  await publish('bet.settled', { ...payload, winAmount: '3' });
  const conflict = await deadMessage();
  assert.equal(conflict.properties.headers?.['x-error-code'], 'permanent-event');
  assert.equal((await board.get(payload.playerId)).player.points, '2');
});

test('transient failures retry through RabbitMQ and eventually award points once', async () => {
  const payload = event('retry-success-bet', 'retry-success-player', '4');
  const original = bets.process.bind(bets);
  let attempts = 0;
  bets.process = async (type, value) => {
    if ((value as { betId?: string })?.betId === payload.betId && ++attempts < 3) {
      throw new Error('Synthetic transient failure');
    }
    return original(type, value);
  };
  try {
    await publish('bet.settled', payload);
    await eventually(() => board.get(payload.playerId), value => value.player.points === '4', 'retry success');
    assert.equal(attempts, 3);
  } finally {
    bets.process = original;
  }
});

test('retry exhaustion moves the original event to DLQ with no awarded points', async () => {
  const payload = event('retry-exhausted-bet', 'retry-exhausted-player');
  const original = bets.process.bind(bets);
  let attempts = 0;
  bets.process = async (type, value) => {
    if ((value as { betId?: string })?.betId === payload.betId) {
      attempts += 1;
      throw new Error('Synthetic persistent failure');
    }
    return original(type, value);
  };
  try {
    await publish('bet.settled', payload);
    const dead = await deadMessage();
    assert.equal(dead.properties.headers?.['x-error-code'], 'retries-exhausted');
    assert.deepEqual(JSON.parse(dead.content.toString()), payload);
    assert.equal(attempts, config.maxRetries + 1);
    assert.equal((await board.get(payload.playerId)).player.points, '0');
  } finally {
    bets.process = original;
  }
});

test('leaderboard returns a stable top 100 plus a player outside that top', async () => {
  await Promise.all(Array.from({ length: 105 }, (_, index) =>
    bets.process('bet.settled', event(`ranking-bet-${index}`, `ranking-${String(index).padStart(3, '0')}`, '1000000'))));
  const result = await board.get('ranking-104');
  assert.equal(result.top.length, 100);
  assert.equal(result.top[0]?.playerId, 'ranking-000');
  assert.equal(result.top[99]?.playerId, 'ranking-099');
  assert.deepEqual(result.player, { playerId: 'ranking-104', points: '1000000', position: 105 });
  assert.deepEqual(result.top.map(entry => entry.position), Array.from({ length: 100 }, (_, index) => index + 1));
});

test('HTTP endpoint validates X-Player-Id and reports unknown players', async () => {
  const missing = await fetch(`${baseUrl}/leaderboard`);
  assert.equal(missing.status, 400);
  const invalid = await fetch(`${baseUrl}/leaderboard`, { headers: { 'X-Player-Id': 'bad player' } });
  assert.equal(invalid.status, 400);
  const valid = await fetch(`${baseUrl}/leaderboard`, { headers: { 'X-Player-Id': 'ranking-104' } });
  assert.equal(valid.status, 200);
  const result = await valid.json() as { top: unknown[]; player: { position: number } };
  assert.equal(result.top.length, 100);
  assert.equal(result.player.position, 105);
  const unknown = await fetch(`${baseUrl}/leaderboard`, { headers: { 'X-Player-Id': 'never-seen' } });
  assert.deepEqual((await unknown.json() as { player: unknown }).player,
    { playerId: 'never-seen', points: '0', position: null });
});
