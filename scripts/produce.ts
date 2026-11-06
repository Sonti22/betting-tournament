import 'reflect-metadata';
import { readFile } from 'node:fs/promises';
import { connect } from 'amqplib';
import { loadConfig } from '../src/config';
import { publishConfirmed } from '../src/messaging/publish';
import { queueNames, setupTopology } from '../src/messaging/topology';

type EventType = 'bet.settled' | 'bet.rollback';
type Publication = { type: EventType; payload: unknown };

function demoEvents(): Publication[] {
  const event = (betId: string, playerId: string, currency: string, winAmount: string) => ({
    betId, playerId, currency, stake: '10', winAmount, settledAt: '2026-01-01T12:00:00.000Z',
  });
  const alice = event('demo-alice-1', 'demo-alice', 'USD', '100.25');
  const bob = event('demo-bob-1', 'demo-bob', 'EUR', '100');
  const carol = event('demo-carol-1', 'demo-carol', 'USD', '60');
  const carolLost = event('demo-carol-0', 'demo-carol', 'USD', '0');
  const cancelled = event('demo-alice-2', 'demo-alice', 'USD', '100');
  return [
    { type: 'bet.settled', payload: alice },
    { type: 'bet.settled', payload: alice },
    { type: 'bet.settled', payload: bob },
    { type: 'bet.settled', payload: carolLost },
    { type: 'bet.settled', payload: carol },
    { type: 'bet.rollback', payload: carol },
    { type: 'bet.rollback', payload: cancelled },
    { type: 'bet.settled', payload: cancelled },
  ];
}

async function main(): Promise<void> {
  const [command = 'demo', file, ...rest] = process.argv.slice(2);
  let events: Publication[];
  if (command === 'demo' && !file && rest.length === 0) events = demoEvents();
  else if ((command === 'bet.settled' || command === 'bet.rollback') && file && rest.length === 0) {
    // Intentionally no business validation: this producer also lets reviewers exercise the DLQ.
    events = [{ type: command, payload: JSON.parse(await readFile(file, 'utf8')) as unknown }];
  } else {
    throw new Error('Usage: npm run produce -- demo | bet.settled event.json | bet.rollback event.json');
  }
  const config = loadConfig();
  const connection = await connect(config.rabbitmqUrl, { timeout: 5_000 });
  // Do not print error objects, which may contain broker URLs or payload fragments.
  connection.on('error', () => console.error('RabbitMQ connection error'));
  try {
    const channel = await connection.createConfirmChannel();
    channel.on('error', () => console.error('RabbitMQ channel error'));
    try {
      await setupTopology(channel, config);
      const names = queueNames(config.queuePrefix);
      for (const event of events) {
        await publishConfirmed(channel, names.exchange, event.type, Buffer.from(JSON.stringify(event.payload)), {
          contentType: 'application/json',
          type: event.type,
        });
      }
      console.log(`Broker confirmed ${events.length} event(s). Processing is asynchronous.`);
      if (command === 'demo') {
        console.log('With default FX and an empty database: demo-bob=110, demo-alice=100.25, demo-carol=0.');
        console.log('Demo betIds are stable; rerunning the demo does not add points.');
      }
    } finally {
      await channel.close().catch(() => undefined);
    }
  } finally {
    await connection.close().catch(() => undefined);
  }
}

void main().catch(() => {
  console.error('Producer failed. Check arguments, JSON file, broker availability and configuration.');
  process.exitCode = 1;
});
