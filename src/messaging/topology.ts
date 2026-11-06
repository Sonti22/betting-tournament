import type { Channel } from 'amqplib';
import type { AppConfig } from '../config';

export function queueNames(prefix: string) {
  return {
    exchange: `${prefix}.events`,
    queue: `${prefix}.bets`,
    retry: `${prefix}.retry`,
    dead: `${prefix}.dead`,
  };
}

export async function setupTopology(
  channel: Channel,
  config: Pick<AppConfig, 'queuePrefix' | 'retryDelayMs'>,
): Promise<void> {
  const names = queueNames(config.queuePrefix);
  await channel.assertExchange(names.exchange, 'direct', { durable: true });
  await channel.assertQueue(names.dead, {
    durable: true,
    arguments: { 'x-queue-type': 'quorum' },
  });
  await channel.assertQueue(names.queue, {
    durable: true,
    arguments: {
      'x-queue-type': 'quorum',
      // Unexpected process/channel failures eventually park poison deliveries too.
      'x-delivery-limit': 20,
      'x-dead-letter-exchange': '',
      'x-dead-letter-routing-key': names.dead,
      'x-dead-letter-strategy': 'at-least-once',
      'x-overflow': 'reject-publish',
    },
  });
  await channel.bindQueue(names.queue, names.exchange, 'bet.settled');
  await channel.bindQueue(names.queue, names.exchange, 'bet.rollback');
  await channel.assertQueue(names.retry, {
    durable: true,
    arguments: {
      'x-queue-type': 'quorum',
      'x-message-ttl': config.retryDelayMs,
      // Retry routes directly back to the main queue; event type is preserved in headers.
      'x-dead-letter-exchange': '',
      'x-dead-letter-routing-key': names.queue,
      'x-dead-letter-strategy': 'at-least-once',
      'x-overflow': 'reject-publish',
    },
  });
}
