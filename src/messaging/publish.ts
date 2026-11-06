import { randomUUID } from 'node:crypto';
import type { ConfirmChannel, Message, Options } from 'amqplib';

type PendingPublication = { reject: (error: Error) => void };
const pendingByChannel = new WeakMap<ConfirmChannel, Map<string, PendingPublication>>();

function publications(channel: ConfirmChannel): Map<string, PendingPublication> {
  let pending = pendingByChannel.get(channel);
  if (pending) return pending;
  pending = new Map();
  pendingByChannel.set(channel, pending);
  channel.on('return', (message: Message) => {
    pending?.get(message.properties.messageId ?? '')?.reject(new Error('Publication has no route'));
  });
  channel.on('close', () => {
    for (const item of pending?.values() ?? []) item.reject(new Error('Publication channel closed'));
    pending?.clear();
  });
  return pending;
}

/** A broker confirm alone does not detect an unroutable message: mandatory returns do. */
export function publishConfirmed(
  channel: ConfirmChannel,
  exchange: string,
  routingKey: string,
  content: Buffer,
  options: Options.Publish = {},
  timeoutMs = 10_000,
): Promise<void> {
  const pending = publications(channel);
  const messageId = randomUUID();
  return new Promise<void>((resolve, reject) => {
    const finish = (error?: Error | null) => {
      if (!pending.delete(messageId)) return;
      clearTimeout(timer);
      if (error) reject(error);
      else resolve();
    };
    const timer = setTimeout(() => finish(new Error('Publisher confirm timed out')), timeoutMs);
    pending.set(messageId, { reject: finish });
    try {
      channel.publish(exchange, routingKey, content, {
        ...options,
        messageId,
        mandatory: true,
        persistent: true,
      }, finish);
    } catch {
      finish(new Error('Publication failed'));
    }
  });
}
