import { Inject, Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import { connect, AmqpConnectionManager, ChannelWrapper } from 'amqp-connection-manager';
import type { ConfirmChannel, ConsumeMessage } from 'amqplib';
import { APP_CONFIG, AppConfig } from '../config';
import { BetsService } from '../bets/bets.service';
import { PermanentEventError } from '../bets/events';
import { publishConfirmed } from './publish';
import { queueNames, setupTopology } from './topology';

@Injectable()
export class RabbitMqService implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(RabbitMqService.name);
  private connection?: AmqpConnectionManager;
  private wrapper?: ChannelWrapper;
  private channel?: ConfirmChannel;
  private consumerTag?: string;
  private ready = false;
  private stopping = false;
  private reconnectTimer?: ReturnType<typeof setTimeout>;
  private readonly pending = new Set<Promise<void>>();

  constructor(
    @Inject(APP_CONFIG) private readonly config: AppConfig,
    private readonly bets: BetsService,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    this.connection = connect([this.config.rabbitmqUrl], {
      heartbeatIntervalInSeconds: 5,
      reconnectTimeInSeconds: 2,
      connectionOptions: { timeout: 5_000 },
    });
    this.connection.on('disconnect', () => { this.ready = false; });
    this.connection.on('connectFailed', () => {
      this.ready = false;
      this.logger.warn('RabbitMQ connection unavailable; reconnect scheduled');
    });
    this.connection.on('error', () => {
      this.ready = false;
      this.logger.error('RabbitMQ connection error');
    });
    this.wrapper = this.connection.createChannel({
      name: 'tournament-consumer',
      confirm: true,
      setup: async (channel: ConfirmChannel) => {
        this.ready = false;
        this.channel = channel;
        channel.on('error', () => {
          this.ready = false;
          this.logger.error('RabbitMQ channel error');
        });
        channel.on('close', () => {
          if (this.channel === channel) {
            this.ready = false;
            this.requestReconnect();
          }
        });
        await setupTopology(channel, this.config);
        await channel.prefetch(this.config.prefetch);
        const consumer = await channel.consume(queueNames(this.config.queuePrefix).queue, (message) => {
          if (!message) {
            this.ready = false;
            this.requestReconnect();
            return;
          }
          const task = this.handle(channel, message).catch(() => {
            // Keep delivery unacked. Reconnection requeues it; a committed duplicate is harmless.
            this.ready = false;
            this.logger.warn('Delivery interrupted; reconnecting for redelivery');
            this.requestReconnect();
          });
          this.pending.add(task);
          void task.finally(() => this.pending.delete(task));
        }, { noAck: false });
        this.consumerTag = consumer.consumerTag;
        this.ready = !this.stopping;
      },
    });
    this.wrapper.on('error', () => {
      this.ready = false;
      this.logger.error('RabbitMQ topology or channel error');
      this.requestReconnect();
    });
    // HTTP can start during a broker outage; /health must report not ready until setup succeeds.
  }

  isReady(): boolean {
    return this.ready && !this.stopping && !!this.connection?.isConnected();
  }

  private requestReconnect(): void {
    if (this.stopping || this.reconnectTimer) return;
    // Coalesce failures of concurrent deliveries and allow connection-level recovery first.
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      if (!this.stopping && this.connection?.isConnected()) this.connection.reconnect();
    }, 1000);
  }

  private async handle(channel: ConfirmChannel, message: ConsumeMessage): Promise<void> {
    const headers = message.properties.headers ?? {};
    const names = queueNames(this.config.queuePrefix);
    // An initial publication's routing key is authoritative; retry headers cannot override it.
    const type = message.fields.exchange === names.exchange
      ? message.fields.routingKey
      : headers['x-original-routing-key'] ?? message.fields.routingKey;
    const attempts = headers['x-retry-count'] ?? 0;
    let reason: string | undefined;
    if (!Number.isSafeInteger(attempts) || attempts < 0 || attempts > this.config.maxRetries) {
      reason = 'invalid-retry-count';
    } else if (type !== 'bet.settled' && type !== 'bet.rollback') {
      reason = 'invalid-event-type';
    } else if (message.content.length > 16_384) {
      reason = 'event-too-large';
    } else {
      let payload: unknown;
      try {
        payload = JSON.parse(message.content.toString('utf8')) as unknown;
      } catch {
        reason = 'invalid-json';
      }
      if (!reason) {
        try {
          await this.bets.process(type, payload);
          channel.ack(message);
          return;
        } catch (error) {
          if (error instanceof PermanentEventError) reason = 'permanent-event';
          else if (attempts >= this.config.maxRetries) reason = 'retries-exhausted';
          else {
            await this.forward(channel, message, queueNames(this.config.queuePrefix).retry, {
              'x-original-routing-key': type,
              'x-retry-count': attempts + 1,
            });
            channel.ack(message);
            this.logger.warn('Transient processing failure; delayed retry confirmed');
            return;
          }
        }
      }
    }
    await this.forward(channel, message, queueNames(this.config.queuePrefix).dead, {
      'x-original-routing-key': typeof type === 'string' ? type : 'invalid',
      'x-error-code': reason ?? 'invalid-event',
    });
    channel.ack(message);
    this.logger.warn(`Delivery parked in DLQ (${reason})`);
  }

  private async forward(
    channel: ConfirmChannel,
    message: ConsumeMessage,
    queue: string,
    headers: Record<string, string | number>,
  ): Promise<void> {
    await publishConfirmed(channel, '', queue, message.content, {
      contentType: message.properties.contentType ?? 'application/json',
      type: message.properties.type,
      headers: { ...message.properties.headers, ...headers },
    });
  }

  async onModuleDestroy(): Promise<void> {
    this.stopping = true;
    this.ready = false;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    if (this.channel && this.consumerTag) {
      await this.channel.cancel(this.consumerTag).catch(() => undefined);
    }
    let timer: ReturnType<typeof setTimeout> | undefined;
    await Promise.race([
      Promise.allSettled([...this.pending]),
      new Promise<void>((resolve) => { timer = setTimeout(resolve, 15_000); }),
    ]);
    if (timer) clearTimeout(timer);
    await this.connection?.close();
  }
}
