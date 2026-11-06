import Decimal from 'decimal.js';

export const APP_CONFIG = Symbol('APP_CONFIG');
export interface AppConfig {
  databaseUrl: string;
  rabbitmqUrl: string;
  port: number;
  fxRates: Record<string, string>;
  queuePrefix: string;
  prefetch: number;
  maxRetries: number;
  retryDelayMs: number;
}

function integer(value: string | undefined, fallback: number, min: number, max: number, name: string): number {
  if (value === undefined) return fallback;
  if (!/^\d+$/.test(value)) throw new Error(`Invalid ${name}`);
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < min || parsed > max) throw new Error(`Invalid ${name}`);
  return parsed;
}

function url(value: string, protocols: string[], name: string): string {
  try {
    if (!protocols.includes(new URL(value).protocol)) throw new Error();
    return value;
  } catch {
    throw new Error(`Invalid ${name}`);
  }
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  let rates: unknown;
  try {
    rates = JSON.parse(env.FX_RATES ?? '{"USD":"1","EUR":"1.10","GBP":"1.25"}');
  } catch {
    throw new Error('FX_RATES must be a JSON object');
  }
  if (!rates || typeof rates !== 'object' || Array.isArray(rates)) throw new Error('Invalid FX_RATES');
  const fxRates: Record<string, string> = Object.create(null) as Record<string, string>;
  for (const [currency, rate] of Object.entries(rates)) {
    if (!/^[A-Z]{3}$/.test(currency) ||
      !['string', 'number'].includes(typeof rate) ||
      !/^(0|[1-9]\d{0,5})(\.\d{1,8})?$/.test(String(rate)) ||
      !new Decimal(String(rate)).greaterThan(0)) throw new Error('Invalid FX_RATES entry');
    fxRates[currency] = new Decimal(String(rate)).toFixed();
  }
  if (fxRates.USD !== '1') throw new Error('FX_RATES must include USD=1');
  const queuePrefix = env.QUEUE_PREFIX ?? 'tournament';
  if (!/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,99}$/.test(queuePrefix) || queuePrefix.startsWith('amq.')) {
    throw new Error('Invalid QUEUE_PREFIX');
  }
  return {
    databaseUrl: url(env.DATABASE_URL ?? 'postgresql://tournament:tournament@localhost:55432/tournament', ['postgres:', 'postgresql:'], 'DATABASE_URL'),
    rabbitmqUrl: url(env.RABBITMQ_URL ?? 'amqp://tournament:tournament@localhost:5673', ['amqp:', 'amqps:'], 'RABBITMQ_URL'),
    port: integer(env.PORT, 3000, 1, 65535, 'PORT'),
    fxRates,
    queuePrefix,
    prefetch: integer(env.PREFETCH, 16, 1, 1000, 'PREFETCH'),
    maxRetries: integer(env.MAX_RETRIES, 5, 0, 20, 'MAX_RETRIES'),
    retryDelayMs: integer(env.RETRY_DELAY_MS, 1000, 100, 60000, 'RETRY_DELAY_MS'),
  };
}
