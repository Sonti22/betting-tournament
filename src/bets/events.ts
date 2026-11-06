import Decimal from 'decimal.js';
import { z } from 'zod';

export type EventType = 'bet.settled' | 'bet.rollback';
export const PLAYER_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
export class PermanentEventError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PermanentEventError';
  }
}

const amount = z.union([z.string(), z.number().finite().nonnegative().max(Number.MAX_SAFE_INTEGER)])
  .transform((value) => typeof value === 'number' ? new Decimal(value).toFixed() : value)
  .pipe(z.string().regex(/^(0|[1-9]\d{0,11})(\.\d{1,8})?$/))
  .transform((value) => new Decimal(value).toFixed());

const eventSchema = z.object({
  betId: z.string().regex(PLAYER_ID_PATTERN),
  playerId: z.string().regex(PLAYER_ID_PATTERN),
  currency: z.string().regex(/^[A-Z]{3}$/),
  stake: amount,
  winAmount: amount,
  settledAt: z.iso.datetime({ offset: true }).transform((value) => new Date(value).toISOString()),
}).strict();

export type BetEvent = z.infer<typeof eventSchema>;
export function parseEvent(payload: unknown): BetEvent {
  const result = eventSchema.safeParse(payload);
  if (!result.success) throw new PermanentEventError('Invalid event payload');
  return result.data;
}
