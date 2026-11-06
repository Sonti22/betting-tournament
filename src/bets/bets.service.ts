import { Inject, Injectable } from '@nestjs/common';
import { APP_CONFIG, AppConfig } from '../config';
import { Database } from '../database';
import { BetEvent, EventType, parseEvent, PermanentEventError } from './events';
import { calculatePoints } from './scoring';

interface BetRow {
  player_id: string;
  status: 'pending' | 'settled' | 'rolled_back';
  settlement: BetEvent | null;
  awarded_points: string;
}

function sameSettlement(a: BetEvent, b: BetEvent): boolean {
  return a.betId === b.betId && a.playerId === b.playerId && a.currency === b.currency &&
    a.stake === b.stake && a.winAmount === b.winAmount && a.settledAt === b.settledAt;
}

@Injectable()
export class BetsService {
  constructor(private readonly db: Database, @Inject(APP_CONFIG) private readonly config: AppConfig) {}

  async process(type: EventType, payload: unknown): Promise<{ outcome: string }> {
    if (type !== 'bet.settled' && type !== 'bet.rollback') throw new PermanentEventError('Unknown event type');
    const event = parseEvent(payload);
    const client = await this.db.pool.connect();
    try {
      await client.query('BEGIN');
      // The unique key serializes first insertion too; a row lock alone cannot lock an absent bet.
      await client.query(`INSERT INTO bets(bet_id,player_id,status) VALUES($1,$2,'pending')
        ON CONFLICT (bet_id) DO NOTHING`, [event.betId, event.playerId]);
      const result = await client.query<BetRow>(
        'SELECT player_id,status,settlement,awarded_points FROM bets WHERE bet_id=$1 FOR UPDATE', [event.betId]);
      const bet = result.rows[0]!;
      if (bet.player_id !== event.playerId) throw new PermanentEventError('betId belongs to another player');
      let outcome: string;
      if (type === 'bet.rollback') {
        if (bet.status === 'rolled_back') {
          outcome = 'duplicate';
        } else {
          if (bet.status === 'settled') {
            const updated = await client.query('UPDATE player_scores SET points=points-$2::numeric WHERE player_id=$1',
              [event.playerId, bet.awarded_points]);
            if (updated.rowCount !== 1) throw new Error('Missing score for settled bet');
          }
          await client.query("UPDATE bets SET status='rolled_back',updated_at=now() WHERE bet_id=$1", [event.betId]);
          outcome = 'rolled_back';
        }
      } else {
        if (bet.settlement && !sameSettlement(bet.settlement, event)) {
          throw new PermanentEventError('Conflicting settlement for betId');
        }
        if (bet.status === 'rolled_back') {
          outcome = 'ignored_after_rollback';
        } else if (bet.status === 'settled') {
          outcome = 'duplicate';
        } else {
          const rate = this.config.fxRates[event.currency];
          if (!rate) throw new PermanentEventError('Unsupported settlement currency');
          const points = calculatePoints(event.winAmount, rate);
          await client.query(`UPDATE bets SET status='settled',settlement=$2::jsonb,
            usd_rate=$3,awarded_points=$4,updated_at=now() WHERE bet_id=$1`,
          [event.betId, JSON.stringify(event), rate, points]);
          await client.query(`INSERT INTO player_scores(player_id,points) VALUES ($1,$2)
            ON CONFLICT(player_id) DO UPDATE SET points=player_scores.points+EXCLUDED.points`,
          [event.playerId, points]);
          outcome = 'settled';
        }
      }
      await client.query('COMMIT');
      return { outcome };
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }
}
