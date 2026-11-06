import { Injectable } from '@nestjs/common';
import { Database } from '../database';

export interface LeaderboardEntry { playerId: string; points: string; position: number }
export interface Leaderboard {
  top: LeaderboardEntry[];
  player: { playerId: string; points: string; position: number | null };
}

@Injectable()
export class LeaderboardService {
  constructor(private readonly db: Database) {}
  async get(playerId: string): Promise<Leaderboard> {
    // One statement means top and personal rank see the same committed snapshot.
    // LIMIT uses the ranking index; computing an arbitrary player's rank is still O(N).
    const result = await this.db.pool.query<Leaderboard>(`
      WITH leaders AS (
        SELECT player_id,points FROM player_scores ORDER BY points DESC,player_id ASC LIMIT 100
      ), ranked_top AS (
        SELECT player_id,points,row_number() OVER (ORDER BY points DESC,player_id ASC) AS position FROM leaders
      ), me AS (SELECT player_id,points FROM player_scores WHERE player_id=$1)
      SELECT COALESCE((SELECT jsonb_agg(jsonb_build_object(
        'playerId',player_id,'points',trim_scale(points)::text,'position',position) ORDER BY position) FROM ranked_top),'[]'::jsonb) AS top,
        COALESCE((SELECT jsonb_build_object('playerId',m.player_id,'points',trim_scale(m.points)::text,'position',
          (SELECT count(*)+1 FROM player_scores p WHERE p.points>m.points
            OR (p.points=m.points AND p.player_id<m.player_id))) FROM me m),
          jsonb_build_object('playerId',$1::text,'points','0','position',NULL)) AS player
    `, [playerId]);
    return result.rows[0]!;
  }
}
