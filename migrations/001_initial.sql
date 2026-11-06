CREATE TABLE bets (
  bet_id text COLLATE "C" PRIMARY KEY,
  player_id text COLLATE "C" NOT NULL,
  status text NOT NULL CHECK (status IN ('pending', 'settled', 'rolled_back')),
  settlement jsonb,
  usd_rate numeric(14,8),
  awarded_points numeric(38,16) NOT NULL DEFAULT 0 CHECK (awarded_points >= 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK ((settlement IS NULL AND usd_rate IS NULL AND awarded_points = 0)
      OR (settlement IS NOT NULL AND usd_rate > 0)),
  CHECK (status <> 'settled' OR settlement IS NOT NULL)
);

CREATE TABLE player_scores (
  player_id text COLLATE "C" PRIMARY KEY,
  points numeric(48,16) NOT NULL CHECK (points >= 0)
);

CREATE INDEX player_scores_leaderboard_idx ON player_scores (points DESC, player_id ASC);
