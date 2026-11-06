import { BadRequestException, Controller, Get, Headers } from '@nestjs/common';
import { PLAYER_ID_PATTERN } from '../bets/events';
import { LeaderboardService } from './leaderboard.service';

@Controller('leaderboard')
export class LeaderboardController {
  constructor(private readonly leaderboard: LeaderboardService) {}
  @Get()
  get(@Headers('x-player-id') playerId: string | undefined) {
    if (typeof playerId !== 'string' || !PLAYER_ID_PATTERN.test(playerId)) {
      throw new BadRequestException('A valid X-Player-Id header is required');
    }
    return this.leaderboard.get(playerId);
  }
}
