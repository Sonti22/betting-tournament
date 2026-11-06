import { DynamicModule, Module } from '@nestjs/common';
import { APP_CONFIG, AppConfig } from './config';
import { Database } from './database';
import { BetsService } from './bets/bets.service';
import { LeaderboardController } from './leaderboard/leaderboard.controller';
import { LeaderboardService } from './leaderboard/leaderboard.service';
import { RabbitMqService } from './messaging/rabbitmq.service';
import { HealthController } from './health.controller';

@Module({})
export class AppModule {
  static register(config: AppConfig): DynamicModule {
    return {
      module: AppModule,
      providers: [{ provide: APP_CONFIG, useValue: config }, Database, BetsService, LeaderboardService, RabbitMqService],
      controllers: [LeaderboardController, HealthController],
    };
  }
}
