import { Controller, Get, ServiceUnavailableException } from '@nestjs/common';
import { Database } from './database';
import { RabbitMqService } from './messaging/rabbitmq.service';

@Controller('health')
export class HealthController {
  constructor(private readonly db: Database, private readonly rabbitmq: RabbitMqService) {}
  @Get()
  async get() {
    try {
      await this.db.pool.query('SELECT 1');
      if (!this.rabbitmq.isReady()) throw new Error();
      return { status: 'ok' };
    } catch {
      throw new ServiceUnavailableException('Service dependencies are unavailable');
    }
  }
}
