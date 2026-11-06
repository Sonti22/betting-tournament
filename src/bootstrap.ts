import 'reflect-metadata';
import { INestApplication } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { AppConfig, loadConfig } from './config';

export async function createApp(config: AppConfig = loadConfig()): Promise<INestApplication> {
  const app = await NestFactory.create(AppModule.register(config), { abortOnError: false });
  try {
    app.enableShutdownHooks();
    await app.init();
    return app;
  } catch (error) {
    await app.close().catch(() => undefined);
    throw error;
  }
}
