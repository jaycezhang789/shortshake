import { config as loadEnv } from 'dotenv';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { BinanceService, Timeframe } from './binance/binance.service';

loadEnv();

async function bootstrap() {
  const app = await NestFactory.createApplicationContext(AppModule);

  try {
    const binanceService = app.get(BinanceService);
    const timeframe = parseTimeframe(process.env.TIMEFRAME);
    const movers = await binanceService.getMovers(timeframe);

    console.dir(movers, { depth: null });
  } catch (error) {
    console.error('Main process failed:', error);
    process.exitCode = 1;
  } finally {
    await app.close();
  }
}

function parseTimeframe(
  rawTimeframe: string | undefined,
): Timeframe | undefined {
  if (!rawTimeframe) {
    return undefined;
  }

  return Object.values(Timeframe).includes(rawTimeframe as Timeframe)
    ? (rawTimeframe as Timeframe)
    : undefined;
}

bootstrap();
