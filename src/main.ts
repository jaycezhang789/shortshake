import { config as loadEnv } from 'dotenv';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { BinanceService } from './binance/binance.service';
import { TelegramService } from './telegram/telegram.service';
import { TradingService } from './trading/trading.service';

loadEnv();

async function bootstrap() {
  const app = await NestFactory.createApplicationContext(AppModule);
  const intervalMinutes = Number(process.env.REFRESH_INTERVAL_MINUTES) || 10;
  const intervalMs = intervalMinutes * 60 * 1000;
  const binanceService = app.get(BinanceService);
  const telegramService = app.get(TelegramService);
  const tradingService = app.get(TradingService);
  let isRunning = false;
  let isShuttingDown = false;

  await tradingService.initialize();

  const execute = async () => {
    if (isRunning) {
      console.warn(
        `Skip execution at ${new Date().toISOString()} - previous run in progress.`,
      );
      return;
    }

    isRunning = true;
    try {
      await tradingService.refreshState();

      console.log(`\n[${new Date().toISOString()}] Refreshing movers data...`);
      const movers = await binanceService.getTopMovers();
      console.dir(movers, { depth: null });
      await telegramService.sendMoversReport(movers);
    } catch (error) {
      console.error('Main process failed:', error);
    } finally {
      isRunning = false;
    }
  };

  await execute();
  const timer = setInterval(() => {
    void execute();
  }, intervalMs);

  const shutdown = async (signal: NodeJS.Signals) => {
    if (isShuttingDown) {
      return;
    }
    isShuttingDown = true;
    console.log(`\nReceived ${signal}, shutting down...`);
    clearInterval(timer);
    await app.close();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
  process.on('uncaughtException', async (error) => {
    console.error('Uncaught exception:', error);
    await shutdown('SIGTERM');
  });
  process.on('unhandledRejection', async (reason) => {
    console.error('Unhandled rejection:', reason);
    await shutdown('SIGTERM');
  });
}
bootstrap();
