import { config as loadEnv } from 'dotenv';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { BinanceService } from './binance/binance.service';
import { TelegramService } from './telegram/telegram.service';
import { TradingService } from './trading/trading.service';
import { StrategyService } from './trading/strategy.service';

loadEnv();

async function bootstrap() {
  console.log('Bootstrapping application context...');
  const app = await NestFactory.createApplicationContext(AppModule);
  const intervalMinutes = Number(process.env.REFRESH_INTERVAL_MINUTES) || 10;
  const intervalMs = intervalMinutes * 60 * 1000;
  const binanceService = app.get(BinanceService);
  const telegramService = app.get(TelegramService);
  const tradingService = app.get(TradingService);
  const strategyService = app.get(StrategyService);
  let lastWalletReport = 0;
  const walletReportIntervalMs = 10 * 60 * 1000;
  let isRunning = false;
  let isShuttingDown = false;

  console.log('Initializing trading service state...');
  await tradingService.initialize();
  console.log('Trading service initialization complete.');

  const execute = async () => {
    if (isRunning) {
      console.warn(
        `Skip execution at ${new Date().toISOString()} - previous run in progress.`,
      );
      return;
    }

    isRunning = true;
    try {
      console.log(`[${new Date().toISOString()}] Starting refresh cycle...`);
      await tradingService.refreshState();
      console.log(
        `[${new Date().toISOString()}] Trading service state refreshed.`,
      );

      console.log(`\n[${new Date().toISOString()}] Refreshing movers data...`);
      const movers = await binanceService.getTopMovers();
      for (const [label, snapshot] of Object.entries(movers.snapshots)) {
        console.log(
          `[${label}] 涨幅榜 ${snapshot.topGainers.length} 项 | 跌幅榜 ${snapshot.topLosers.length} 项`,
        );
      }
      console.log(
        `[综合排名] 已选取 ${movers.aggregatedTop.length} 个高分标的用于推送。`,
      );
      await strategyService.process(movers);
      await telegramService.sendMoversReport(movers);
      if (Date.now() - lastWalletReport >= walletReportIntervalMs) {
        await telegramService.sendWalletSummary(
          tradingService.getWalletBalance(),
          tradingService.getAvailableBalance(),
          tradingService.getUnrealizedPnl(),
          tradingService.getPositionSummaries(),
        );
        lastWalletReport = Date.now();
      }
      console.log(
        `[${new Date().toISOString()}] Movers report dispatched to Telegram.`,
      );
    } catch (error) {
      console.error('Main process failed:', error);
    } finally {
      isRunning = false;
      console.log(
        `[${new Date().toISOString()}] Refresh cycle finished. Awaiting next run.`,
      );
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
    console.log('Clearing timers and closing Nest application context...');
    clearInterval(timer);
    await app.close();
    console.log('Shutdown sequence complete. Exiting process.');
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
