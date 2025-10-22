import { Module } from '@nestjs/common';
import { BinanceModule } from './binance/binance.module';
import { TelegramService } from './telegram/telegram.service';
import { TradingService } from './trading/trading.service';
import { StrategyService } from './trading/strategy.service';

@Module({
  imports: [BinanceModule],
  providers: [TelegramService, TradingService, StrategyService],
  exports: [TelegramService, TradingService, StrategyService],
})
export class AppModule {}
