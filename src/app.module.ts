import { Module } from '@nestjs/common';
import { BinanceModule } from './binance/binance.module';
import { TelegramService } from './telegram/telegram.service';
import { TradingService } from './trading/trading.service';

@Module({
  imports: [BinanceModule],
  providers: [TelegramService, TradingService],
  exports: [TelegramService, TradingService],
})
export class AppModule {}
