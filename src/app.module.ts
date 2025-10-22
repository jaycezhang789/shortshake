import { Module } from '@nestjs/common';
import { BinanceModule } from './binance/binance.module';
import { TelegramService } from './telegram/telegram.service';

@Module({
  imports: [BinanceModule],
  providers: [TelegramService],
  exports: [TelegramService],
})
export class AppModule {}
