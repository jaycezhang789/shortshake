import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { BinanceService } from './binance.service';
import { BinanceController } from './binance.controller';

@Module({
  imports: [
    HttpModule.register({
      baseURL: 'https://fapi.binance.com',
      timeout: 10_000,
      maxRedirects: 0,
    }),
  ],
  controllers: [BinanceController],
  providers: [BinanceService],
})
export class BinanceModule {}
