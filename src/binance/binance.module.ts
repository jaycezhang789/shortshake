import { Module } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { BinanceService } from './binance.service';

@Module({
  imports: [
    HttpModule.register({
      baseURL: 'https://fapi.binance.com',
      timeout: 10_000,
      maxRedirects: 0,
    }),
  ],
  providers: [BinanceService],
})
export class BinanceModule {}
