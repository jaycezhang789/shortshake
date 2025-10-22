import { Controller, Get, ParseEnumPipe, Query } from '@nestjs/common';
import { BinanceService, Timeframe } from './binance.service';

@Controller('futures')
export class BinanceController {
  constructor(private readonly binanceService: BinanceService) {}

  @Get('movers')
  getMovers(
    @Query('timeframe', new ParseEnumPipe(Timeframe, { optional: true }))
    timeframe?: Timeframe,
  ) {
    return this.binanceService.getMovers(timeframe);
  }
}
