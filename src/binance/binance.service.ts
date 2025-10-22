import { HttpService } from '@nestjs/axios';
import { Injectable, Logger } from '@nestjs/common';
import { firstValueFrom } from 'rxjs';

export enum Timeframe {
  TEN_MINUTES = '10m',
  THIRTY_MINUTES = '30m',
  ONE_HOUR = '1h',
  FOUR_HOURS = '4h',
}

type TimeframeKey = `${Timeframe}`;

const TIMEFRAME_MINUTES: Record<TimeframeKey, number> = {
  [Timeframe.TEN_MINUTES]: 10,
  [Timeframe.THIRTY_MINUTES]: 30,
  [Timeframe.ONE_HOUR]: 60,
  [Timeframe.FOUR_HOURS]: 240,
};

const MAX_LOOKBACK_MINUTES = Math.max(...Object.values(TIMEFRAME_MINUTES));
const KLINE_LIMIT = MAX_LOOKBACK_MINUTES + 1;
const TOP_LIMIT = 15;
const SYMBOL_CACHE_TTL = 6 * 60 * 60 * 1000; // 6 hours
const METRICS_CACHE_TTL = 30 * 1000; // 30 seconds
const CONCURRENCY = 8;

interface SymbolMetrics {
  symbol: string;
  lastPrice: number;
  changes: Record<TimeframeKey, number>;
}

interface Movers {
  timeframe: Timeframe;
  topGainers: Array<{ symbol: string; price: number; changePercent: number }>;
  topLosers: Array<{ symbol: string; price: number; changePercent: number }>;
}

@Injectable()
export class BinanceService {
  private readonly logger = new Logger(BinanceService.name);

  private symbolsCache: { data: string[]; expiresAt: number } | null = null;
  private metricsCache:
    | { data: SymbolMetrics[]; expiresAt: number }
    | null = null;

  constructor(private readonly http: HttpService) {}

  async getMovers(timeframe?: Timeframe): Promise<Record<TimeframeKey, Movers> | Movers> {
    const metrics = await this.loadSymbolMetrics();
    if (timeframe) {
      return this.buildMovers(metrics, timeframe);
    }

    const result = {} as Record<TimeframeKey, Movers>;
    for (const key of Object.values(Timeframe)) {
      result[key] = this.buildMovers(metrics, key);
    }
    return result;
  }

  private buildMovers(metrics: SymbolMetrics[], timeframe: Timeframe): Movers {
    const sortedDesc = [...metrics].sort(
      (a, b) => b.changes[timeframe] - a.changes[timeframe],
    );
    const sortedAsc = [...metrics].sort(
      (a, b) => a.changes[timeframe] - b.changes[timeframe],
    );

    const topGainers = sortedDesc.slice(0, TOP_LIMIT).map((item) => ({
      symbol: item.symbol,
      price: item.lastPrice,
      changePercent: item.changes[timeframe],
    }));

    const topLosers = sortedAsc.slice(0, TOP_LIMIT).map((item) => ({
      symbol: item.symbol,
      price: item.lastPrice,
      changePercent: item.changes[timeframe],
    }));

    return {
      timeframe,
      topGainers,
      topLosers,
    };
  }

  private async loadSymbolMetrics(): Promise<SymbolMetrics[]> {
    const now = Date.now();
    if (this.metricsCache && this.metricsCache.expiresAt > now) {
      return this.metricsCache.data;
    }

    const symbols = await this.getTradableSymbols();
    const results: SymbolMetrics[] = [];

    for (let i = 0; i < symbols.length; i += CONCURRENCY) {
      const chunk = symbols.slice(i, i + CONCURRENCY);
      const chunkResults = await Promise.all(
        chunk.map(async (symbol) => {
          try {
            return await this.fetchSymbolMetrics(symbol);
          } catch (error) {
            this.logger.warn(
              `Failed to fetch metrics for ${symbol}: ${(error as Error).message}`,
            );
            return null;
          }
        }),
      );

      for (const item of chunkResults) {
        if (item) {
          results.push(item);
        }
      }
    }

    this.metricsCache = {
      data: results,
      expiresAt: now + METRICS_CACHE_TTL,
    };

    return results;
  }

  private async getTradableSymbols(): Promise<string[]> {
    const now = Date.now();
    if (this.symbolsCache && this.symbolsCache.expiresAt > now) {
      return this.symbolsCache.data;
    }

    const response = await firstValueFrom(
      this.http.get<{ symbols: Array<Record<string, any>> }>('/fapi/v1/exchangeInfo'),
    );

    const symbols =
      response.data?.symbols
        ?.filter(
          (symbol) =>
            symbol.contractType === 'PERPETUAL' &&
            symbol.quoteAsset === 'USDT' &&
            symbol.status === 'TRADING',
        )
        .map((symbol) => symbol.symbol) ?? [];

    this.symbolsCache = {
      data: symbols,
      expiresAt: now + SYMBOL_CACHE_TTL,
    };

    return symbols;
  }

  private async fetchSymbolMetrics(symbol: string): Promise<SymbolMetrics | null> {
    const response = await firstValueFrom(
      this.http.get<any[]>('/fapi/v1/klines', {
        params: {
          symbol,
          interval: '1m',
          limit: KLINE_LIMIT,
        },
      }),
    );

    const candles = Array.isArray(response.data) ? response.data : [];
    if (candles.length < KLINE_LIMIT) {
      this.logger.debug(
        `Insufficient candle data for ${symbol}, expected ${KLINE_LIMIT}, received ${candles.length}`,
      );
      return null;
    }

    const latest = candles[candles.length - 1];
    const lastPrice = parseFloat(latest[4]);
    if (!Number.isFinite(lastPrice)) {
      return null;
    }

    const changes: Record<TimeframeKey, number> = {} as Record<
      TimeframeKey,
      number
    >;

    for (const [timeframe, minutes] of Object.entries(TIMEFRAME_MINUTES)) {
      const change = this.calculateChange(candles, minutes);
      if (change === null) {
        return null;
      }
      changes[timeframe as TimeframeKey] = change;
    }

    return {
      symbol,
      lastPrice,
      changes,
    };
  }

  private calculateChange(candles: any[], minutes: number): number | null {
    const latestIndex = candles.length - 1;
    const openIndex = latestIndex - minutes;
    if (openIndex < 0) {
      return null;
    }

    const open = parseFloat(candles[openIndex][1]);
    const close = parseFloat(candles[latestIndex][4]);
    if (!Number.isFinite(open) || !Number.isFinite(close) || open === 0) {
      return null;
    }

    return ((close - open) / open) * 100;
  }
}
