import { HttpService } from '@nestjs/axios';
import { Injectable, Logger } from '@nestjs/common';
import { AxiosError } from 'axios';
import { firstValueFrom } from 'rxjs';

const ONE_MINUTE_MS = 60_000;
const ONE_DAY_MINUTES = 24 * 60;
const KLINE_LIMIT = ONE_DAY_MINUTES;
const CONCURRENCY = 8;
const VOLUME_REFRESH_INTERVAL = 12 * 60 * 60 * 1000; // 12 hours
const REQUEST_INTERVAL_MS = 150;
const MAX_RETRY_ATTEMPTS = 5;
const RETRY_BACKOFF_BASE_MS = 500;
const MAX_RETRY_BACKOFF_MS = 4_000;

const MOVERS_TIMEFRAMES = [
  { minutes: 10, label: '10m' },
  { minutes: 30, label: '30m' },
  { minutes: 60, label: '1h' },
  { minutes: 120, label: '2h' },
];
const TOP_MOVERS = 10;
const FLOW_BUY_THRESHOLD = 0.62;
const FLOW_SELL_THRESHOLD = 0.38;

interface Candle {
  openTime: number;
  closeTime: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  quoteVolume: number;
  takerBuyQuoteVolume: number;
}

interface MoversSnapshot {
  timeframe: string;
  topGainers: MoversEntry[];
  topLosers: MoversEntry[];
}

interface MoversEntry {
  symbol: string;
  lastPrice: number;
  changePercent: number;
  flowRatio?: number;
  flowLabel?: string;
}

interface TimeframeMetric {
  changePercent: number;
  flowRatio?: number;
  flowLabel?: string;
}

@Injectable()
export class BinanceService {
  private readonly logger = new Logger(BinanceService.name);
  private topVolumeCache:
    | { symbols: string[]; total: number; expiresAt: number }
    | null = null;
  private throttleQueue: Promise<void> = Promise.resolve();
  private lastRequestTimestamp = 0;

  constructor(private readonly http: HttpService) {}

  async getTopMovers(): Promise<Record<string, MoversSnapshot>> {
    const volumeSelection = await this.getTopVolumeSymbols();
    if (volumeSelection.symbols.length === 0) {
      this.logger.warn('No symbols selected based on 24h quote volume.');
      return {};
    }

    this.logger.log(
      `Selected ${volumeSelection.symbols.length} symbols from ${volumeSelection.total} tradable contracts.`,
    );

    const metricsByTimeframe: Record<
      string,
      MoversEntry[]
    > = {};
    for (const { label } of MOVERS_TIMEFRAMES) {
      metricsByTimeframe[label] = [];
    }

    for (let i = 0; i < volumeSelection.symbols.length; i += CONCURRENCY) {
      const chunk = volumeSelection.symbols.slice(i, i + CONCURRENCY);
      const chunkResults = await Promise.all(
        chunk.map(async (symbol) => {
          try {
            const rawCandles = await this.fetchOneMinuteCandles(symbol);
            if (rawCandles.length === 0) {
              return null;
            }
            const metrics = this.calculateTimeframeMetrics(rawCandles);
            if (Object.keys(metrics).length === 0) {
              return null;
            }
            const lastPrice = rawCandles[rawCandles.length - 1].close;
            if (!Number.isFinite(lastPrice)) {
              return null;
            }
            return {
              symbol,
              lastPrice,
              metrics,
            };
          } catch (error) {
            this.logger.warn(
              `Failed to process symbol ${symbol}: ${(error as Error).message}`,
            );
            return null;
          }
        }),
      );

      for (const item of chunkResults) {
        if (!item) {
          continue;
        }
        for (const { label } of MOVERS_TIMEFRAMES) {
          const metric = item.metrics[label];
          if (!metric || !Number.isFinite(metric.changePercent)) {
            continue;
          }
          metricsByTimeframe[label].push({
            symbol: item.symbol,
            lastPrice: item.lastPrice,
            changePercent: metric.changePercent,
            flowRatio:
              metric.flowRatio !== undefined
                ? Math.round(metric.flowRatio * 100) / 100
                : undefined,
            flowLabel: metric.flowLabel,
          });
        }
      }
    }

    const result: Record<string, MoversSnapshot> = {};
    for (const { label } of MOVERS_TIMEFRAMES) {
      const entries = metricsByTimeframe[label];
      if (entries.length === 0) {
        result[label] = {
          timeframe: label,
          topGainers: [],
          topLosers: [],
        };
        continue;
      }

      const sortedAsc = [...entries].sort(
        (a, b) => a.changePercent - b.changePercent,
      );
      const sortedDesc = [...entries].sort(
        (a, b) => b.changePercent - a.changePercent,
      );

      result[label] = {
        timeframe: label,
        topGainers: sortedDesc.slice(0, TOP_MOVERS),
        topLosers: sortedAsc.slice(0, TOP_MOVERS),
      };
    }

    return result;
  }

  private async getTopVolumeSymbols(): Promise<{
    symbols: string[];
    total: number;
  }> {
    const now = Date.now();
    if (this.topVolumeCache && this.topVolumeCache.expiresAt > now) {
      return {
        symbols: this.topVolumeCache.symbols,
        total: this.topVolumeCache.total,
      };
    }

    const tradableSymbols = await this.getTradableSymbols();
    const total = tradableSymbols.length;
    if (total === 0) {
      this.topVolumeCache = {
        symbols: [],
        total,
        expiresAt: now + VOLUME_REFRESH_INTERVAL,
      };
      return { symbols: [], total };
    }

    const volumeBySymbol = await this.get24hQuoteVolumes();
    const rankedSymbols = tradableSymbols
      .map((symbol) => ({
        symbol,
        volume: volumeBySymbol[symbol] ?? 0,
      }))
      .sort((a, b) => b.volume - a.volume);

    const topCount = Math.max(1, Math.ceil(rankedSymbols.length / 2));
    const selectedSymbols = rankedSymbols
      .slice(0, topCount)
      .map((item) => item.symbol);

    this.topVolumeCache = {
      symbols: selectedSymbols,
      total,
      expiresAt: now + VOLUME_REFRESH_INTERVAL,
    };

    return {
      symbols: selectedSymbols,
      total,
    };
  }

  private async getTradableSymbols(): Promise<string[]> {
    const response = await this.requestWithRetry(
      () =>
        firstValueFrom(
          this.http.get<{ symbols: Array<Record<string, any>> }>(
            '/fapi/v1/exchangeInfo',
          ),
        ),
      'exchangeInfo',
    );

    return (
      response.data?.symbols
        ?.filter(
          (symbol) =>
            symbol.contractType === 'PERPETUAL' &&
            symbol.quoteAsset === 'USDT' &&
            symbol.status === 'TRADING',
        )
        .map((symbol) => symbol.symbol) ?? []
    );
  }

  private async get24hQuoteVolumes(): Promise<Record<string, number>> {
    const response = await this.requestWithRetry(
      () =>
        firstValueFrom(
          this.http.get<Array<Record<string, any>>>('/fapi/v1/ticker/24hr'),
        ),
      '24hrTicker',
    );
    const tickers = Array.isArray(response.data) ? response.data : [];
    const volumes: Record<string, number> = {};

    for (const ticker of tickers) {
      const symbol = ticker?.symbol;
      if (typeof symbol !== 'string') {
        continue;
      }

      const quoteVolume = parseFloat(ticker?.quoteVolume ?? '0');
      if (Number.isFinite(quoteVolume)) {
        volumes[symbol] = quoteVolume;
      }
    }

    return volumes;
  }

  private async fetchOneMinuteCandles(symbol: string): Promise<Candle[]> {
    const response = await this.requestWithRetry(
      () =>
        firstValueFrom(
          this.http.get<any[]>('/fapi/v1/klines', {
            params: {
              symbol,
              interval: '1m',
              limit: KLINE_LIMIT,
            },
          }),
        ),
      `klines:${symbol}`,
    );

    const rows = Array.isArray(response.data) ? response.data : [];
    const candles: Candle[] = [];

    for (const row of rows) {
      if (!Array.isArray(row) || row.length < 7) {
        continue;
      }

      const openTime = Number(row[0]);
      const open = parseFloat(row[1]);
      const high = parseFloat(row[2]);
      const low = parseFloat(row[3]);
      const close = parseFloat(row[4]);
      const volume = parseFloat(row[5]);
      const closeTime = Number(row[6]);
      const quoteVolume = parseFloat(row[7]);
      const takerBuyQuoteVolume = parseFloat(row[10]);

      if (
        !Number.isFinite(openTime) ||
        !Number.isFinite(closeTime) ||
        !Number.isFinite(open) ||
        !Number.isFinite(high) ||
        !Number.isFinite(low) ||
        !Number.isFinite(close) ||
        !Number.isFinite(volume) ||
        !Number.isFinite(quoteVolume) ||
        !Number.isFinite(takerBuyQuoteVolume)
      ) {
        continue;
      }

      candles.push({
        openTime,
        closeTime,
        open,
        high,
        low,
        close,
        volume,
        quoteVolume,
        takerBuyQuoteVolume,
      });
    }

    candles.sort((a, b) => a.openTime - b.openTime);
    return candles;
  }

  private calculateTimeframeMetrics(
    candles: Candle[],
  ): Record<string, TimeframeMetric> {
    if (candles.length === 0) {
      return {};
    }

    const latest = candles[candles.length - 1];
    if (!Number.isFinite(latest.close) || latest.close === 0) {
      return {};
    }

    const byOpenTime = new Map<number, Candle>();
    for (const candle of candles) {
      byOpenTime.set(candle.openTime, candle);
    }

    const metrics: Record<string, TimeframeMetric> = {};

    for (const { minutes, label } of MOVERS_TIMEFRAMES) {
      const targetOpenTime = latest.openTime - minutes * ONE_MINUTE_MS;
      const reference = byOpenTime.get(targetOpenTime);
      if (!reference || !Number.isFinite(reference.close) || reference.close === 0) {
        continue;
      }

      const windowCandles: Candle[] = [];
      let hasFullWindow = true;
      for (let i = 1; i <= minutes; i += 1) {
        const openTime = targetOpenTime + i * ONE_MINUTE_MS;
        const candle = byOpenTime.get(openTime);
        if (!candle) {
          hasFullWindow = false;
          break;
        }
        windowCandles.push(candle);
      }
      if (!hasFullWindow || windowCandles.length !== minutes) {
        continue;
      }

      const changePercent =
        ((latest.close - reference.close) / reference.close) * 100;

      if (!Number.isFinite(changePercent)) {
        continue;
      }

      const totalQuote = windowCandles.reduce(
        (sum, candle) => sum + candle.quoteVolume,
        0,
      );
      const totalTakerBuyQuote = windowCandles.reduce(
        (sum, candle) => sum + candle.takerBuyQuoteVolume,
        0,
      );

      let flowRatio: number | undefined;
      let flowLabel: string | undefined;

      if (totalQuote > 0 && Number.isFinite(totalTakerBuyQuote)) {
        flowRatio = totalTakerBuyQuote / totalQuote;
        flowLabel = this.classifyFlow(flowRatio);
      }

      metrics[label] = {
        changePercent,
        flowRatio: flowRatio !== undefined ? flowRatio * 100 : undefined,
        flowLabel,
      };
    }

    return metrics;
  }

  private classifyFlow(ratio: number): string {
    if (ratio >= FLOW_BUY_THRESHOLD) {
      return '主动买强🟢';
    }
    if (ratio <= FLOW_SELL_THRESHOLD) {
      return '主动卖强🔴';
    }
    return '均衡⚪';
  }

  private async requestWithRetry<T>(
    requestFactory: () => Promise<T>,
    label: string,
  ): Promise<T> {
    let attempt = 0;
    let delay = RETRY_BACKOFF_BASE_MS;
    let lastError: unknown;

    while (attempt < MAX_RETRY_ATTEMPTS) {
      attempt += 1;
      try {
        await this.throttle();
        return await requestFactory();
      } catch (error) {
        lastError = error;
        if (!this.shouldRetry(error)) {
          throw error;
        }

        this.logger.warn(
          `[${label}] attempt ${attempt} failed: ${(error as Error).message}`,
        );

        if (attempt >= MAX_RETRY_ATTEMPTS) {
          break;
        }

        await this.sleep(delay);
        delay = Math.min(delay * 2, MAX_RETRY_BACKOFF_MS);
      }
    }

    throw lastError instanceof Error
      ? lastError
      : new Error(`[${label}] request failed after retries.`);
  }

  private shouldRetry(error: unknown): boolean {
    if (error instanceof AxiosError) {
      const status = error.response?.status;
      if (status && status < 500 && status !== 429) {
        return false;
      }
    }
    return true;
  }

  private throttle(): Promise<void> {
    this.throttleQueue = this.throttleQueue.then(async () => {
      const now = Date.now();
      const elapsed = now - this.lastRequestTimestamp;
      if (elapsed < REQUEST_INTERVAL_MS) {
        await this.sleep(REQUEST_INTERVAL_MS - elapsed);
      }
      this.lastRequestTimestamp = Date.now();
    });
    return this.throttleQueue;
  }

  private async sleep(duration: number): Promise<void> {
    if (duration <= 0) {
      return;
    }
    await new Promise<void>((resolve) => setTimeout(resolve, duration));
  }
}
