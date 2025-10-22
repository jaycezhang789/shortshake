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

export const MOVERS_TIMEFRAMES = [
  { minutes: 10, label: '10m' },
  { minutes: 30, label: '30m' },
  { minutes: 60, label: '1h' },
  { minutes: 120, label: '2h' },
];
const TOP_MOVERS = 10;
const FLOW_BUY_THRESHOLD = 0.62;
const FLOW_SELL_THRESHOLD = 0.38;
const MAX_SELECTED_SYMBOLS = 80;
const CHG_SMALL = 0.01;
const SLIPPAGE_TARGET_QUOTE = 10_000;
const STATS_EPSILON = 1e-9;

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

export interface MoversSnapshot {
  timeframe: string;
  topGainers: MoversEntry[];
  topLosers: MoversEntry[];
}

export interface MoversEntry {
  symbol: string;
  lastPrice: number;
  changePercent: number;
  flowPercent?: number;
  flowLabel?: string;
  scores: {
    final: number;
    core: number;
    confirm: number;
    liquidityPenalty: number;
    efficiency: number;
    chop: number;
    momentumAtr: number;
    align: number;
    gate: number;
    volumeBoost: number;
    flowBoost: number;
  };
}

interface SymbolTimeframeMetric {
  changePercent: number;
  netChange: number;
  efficiency: number;
  chop: number;
  momentumAtr: number;
  smallMoveGate: number;
  totalQuoteVolume: number;
  flowRatio?: number;
  flowLabel?: string;
  align?: number;
  volumeBoost?: number;
  flowBoost?: number;
  confirmScore?: number;
  coreScore?: number;
  finalScore?: number;
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

    const symbolDataList: Array<{
      symbol: string;
      lastPrice: number;
      metrics: Record<string, SymbolTimeframeMetric>;
      liquidityPenalty: number;
    }> = [];

    for (let i = 0; i < volumeSelection.symbols.length; i += CONCURRENCY) {
      const chunk = volumeSelection.symbols.slice(i, i + CONCURRENCY);
      const chunkResults = await Promise.all(
        chunk.map(async (symbol) => {
          try {
            const [rawCandles, liquidityMetrics] = await Promise.all([
              this.fetchOneMinuteCandles(symbol),
              this.fetchLiquidityMetrics(symbol),
            ]);
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
              liquidityPenalty: liquidityMetrics?.penalty ?? 0,
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
        symbolDataList.push(item);
      }
    }

    if (symbolDataList.length === 0) {
      return {};
    }

    const volumeCollections: Record<string, number[]> = {};
    for (const { label } of MOVERS_TIMEFRAMES) {
      volumeCollections[label] = [];
    }

    for (const { metrics } of symbolDataList) {
      for (const { label } of MOVERS_TIMEFRAMES) {
        const metric = metrics[label];
        if (!metric) {
          continue;
        }
        volumeCollections[label].push(metric.totalQuoteVolume);
      }
    }

    const volumeStats = this.computeVolumeStats(volumeCollections);

    for (const symbolData of symbolDataList) {
      this.applyAlignment(symbolData.metrics);
    }

    const metricsByTimeframe: Record<string, MoversEntry[]> = {};
    for (const { label } of MOVERS_TIMEFRAMES) {
      metricsByTimeframe[label] = [];
    }

    for (const symbolData of symbolDataList) {
      const liqPenalty = this.clamp(symbolData.liquidityPenalty, 0, 1);

      for (const { label } of MOVERS_TIMEFRAMES) {
        const metric = symbolData.metrics[label];
        if (!metric || !Number.isFinite(metric.changePercent)) {
          continue;
        }

        const volStats = volumeStats[label];
        const volZ =
          volStats.std > STATS_EPSILON
            ? (metric.totalQuoteVolume - volStats.mean) / volStats.std
            : 0;
        const volBoost = this.sigmoid(this.clamp(volZ, -3, 3));
        const flowBoost =
          metric.flowRatio !== undefined
            ? this.clamp((metric.flowRatio - 0.5) / 0.12 + 0.5, 0, 1)
            : 0.5;

        const align = metric.align ?? 0.5;
        const coreComponents = [
          metric.efficiency,
          1 - metric.chop,
          metric.momentumAtr,
          align,
        ];
        const coreScore = metric.smallMoveGate * this.average(coreComponents);
        const confirmScore = this.average([volBoost, flowBoost]);
        const finalScore = this.clamp(
          coreScore * 0.7 + confirmScore * 0.3 - liqPenalty,
          0,
          1,
        );

        metric.volumeBoost = volBoost;
        metric.flowBoost = flowBoost;
        metric.align = align;
        metric.coreScore = coreScore;
        metric.confirmScore = confirmScore;
        metric.finalScore = finalScore;

        metricsByTimeframe[label].push({
          symbol: symbolData.symbol,
          lastPrice: symbolData.lastPrice,
          changePercent: metric.changePercent,
          flowPercent:
            metric.flowRatio !== undefined
              ? Math.round(metric.flowRatio * 10_000) / 100
              : undefined,
          flowLabel: metric.flowLabel,
          scores: {
            final: finalScore,
            core: coreScore,
            confirm: confirmScore,
            liquidityPenalty: liqPenalty,
            efficiency: metric.efficiency,
            chop: metric.chop,
            momentumAtr: metric.momentumAtr,
            align,
            gate: metric.smallMoveGate,
            volumeBoost: volBoost,
            flowBoost,
          },
        });
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

    const topCount = Math.min(
      MAX_SELECTED_SYMBOLS,
      Math.max(1, Math.ceil(rankedSymbols.length / 2)),
    );
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
  ): Record<string, SymbolTimeframeMetric> {
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

    const metrics: Record<string, SymbolTimeframeMetric> = {};

    for (const { minutes, label } of MOVERS_TIMEFRAMES) {
      const targetOpenTime = latest.openTime - minutes * ONE_MINUTE_MS;
      const reference = byOpenTime.get(targetOpenTime);
      if (!reference || !Number.isFinite(reference.close) || reference.close === 0) {
        continue;
      }

      const windowCandles = candles.filter(
        (candle) =>
          candle.openTime > targetOpenTime && candle.openTime <= latest.openTime,
      );

      if (windowCandles.length < minutes) {
        continue;
      }

      const first = windowCandles[0];
      const last = windowCandles[windowCandles.length - 1];

      const netChange = (last.close - first.open) / first.open;
      if (!Number.isFinite(netChange)) {
        continue;
      }

      let sumLog = 0;
      let sumAbsLog = 0;
      let incrementalSum = 0;
      let waste = 0;
      let prevClose = first.open;
      let trSum = 0;

      for (const candle of windowCandles) {
        const logReturn = Math.log(candle.close / candle.open);
        if (Number.isFinite(logReturn)) {
          sumLog += logReturn;
          sumAbsLog += Math.abs(logReturn);
        }

        const incremental = (candle.close - candle.open) / candle.open;
        if (Number.isFinite(incremental)) {
          incrementalSum += incremental;
        }

        const highLow = candle.high - candle.low;
        const highPrevClose = Math.abs(candle.high - prevClose);
        const lowPrevClose = Math.abs(candle.low - prevClose);
        const tr = Math.max(highLow, highPrevClose, lowPrevClose);
        if (Number.isFinite(tr)) {
          trSum += tr;
        }

        prevClose = candle.close;
      }

      const efficiency =
        sumAbsLog > 0 ? this.clamp(Math.abs(sumLog) / sumAbsLog, 0, 1) : 0;

      const net = netChange;
      const wasteRaw = incrementalSum - net;
      waste = wasteRaw > 0 ? wasteRaw : 0;
      const chop =
        waste <= 0 && Math.abs(net) <= STATS_EPSILON
          ? 0
          : this.clamp(waste / (waste + Math.abs(net)), 0, 1);

      const atr = windowCandles.length > 0 ? trSum / windowCandles.length : 0;
      const atrPct = last.close !== 0 ? atr / last.close : 0;
      const momentumAtr =
        atrPct > 0
          ? this.clamp(Math.abs(net) / (atrPct * 2), 0, 1)
          : 0;

      const gate = this.clamp(Math.abs(net) / (3 * CHG_SMALL), 0, 1);

      const totalQuoteVolume = windowCandles.reduce(
        (sum, candle) => sum + candle.quoteVolume,
        0,
      );
      const totalTakerBuyQuote = windowCandles.reduce(
        (sum, candle) => sum + candle.takerBuyQuoteVolume,
        0,
      );

      let flowRatio: number | undefined;
      let flowLabel: string | undefined;

      if (totalQuoteVolume > 0 && Number.isFinite(totalTakerBuyQuote)) {
        flowRatio = totalTakerBuyQuote / totalQuoteVolume;
        flowLabel = this.classifyFlow(flowRatio);
      }

      metrics[label] = {
        changePercent: net * 100,
        netChange: net,
        efficiency,
        chop,
        momentumAtr,
        smallMoveGate: gate,
        totalQuoteVolume,
        flowRatio,
        flowLabel,
      };
    }

    return metrics;
  }

  private applyAlignment(
    metrics: Record<string, SymbolTimeframeMetric>,
  ): void {
    const entries = Object.entries(metrics);
    for (const [label, metric] of entries) {
      if (!Number.isFinite(metric.changePercent)) {
        metric.align = 0.5;
        continue;
      }

      const baseDirection = Math.sign(metric.changePercent);
      if (baseDirection === 0) {
        metric.align = 0.5;
        continue;
      }

      let scoreSum = 0;
      let comparisons = 0;

      for (const [otherLabel, otherMetric] of entries) {
        if (otherLabel === label) {
          continue;
        }
        if (!Number.isFinite(otherMetric.changePercent)) {
          continue;
        }
        const otherDirection = Math.sign(otherMetric.changePercent);
        if (otherDirection === 0) {
          continue;
        }

        comparisons += 1;
        scoreSum += otherDirection === baseDirection ? 1 : -0.5;
      }

      if (comparisons === 0) {
        metric.align = 0.5;
        continue;
      }

      const normalized =
        (scoreSum + 0.5 * comparisons) / (1.5 * comparisons);
      metric.align = this.clamp(normalized, 0, 1);
    }
  }

  private computeVolumeStats(
    collections: Record<string, number[]>,
  ): Record<string, { mean: number; std: number }> {
    const stats: Record<string, { mean: number; std: number }> = {};

    for (const [label, values] of Object.entries(collections)) {
      if (!values || values.length === 0) {
        stats[label] = { mean: 0, std: 1 };
        continue;
      }
      const mean =
        values.reduce((sum, value) => sum + value, 0) / values.length;
      const variance =
        values.reduce((sum, value) => sum + (value - mean) ** 2, 0) /
        Math.max(values.length - 1, 1);
      const std = Math.sqrt(variance);
      stats[label] = {
        mean,
        std: std > STATS_EPSILON ? std : 1,
      };
    }

    return stats;
  }

  private async fetchLiquidityMetrics(
    symbol: string,
  ): Promise<{ spreadBps: number; slippageBps: number; penalty: number } | null> {
    try {
      const [tickerResponse, depthResponse] = await Promise.all([
        this.requestWithRetry(
          () =>
            firstValueFrom(
              this.http.get<{ bidPrice: string; askPrice: string }>(
                '/fapi/v1/ticker/bookTicker',
                { params: { symbol } },
              ),
            ),
          `bookTicker:${symbol}`,
        ),
        this.requestWithRetry(
          () =>
            firstValueFrom(
              this.http.get<{ bids: string[][]; asks: string[][] }>(
                '/fapi/v1/depth',
                { params: { symbol, limit: 200 } },
              ),
            ),
          `depth:${symbol}`,
        ),
      ]);

      const ticker = tickerResponse.data;
      const depth = depthResponse.data;

      const bestBid = parseFloat(ticker?.bidPrice ?? '0');
      const bestAsk = parseFloat(ticker?.askPrice ?? '0');
      if (
        !Number.isFinite(bestBid) ||
        !Number.isFinite(bestAsk) ||
        bestBid <= 0 ||
        bestAsk <= 0 ||
        bestAsk <= bestBid
      ) {
        return null;
      }

      const mid = (bestBid + bestAsk) / 2;
      if (!Number.isFinite(mid) || mid <= 0) {
        return null;
      }

      const spreadBps = this.clamp(
        ((bestAsk - bestBid) / mid) * 10_000,
        0,
        Number.POSITIVE_INFINITY,
      );

      const slippageBps = this.calculateSlippageBps(depth, mid);
      if (!Number.isFinite(slippageBps)) {
        return {
          spreadBps,
          slippageBps: spreadBps,
          penalty: this.clamp((spreadBps / 10) * 0.6 + 0.4, 0, 1),
        };
      }

      const penalty =
        this.clamp(spreadBps / 10, 0, 1) * 0.6 +
        this.clamp(slippageBps / 20, 0, 1) * 0.4;

      return {
        spreadBps,
        slippageBps,
        penalty: this.clamp(penalty, 0, 1),
      };
    } catch (error) {
      this.logger.debug(
        `Liquidity fetch failed for ${symbol}: ${(error as Error).message}`,
      );
      return null;
    }
  }

  private calculateSlippageBps(
    depth: { bids?: string[][]; asks?: string[][] } | undefined,
    mid: number,
  ): number {
    const asks = Array.isArray(depth?.asks) ? depth?.asks : [];
    const bids = Array.isArray(depth?.bids) ? depth?.bids : [];

    const buyAverage = this.computeAverageFillPrice(asks, SLIPPAGE_TARGET_QUOTE);
    const sellAverage = this.computeAverageFillPrice(
      bids,
      SLIPPAGE_TARGET_QUOTE,
    );

    const slips: number[] = [];

    if (buyAverage !== null) {
      slips.push(
        this.clamp(((buyAverage - mid) / mid) * 10_000, 0, Number.POSITIVE_INFINITY),
      );
    }

    if (sellAverage !== null) {
      slips.push(
        this.clamp(((mid - sellAverage) / mid) * 10_000, 0, Number.POSITIVE_INFINITY),
      );
    }

    if (slips.length === 0) {
      return Number.NaN;
    }

    return Math.max(...slips);
  }

  private computeAverageFillPrice(
    levels: string[][],
    targetQuote: number,
  ): number | null {
    if (!Array.isArray(levels) || levels.length === 0 || targetQuote <= 0) {
      return null;
    }

    let remainingQuote = targetQuote;
    let accumulatedQuote = 0;
    let accumulatedBase = 0;

    for (const level of levels) {
      if (!Array.isArray(level) || level.length < 2) {
        continue;
      }
      const price = parseFloat(level[0] ?? '0');
      const quantity = parseFloat(level[1] ?? '0');
      if (
        !Number.isFinite(price) ||
        !Number.isFinite(quantity) ||
        price <= 0 ||
        quantity <= 0
      ) {
        continue;
      }

      const levelQuote = price * quantity;
      const usedQuote = Math.min(levelQuote, remainingQuote);
      const usedBase = usedQuote / price;

      accumulatedQuote += usedQuote;
      accumulatedBase += usedBase;
      remainingQuote -= usedQuote;

      if (remainingQuote <= 0) {
        break;
      }
    }

    if (accumulatedBase <= 0 || remainingQuote > targetQuote * 0.05) {
      return null;
    }

    return accumulatedQuote / accumulatedBase;
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

  private clamp(value: number, min: number, max: number): number {
    if (Number.isNaN(value)) {
      return min;
    }
    if (value < min) {
      return min;
    }
    if (value > max) {
      return max;
    }
    return value;
  }

  private sigmoid(value: number): number {
    return 1 / (1 + Math.exp(-value));
  }

  private average(values: number[]): number {
    const filtered = values.filter((value) => Number.isFinite(value));
    if (filtered.length === 0) {
      return 0;
    }
    return filtered.reduce((sum, value) => sum + value, 0) / filtered.length;
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
