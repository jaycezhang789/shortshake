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
const FLOW_SIGMA_K = 0.2;
const MAX_SELECTED_SYMBOLS = 80;
const CHG_SMALL = 0.01;
const STATS_EPSILON = 1e-9;
const FINAL_CORE_WEIGHT = 0.67;

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
  changes: Record<string, number>;
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
    mtfConsistency: number;
    gate: number;
    volumeBoost: number;
    flowActive: number;
    flowPersistence: number;
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
  flowImmediateBase?: number;
  flowPersistence?: number;
  mtfConsistency?: number;
  activeFlow?: number;
  confirmScore?: number;
  coreScore?: number;
  finalScore?: number;
  atrPct: number;
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
    this.logger.log('Starting top movers computation.');
    const volumeSelection = await this.getTopVolumeSymbols();
    if (volumeSelection.symbols.length === 0) {
      this.logger.warn('No symbols selected based on 24h quote volume.');
      return {};
    }

    this.logger.log(
      `Selected ${volumeSelection.symbols.length} symbols from ${volumeSelection.total} tradable contracts.`,
    );
    this.logger.log(
      `Processing symbols in batches of ${CONCURRENCY}.`,
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

      const processedCount = symbolDataList.length;
      this.logger.log(
        `Processed ${processedCount}/${volumeSelection.symbols.length} symbols so far.`,
      );
    }

    if (symbolDataList.length === 0) {
      this.logger.warn('No symbol data collected after batch processing.');
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

    this.logger.log('Calculating volume statistics for each timeframe.');
    const volumeStats = this.computeVolumeStats(volumeCollections);

    for (const symbolData of symbolDataList) {
      this.applyAlignment(symbolData.metrics);
      this.applyMultiTimeframeConsistency(symbolData.metrics);
    }

    const metricsByTimeframe: Record<string, MoversEntry[]> = {};
    const changeMaps: Record<string, Record<string, number>> = {};
    for (const { label } of MOVERS_TIMEFRAMES) {
      metricsByTimeframe[label] = [];
      changeMaps[label] = {};
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
        const gVol = this.computeVolumeScaler(volZ);
        const activeFlow = this.clamp(
          (metric.flowImmediateBase ?? 0.5) * gVol,
          0,
          1,
        );
        const flowPersistence = this.clamp(metric.flowPersistence ?? 0, 0, 1);
        const mtfConsistency = this.clamp(metric.mtfConsistency ?? 0.5, 0, 1);
        const align = this.clamp(metric.align ?? 0.5, 0, 1);

        const coreScore =
          metric.smallMoveGate *
          this.weightedAverage([
            { value: metric.efficiency, weight: 1 },
            { value: 1 - metric.chop, weight: 1 },
            { value: metric.momentumAtr, weight: 1 },
            { value: align, weight: 1 },
            { value: mtfConsistency, weight: 0.8 },
          ]);

        const confirmScore = this.weightedAverage([
          { value: volBoost, weight: 0.5 },
          { value: activeFlow, weight: 0.3 },
          { value: flowPersistence, weight: 0.2 },
        ]);

        const finalScore = this.clamp(
          FINAL_CORE_WEIGHT * coreScore +
            (1 - FINAL_CORE_WEIGHT) * confirmScore -
            liqPenalty,
          0,
          1,
        );

        metric.volumeBoost = volBoost;
        metric.activeFlow = activeFlow;
        metric.flowPersistence = flowPersistence;
        metric.mtfConsistency = mtfConsistency;
        metric.align = align;
        metric.coreScore = coreScore;
        metric.confirmScore = confirmScore;
        metric.finalScore = finalScore;

        changeMaps[label][symbolData.symbol] = metric.changePercent;

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
            mtfConsistency,
            gate: metric.smallMoveGate,
            volumeBoost: volBoost,
            flowActive: activeFlow,
            flowPersistence,
          },
        });
      }
    }

    const result: Record<string, MoversSnapshot> = {};
    for (const { label } of MOVERS_TIMEFRAMES) {
      const entries = metricsByTimeframe[label];
      const changes = changeMaps[label] ?? {};
      if (entries.length === 0) {
        result[label] = {
          timeframe: label,
          topGainers: [],
          topLosers: [],
          changes,
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
        changes,
      };
      this.logger.log(
        `Computed movers snapshot for timeframe ${label} (gainers: ${result[label].topGainers.length}, losers: ${result[label].topLosers.length}).`,
      );
    }

    this.logger.log('Top movers computation completed.');
    return result;
  }

  private async getTopVolumeSymbols(): Promise<{
    symbols: string[];
    total: number;
  }> {
    const now = Date.now();
    if (this.topVolumeCache && this.topVolumeCache.expiresAt > now) {
      this.logger.log(
        'Using cached 24h volume selection for symbol universe.',
      );
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

    this.logger.log(
      `Refreshed 24h volume selection -> selected ${selectedSymbols.length}/${total} symbols.`,
    );

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
      const flowSeries: number[] = [];
      const returnSeries: number[] = [];

      for (const candle of windowCandles) {
        const logReturn = Math.log(candle.close / candle.open);
        if (Number.isFinite(logReturn)) {
          sumLog += logReturn;
          sumAbsLog += Math.abs(logReturn);
        }

        const incremental = (candle.close - candle.open) / candle.open;
        if (Number.isFinite(incremental)) {
          incrementalSum += incremental;
          returnSeries.push(incremental);
        } else {
          returnSeries.push(0);
        }

        const highLow = candle.high - candle.low;
        const highPrevClose = Math.abs(candle.high - prevClose);
        const lowPrevClose = Math.abs(candle.low - prevClose);
        const tr = Math.max(highLow, highPrevClose, lowPrevClose);
        if (Number.isFinite(tr)) {
          trSum += tr;
        }

        let flowMinute = 0.5;
        if (
          Number.isFinite(candle.takerBuyQuoteVolume) &&
          Number.isFinite(candle.quoteVolume) &&
          candle.quoteVolume > 0
        ) {
          flowMinute = this.clamp(
            candle.takerBuyQuoteVolume / candle.quoteVolume,
            0,
            1,
          );
        }
        flowSeries.push(flowMinute);

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
      const flowRatio =
        totalQuoteVolume > 0 ? totalTakerBuyQuote / totalQuoteVolume : undefined;
      const flowLabel =
        flowRatio !== undefined ? this.classifyFlow(flowRatio) : undefined;
      const flowImmediateBase =
        flowRatio !== undefined
          ? this.squashedTanh((flowRatio - 0.5) / FLOW_SIGMA_K)
          : 0.5;
      const flowPersistence = this.computeFlowPersistence(flowSeries, returnSeries);

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
        atrPct,
        flowImmediateBase,
        flowPersistence,
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

  private applyMultiTimeframeConsistency(
    metrics: Record<string, SymbolTimeframeMetric>,
  ): void {
    const weightMap: Record<string, number> = {
      '10m': 1,
      '30m': 1,
      '1h': 1.5,
      '2h': 1.5,
    };

    for (const { label } of MOVERS_TIMEFRAMES) {
      const currentMetric = metrics[label];
      if (!currentMetric) {
        continue;
      }

      let agreeWeighted = 0;
      let weightTotal = 0;
      const magnitudes: number[] = [];
      const baseSign = Math.sign(currentMetric.netChange);

      for (const { label: otherLabel } of MOVERS_TIMEFRAMES) {
        if (otherLabel === label) {
          continue;
        }
        const otherMetric = metrics[otherLabel];
        if (!otherMetric) {
          continue;
        }
        const weight = weightMap[otherLabel] ?? 1;
        weightTotal += weight;

        const otherSign = Math.sign(otherMetric.netChange);
        if (baseSign !== 0 && baseSign === otherSign) {
          agreeWeighted += weight;
        }
        magnitudes.push(this.clamp(otherMetric.momentumAtr ?? 0, 0, 1));
      }

      const agree =
        weightTotal > 0 ? this.clamp(agreeWeighted / weightTotal, 0, 1) : 0.5;
      const mag =
        magnitudes.length > 0
          ? this.clamp(
              magnitudes.reduce((sum, value) => sum + value, 0) /
                magnitudes.length,
              0,
              1,
            )
          : 0.5;

      currentMetric.mtfConsistency = this.clamp(agree * mag, 0, 1);
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
    return null;
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

  private weightedAverage(
    entries: Array<{ value: number | undefined; weight: number }>,
  ): number {
    let weightedSum = 0;
    let weightTotal = 0;
    for (const { value, weight } of entries) {
      if (!Number.isFinite(value ?? NaN) || weight <= 0) {
        continue;
      }
      weightedSum += (value ?? 0) * weight;
      weightTotal += weight;
    }
    if (weightTotal <= 0) {
      return 0;
    }
    return this.clamp(weightedSum / weightTotal, 0, 1);
  }

  private computeVolumeScaler(zScore: number): number {
    const capped = this.clamp(zScore, 0, 3);
    return this.clamp(capped / 3, 0, 1);
  }

  private squashedTanh(value: number): number {
    return (Math.tanh(value) + 1) / 2;
  }

  private computeFlowPersistence(
    flowRatios: number[],
    returns: number[],
  ): number {
    const length = Math.min(flowRatios.length, returns.length);
    if (length === 0) {
      return 0;
    }

    const flowSlice = flowRatios.slice(-length);
    const returnSlice = returns.slice(-length);
    const flowZ = this.zNormalize(flowSlice);
    const returnZ = this.zNormalize(returnSlice);
    const corr = this.correlation(flowZ, returnZ);

    let agreeCount = 0;
    for (let i = 0; i < length; i += 1) {
      const flowSign = Math.sign(flowSlice[i] - 0.5);
      const returnSign = Math.sign(returnSlice[i]);
      if (flowSign === 0 || returnSign === 0) {
        continue;
      }
      if (flowSign === returnSign) {
        agreeCount += 1;
      }
    }

    const agreeRatio = length > 0 ? agreeCount / length : 0;
    const persistence = ((corr + 1) / 2) * agreeRatio;
    return this.clamp(persistence, 0, 1);
  }

  private zNormalize(series: number[]): number[] {
    if (series.length === 0) {
      return [];
    }
    const mean = series.reduce((sum, value) => sum + value, 0) / series.length;
    const variance =
      series.reduce((sum, value) => sum + (value - mean) ** 2, 0) /
      Math.max(series.length - 1, 1);
    const std = Math.sqrt(variance);
    if (std <= STATS_EPSILON) {
      return series.map(() => 0);
    }
    return series.map((value) => (value - mean) / std);
  }

  private correlation(a: number[], b: number[]): number {
    const length = Math.min(a.length, b.length);
    if (length === 0) {
      return 0;
    }
    let sum = 0;
    for (let i = 0; i < length; i += 1) {
      sum += a[i] * b[i];
    }
    return this.clamp(sum / length, -1, 1);
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
