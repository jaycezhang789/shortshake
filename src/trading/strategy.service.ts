import { Injectable, Logger } from '@nestjs/common';
import {
  AggregatedMoversEntry,
  MoversResult,
  SymbolTimeframeMetric,
} from '../binance/binance.service';
import { TradingService } from './trading.service';

type PositionDirection = 'LONG' | 'SHORT';

interface ManagedPositionState {
  symbol: string;
  direction: PositionDirection;
  parentTimeframe: string;
  childTimeframe: string;
  entryPrice: number;
  quantity: number;
  kSl: number;
  slDistance: number;
  stopPrice: number;
  trailAtrMultiple: number;
  cleanScore: number;
  gateScore: number;
  openedAt: number;
  addCount: number;
  beMoved: boolean;
}

@Injectable()
export class StrategyService {
  private readonly logger = new Logger(StrategyService.name);
  private readonly managedPositions = new Map<string, ManagedPositionState>();

  constructor(private readonly tradingService: TradingService) {}

  async process(result: MoversResult): Promise<void> {
    if (!this.tradingService.isEnabled()) {
      return;
    }

    this.syncPositionStates();
    await this.tradingService.flattenResidualPositions();

    for (const candidate of result.aggregatedTop) {
      if (this.tradingService.getRemainingSlots() <= 0) {
        break;
      }
      try {
        await this.evaluateCandidate(candidate);
      } catch (error) {
        this.logger.error(
          `Failed to evaluate ${candidate.symbol}: ${(error as Error).message}`,
        );
      }
    }

    // Advanced position management (trail, partials, etc.) can extend here.
  }

  private syncPositionStates(): void {
    const activeSymbols = new Set(this.tradingService.getActiveSymbols());
    for (const symbol of Array.from(this.managedPositions.keys())) {
      if (!activeSymbols.has(symbol)) {
        this.managedPositions.delete(symbol);
      }
    }
  }

  private async evaluateCandidate(
    candidate: AggregatedMoversEntry,
  ): Promise<void> {
    const { parentTimeframe, childTimeframe } = this.selectFramework(
      candidate.metrics,
    );
    if (!parentTimeframe || !childTimeframe) {
      return;
    }

    const parentMetric = candidate.metrics[parentTimeframe];
    const childMetric = candidate.metrics[childTimeframe];
    if (!parentMetric || !childMetric) {
      return;
    }

    const direction = this.determineDirection(parentMetric);
    if (!direction) {
      return;
    }

    if (
      this.managedPositions.has(candidate.symbol) ||
      !this.tradingService.canOpenPosition(candidate.symbol)
    ) {
      return;
    }

    const parentScores = this.computeScores(parentMetric);
    const childScores = this.computeScores(childMetric);

    if (parentScores.efficiency < 45 || parentScores.align < 50) {
      return;
    }

    const liquidityPenalty =
      (candidate.entry.scores.liquidityPenalty ?? 0) * 100;
    if (liquidityPenalty >= 40) {
      this.logger.debug(
        `Skip ${candidate.symbol} due to high liquidity penalty (${liquidityPenalty.toFixed(
          1,
        )}).`,
      );
      return;
    }

    if (!this.triggersSatisfied(direction, childMetric, childScores)) {
      return;
    }

    const cleanParent =
      (Math.abs(parentScores.trend) +
        parentScores.efficiency +
        parentScores.align) /
      300;
    const gateChild = childMetric.smallMoveGate ?? 0;

    const kSl = this.clamp(1.2 + 0.9 * cleanParent + 0.3 * gateChild, 1.2, 2.8);
    const atrChild = childMetric.atrValue || 0;
    if (atrChild <= 0) {
      return;
    }

    const sizeScale = this.computeSizeScale(liquidityPenalty);

    const orderResponse = await this.tradingService.createMarketOrder(
      candidate.symbol,
      direction,
      { sizeScale },
    );
    if (!orderResponse) {
      return;
    }

    const quantity = this.extractNumber(
      orderResponse.executedQty ?? orderResponse.origQty,
    );
    const avgPrice =
      this.extractNumber(orderResponse.avgPrice ?? orderResponse.price) ||
      candidate.entry.lastPrice;

    if (!quantity || !avgPrice) {
      this.logger.warn(
        `Unable to compute entry details for ${candidate.symbol}, skipping stop placement.`,
      );
      return;
    }

    const slDistance = kSl * atrChild;
    let stopPrice =
      direction === 'LONG' ? avgPrice - slDistance : avgPrice + slDistance;
    stopPrice = Math.max(stopPrice, 0.0001);

    await this.tradingService.placeStopLoss(
      candidate.symbol,
      direction,
      quantity,
      stopPrice,
    );

    const trailAtr = this.computeTrailAtr(cleanParent, gateChild);

    this.managedPositions.set(candidate.symbol, {
      symbol: candidate.symbol,
      direction,
      parentTimeframe,
      childTimeframe,
      entryPrice: avgPrice,
      quantity,
      kSl,
      slDistance,
      stopPrice,
      trailAtrMultiple: trailAtr,
      cleanScore: cleanParent,
      gateScore: gateChild,
      openedAt: Date.now(),
      addCount: 0,
      beMoved: false,
    });

    this.logger.log(
      `Opened ${direction} on ${candidate.symbol} @ ${avgPrice.toFixed(
        4,
      )}, SL ${stopPrice.toFixed(4)} (kSL=${kSl.toFixed(2)}, ATR=${atrChild.toFixed(
        4,
      )}).`,
    );
  }

  private selectFramework(metrics: Record<string, SymbolTimeframeMetric>): {
    parentTimeframe: string | null;
    childTimeframe: string | null;
  } {
    const metric1h = metrics['1h'];
    const metric30m = metrics['30m'];
    const metric10m = metrics['10m'];

    const parentVia1h =
      metric1h &&
      this.signedTrend(metric1h) >= 70 &&
      this.computeScores(metric1h).efficiency >= 55;

    let parentTimeframe: string | null = null;
    let childTimeframe: string | null = null;

    if (parentVia1h && metric30m) {
      parentTimeframe = '1h';
      childTimeframe = '30m';
    } else if (metric30m && metric10m) {
      parentTimeframe = '30m';
      childTimeframe = '10m';
    } else if (metric1h && metric30m) {
      parentTimeframe = '1h';
      childTimeframe = '30m';
    }

    return { parentTimeframe, childTimeframe };
  }

  private determineDirection(
    metric: SymbolTimeframeMetric,
  ): PositionDirection | null {
    const trend = this.signedTrend(metric);
    const alignScore = this.computeScores(metric).align;
    const directionSign = Math.sign(metric.netChange ?? 0);

    if (trend >= 65 && alignScore >= 60 && directionSign >= 0) {
      return 'LONG';
    }

    if (trend <= -65 && alignScore >= 60 && directionSign <= 0) {
      return 'SHORT';
    }

    return null;
  }

  private triggersSatisfied(
    direction: PositionDirection,
    metric: SymbolTimeframeMetric,
    scores: { efficiency: number; volume: number; flow: number },
  ): boolean {
    const gate = metric.smallMoveGate ?? 0;
    const momentumSign = Math.sign(metric.netChange ?? 0);
    const momentumMagnitude = metric.momentumAtr ?? 0;

    const momentumTrigger =
      gate >= 0.65 &&
      momentumMagnitude >= 0.5 &&
      ((direction === 'LONG' && momentumSign >= 0) ||
        (direction === 'SHORT' && momentumSign <= 0));

    const efficiencyTrigger =
      scores.efficiency >= 55 &&
      (scores.volume >= 55 || scores.flow >= 55);

    return momentumTrigger || efficiencyTrigger;
  }

  private computeTrailAtr(cleanP: number, gateC: number): number {
    const base =
      2.0 + 1.2 * cleanP - 0.6 * (1 - gateC);
    return this.clamp(base, 1.6, 3.2);
  }

  private computeSizeScale(liquidityPenalty: number): number {
    const normalized = this.clamp((100 - liquidityPenalty) / 100, 0, 1);
    return this.clamp(normalized ** 2, 0.2, 1);
  }

  private computeScores(metric: SymbolTimeframeMetric): {
    trend: number;
    efficiency: number;
    align: number;
    volume: number;
    flow: number;
  } {
    return {
      trend: Math.abs(this.signedTrend(metric)),
      efficiency: (metric.efficiency ?? 0) * 100,
      align: (metric.align ?? 0.5) * 100,
      volume: (metric.volumeBoost ?? 0.5) * 100,
      flow:
        (metric.flowBoost ??
          metric.activeFlow ??
          metric.flowImmediateBase ??
          0.5) * 100,
    };
  }

  private signedTrend(metric: SymbolTimeframeMetric): number {
    const direction = Math.sign(metric.netChange ?? 0);
    const trendStrength = (1 - (metric.chop ?? 1)) * 100;
    return trendStrength * direction;
  }

  private extractNumber(value: unknown): number {
    const num = Number(value);
    return Number.isFinite(num) ? num : 0;
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
}
