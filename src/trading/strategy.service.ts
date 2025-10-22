import { Injectable, Logger } from '@nestjs/common';
import {
  AggregatedMoversEntry,
  MoversResult,
  SymbolTimeframeMetric,
} from '../binance/binance.service';
import {
  TradingService,
  PositionSummary,
  PriceTick,
} from './trading.service';

type PositionDirection = 'LONG' | 'SHORT';

interface ManagedPositionState {
  symbol: string;
  direction: PositionDirection;
  parentTimeframe: string;
  childTimeframe: string;
  entryPrice: number;
  baseQuantity: number;
  totalQuantity: number;
  kSl: number;
  slDistance: number;
  initialSlDistance: number;
  stopPrice: number;
  trailAtrMultiple: number;
  cleanScore: number;
  gateScore: number;
  openedAt: number;
  addCount: number;
  beMoved: boolean;
  highestPrice?: number;
  lowestPrice?: number;
  trailPrice?: number;
  partialOneTaken: boolean;
  partialTwoTaken: boolean;
  timeStopStage: number;
  timeStopTimestamp?: number;
  structureBreakCounter: number;
  parentAtr?: number;
  childAtr?: number;
  riskAmount: number;
  parentMinutes: number;
  childMinutes: number;
  maxR: number;
  parentMetricSnapshot?: SymbolTimeframeMetric;
  childMetricSnapshot?: SymbolTimeframeMetric;
  unsubscribePrice?: () => void;
  processingLiveUpdate: boolean;
  pendingLiveUpdate?: PriceTick;
  lastPrice?: number;
}

@Injectable()
export class StrategyService {
  private readonly logger = new Logger(StrategyService.name);
  private readonly managedPositions = new Map<string, ManagedPositionState>();
  private readonly timeframeMinutes: Record<string, number> = {
    '10m': 10,
    '30m': 30,
    '1h': 60,
    '2h': 120,
  };
  private readonly partialRatio = 0.3;

  constructor(private readonly tradingService: TradingService) {}

  async process(result: MoversResult): Promise<void> {
    if (!this.tradingService.isEnabled()) {
      return;
    }

    this.syncPositionStates();
    await this.tradingService.flattenResidualPositions();
    await this.updateManagedPositions(result);

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

    await this.updateManagedPositions(result);
  }

  private syncPositionStates(): void {
    const summaries = new Map(
      this.tradingService
        .getPositionSummaries()
        .map((summary) => [summary.symbol, summary]),
    );

    for (const [symbol, state] of Array.from(this.managedPositions.entries())) {
      const summary = summaries.get(symbol);
      if (!summary) {
        state.unsubscribePrice?.();
        this.managedPositions.delete(symbol);
        continue;
      }

      const quantity = this.extractQuantityForSide(summary, state.direction);
      if (quantity <= 1e-6) {
        state.unsubscribePrice?.();
        this.managedPositions.delete(symbol);
        continue;
      }

      state.totalQuantity = quantity;
      state.riskAmount = quantity * state.slDistance;
      this.ensurePriceWatcher(state);
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

    const state: ManagedPositionState = {
      symbol: candidate.symbol,
      direction,
      parentTimeframe,
      childTimeframe,
      entryPrice: avgPrice,
      baseQuantity: quantity,
      totalQuantity: quantity,
      kSl,
      slDistance,
      initialSlDistance: slDistance,
      stopPrice,
      trailAtrMultiple: trailAtr,
      cleanScore: cleanParent,
      gateScore: gateChild,
      openedAt: Date.now(),
      addCount: 0,
      beMoved: false,
      highestPrice: direction === 'LONG' ? avgPrice : undefined,
      lowestPrice: direction === 'SHORT' ? avgPrice : undefined,
      trailPrice: undefined,
      partialOneTaken: false,
      partialTwoTaken: false,
      timeStopStage: 0,
      timeStopTimestamp: undefined,
      structureBreakCounter: 0,
      parentAtr: parentMetric?.atrValue,
      childAtr: childMetric.atrValue,
      riskAmount: quantity * slDistance,
      parentMinutes: this.timeframeMinutes[parentTimeframe] ?? 60,
      childMinutes: this.timeframeMinutes[childTimeframe] ?? 10,
      maxR: 0,
      parentMetricSnapshot: this.cloneMetric(parentMetric),
      childMetricSnapshot: this.cloneMetric(childMetric),
      processingLiveUpdate: false,
      pendingLiveUpdate: undefined,
      unsubscribePrice: undefined,
      lastPrice: avgPrice,
    };

    this.managedPositions.set(candidate.symbol, state);
    this.ensurePriceWatcher(state);

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

  private async updateManagedPositions(result: MoversResult): Promise<void> {
    for (const [symbol, state] of Array.from(this.managedPositions.entries())) {
      const metricsMap = result.metrics[symbol];
      const parentMetric = metricsMap?.[state.parentTimeframe];
      const childMetric = metricsMap?.[state.childTimeframe];

      let currentPrice: number;
      if (childMetric?.latestClose) {
        currentPrice = childMetric.latestClose;
      } else {
        currentPrice = await this.tradingService.getMarkPrice(symbol);
      }

      if (!parentMetric || !childMetric) {
        continue;
      }

      state.parentAtr = parentMetric.atrValue;
      state.childAtr = childMetric.atrValue;
      state.cleanScore =
        (Math.abs(this.signedTrend(parentMetric)) +
          this.computeScores(parentMetric).efficiency +
          this.computeScores(parentMetric).align) /
        300;
      state.gateScore = childMetric.smallMoveGate ?? state.gateScore;
      state.childMinutes = this.timeframeMinutes[state.childTimeframe] ?? state.childMinutes;
      state.parentMinutes = this.timeframeMinutes[state.parentTimeframe] ?? state.parentMinutes;
      state.parentMetricSnapshot = this.cloneMetric(parentMetric);
      state.childMetricSnapshot = this.cloneMetric(childMetric);
      state.lastPrice = currentPrice;
      this.ensurePriceWatcher(state);

      if (state.direction === 'LONG') {
        state.highestPrice = Math.max(
          state.highestPrice ?? state.entryPrice,
          parentMetric.highestClose ?? currentPrice,
          currentPrice,
        );
      } else {
        state.lowestPrice = Math.min(
          state.lowestPrice ?? state.entryPrice,
          parentMetric.lowestClose ?? currentPrice,
          currentPrice,
        );
      }

      const rMultiple = this.calculateRMultiple(state, currentPrice);
      state.maxR = Math.max(state.maxR, rMultiple);

      await this.handleBreakEvenMove(state, childMetric, currentPrice);
      if (!this.managedPositions.has(symbol)) {
        continue;
      }
      await this.handleTrailingStop(state, parentMetric, childMetric, currentPrice);
      if (!this.managedPositions.has(symbol)) {
        continue;
      }
      await this.handlePartials(state, childMetric, currentPrice);
      if (!this.managedPositions.has(symbol)) {
        continue;
      }
      await this.handleAdds(state, childMetric, currentPrice);
      if (!this.managedPositions.has(symbol)) {
        continue;
      }
      await this.handleTimeStop(state);
      if (!this.managedPositions.has(symbol)) {
        continue;
      }
      await this.handleStructureBreak(state, childMetric);

      if (state.totalQuantity <= 1e-6) {
        state.unsubscribePrice?.();
        this.managedPositions.delete(symbol);
      }
    }
  }

  private ensurePriceWatcher(state: ManagedPositionState): void {
    if (state.totalQuantity <= 1e-6) {
      state.unsubscribePrice?.();
      state.unsubscribePrice = undefined;
      return;
    }
    if (state.unsubscribePrice) {
      return;
    }
    state.unsubscribePrice = this.tradingService.subscribePriceStream(
      state.symbol,
      (tick) => {
        void this.handleLivePriceTick(state.symbol, tick);
      },
    );
  }

  private async handleLivePriceTick(
    symbol: string,
    tick: PriceTick,
  ): Promise<void> {
    const state = this.managedPositions.get(symbol);
    if (!state || state.totalQuantity <= 1e-6) {
      state?.unsubscribePrice?.();
      return;
    }

    if (state.processingLiveUpdate) {
      state.pendingLiveUpdate = tick;
      return;
    }

    state.processingLiveUpdate = true;
    state.pendingLiveUpdate = undefined;

    try {
      const parentMetricSnapshot = state.parentMetricSnapshot;
      const childMetricSnapshot = state.childMetricSnapshot;
      if (!parentMetricSnapshot || !childMetricSnapshot) {
        return;
      }

      const currentPrice = tick.markPrice;
      state.lastPrice = currentPrice;

      if (state.direction === 'LONG') {
        state.highestPrice = Math.max(
          state.highestPrice ?? state.entryPrice,
          currentPrice,
        );
      } else {
        state.lowestPrice = Math.min(
          state.lowestPrice ?? state.entryPrice,
          currentPrice,
        );
      }

      const liveParentMetric = this.withLivePrice(parentMetricSnapshot, currentPrice);
      const liveChildMetric = this.withLivePrice(childMetricSnapshot, currentPrice);

      const rMultiple = this.calculateRMultiple(state, currentPrice);
      state.maxR = Math.max(state.maxR, rMultiple);

      await this.handleBreakEvenMove(state, liveChildMetric, currentPrice);
      if (!this.managedPositions.has(symbol)) {
        return;
      }

      await this.handleTrailingStop(
        state,
        liveParentMetric,
        liveChildMetric,
        currentPrice,
      );
      if (!this.managedPositions.has(symbol)) {
        return;
      }

      await this.handlePartials(state, liveChildMetric, currentPrice);
      if (!this.managedPositions.has(symbol)) {
        return;
      }

      await this.handleAdds(state, liveChildMetric, currentPrice);
      if (!this.managedPositions.has(symbol)) {
        return;
      }

      await this.handleTimeStop(state);
      if (!this.managedPositions.has(symbol)) {
        return;
      }

      await this.handleStructureBreak(state, liveChildMetric);
      if (!this.managedPositions.has(symbol)) {
        return;
      }

      if (state.totalQuantity <= 1e-6) {
        state.unsubscribePrice?.();
        this.managedPositions.delete(symbol);
      }
    } finally {
      state.processingLiveUpdate = false;
      const pending = state.pendingLiveUpdate;
      state.pendingLiveUpdate = undefined;
      if (pending) {
        void this.handleLivePriceTick(symbol, pending);
      }
    }
  }

  private calculateRMultiple(
    state: ManagedPositionState,
    currentPrice: number,
  ): number {
    const baseDistance = state.initialSlDistance || state.slDistance;
    if (baseDistance <= 0) {
      return 0;
    }
    const dirSign = state.direction === 'LONG' ? 1 : -1;
    return dirSign * (currentPrice - state.entryPrice) / baseDistance;
  }

  private async handleBreakEvenMove(
    state: ManagedPositionState,
    childMetric: SymbolTimeframeMetric,
    currentPrice: number,
  ): Promise<void> {
    if (state.beMoved || state.slDistance <= 0) {
      return;
    }

    const childScores = this.computeScores(childMetric);
    const threshold =
      childScores.volume >= 55 && childScores.flow >= 55 ? 1 : 1.3;

    if (state.maxR < threshold) {
      return;
    }

    const dirSign = state.direction === 'LONG' ? 1 : -1;
    let newStop = state.entryPrice;
    if (state.direction === 'LONG') {
      newStop = Math.min(newStop, currentPrice - 0.0005 * currentPrice);
    } else {
      newStop = Math.max(newStop, currentPrice + 0.0005 * currentPrice);
    }

    await this.tradingService.replaceStopLoss(
      state.symbol,
      state.direction,
      state.totalQuantity,
      newStop,
    );

    state.stopPrice = newStop;
    state.slDistance = Math.abs(newStop - state.entryPrice);
    state.beMoved = true;
    state.riskAmount = state.totalQuantity * state.slDistance;
  }

  private async moveStopToBreakEven(
    state: ManagedPositionState,
    currentPrice: number,
  ): Promise<void> {
    if (
      state.beMoved ||
      state.slDistance <= 0 ||
      state.totalQuantity <= 1e-6
    ) {
      return;
    }

    let newStop = state.entryPrice;
    if (state.direction === 'LONG') {
      newStop = Math.min(newStop, currentPrice - 0.0005 * currentPrice);
    } else {
      newStop = Math.max(newStop, currentPrice + 0.0005 * currentPrice);
    }

    await this.tradingService.replaceStopLoss(
      state.symbol,
      state.direction,
      state.totalQuantity,
      newStop,
    );

    state.stopPrice = newStop;
    state.slDistance = Math.abs(newStop - state.entryPrice);
    state.beMoved = true;
    state.riskAmount = state.totalQuantity * state.slDistance;
  }

  private async handleTrailingStop(
    state: ManagedPositionState,
    parentMetric: SymbolTimeframeMetric,
    childMetric: SymbolTimeframeMetric,
    currentPrice: number,
  ): Promise<void> {
    const parentAtr = parentMetric.atrValue;
    if (!parentAtr || parentAtr <= 0) {
      return;
    }

    let trailMultiple = this.computeTrailAtr(state.cleanScore, state.gateScore);
    if (
      this.isDegrading(childMetric.efficiencyHistory ?? []) ||
      this.isMomentumTurning(childMetric.momentumHistory ?? [])
    ) {
      trailMultiple = Math.max(1.6, trailMultiple - 0.4);
    }

    state.trailAtrMultiple = trailMultiple;
    const referenceHigh = Math.max(
      parentMetric.highestClose ?? currentPrice,
      state.highestPrice ?? currentPrice,
    );
    const referenceLow = Math.min(
      parentMetric.lowestClose ?? currentPrice,
      state.lowestPrice ?? currentPrice,
    );

    if (state.direction === 'LONG') {
      const newTrail = referenceHigh - trailMultiple * parentAtr;
      if (newTrail > (state.trailPrice ?? state.stopPrice) && newTrail < currentPrice) {
        state.trailPrice = newTrail;
        await this.tradingService.replaceStopLoss(
          state.symbol,
          state.direction,
          state.totalQuantity,
          newTrail,
        );
        state.stopPrice = newTrail;
        state.slDistance = Math.abs(newTrail - state.entryPrice);
        state.riskAmount = state.totalQuantity * state.slDistance;
      }
    } else {
      const newTrail = referenceLow + trailMultiple * parentAtr;
      if (newTrail < (state.trailPrice ?? state.stopPrice) && newTrail > currentPrice) {
        state.trailPrice = newTrail;
        await this.tradingService.replaceStopLoss(
          state.symbol,
          state.direction,
          state.totalQuantity,
          newTrail,
        );
        state.stopPrice = newTrail;
        state.slDistance = Math.abs(newTrail - state.entryPrice);
        state.riskAmount = state.totalQuantity * state.slDistance;
      }
    }
  }

  private async handlePartials(
    state: ManagedPositionState,
    childMetric: SymbolTimeframeMetric,
    currentPrice: number,
  ): Promise<void> {
    if (state.totalQuantity <= 1e-6) {
      return;
    }

    const childScores = this.computeScores(childMetric);
    const cleanTrend = state.cleanScore >= 0.6 && state.gateScore >= 0.7;
    const strongVolume = childScores.volume >= 70 && childScores.flow >= 70;
    const rMultiple = this.calculateRMultiple(state, currentPrice);
    const partialQty = Math.min(this.partialRatio * state.baseQuantity, state.totalQuantity);

    if (!state.partialOneTaken) {
      const triggeredCleanTrend = cleanTrend && rMultiple >= 2;
      const triggeredGeneral =
        !cleanTrend && !strongVolume && rMultiple >= 1.5;
      if (triggeredCleanTrend || triggeredGeneral) {
        await this.executePartial(state, partialQty);
        if (!this.managedPositions.has(state.symbol)) {
          return;
        }
        state.partialOneTaken = true;
        if (triggeredGeneral) {
          await this.moveStopToBreakEven(state, currentPrice);
        }
      }
    }

    if (!state.partialTwoTaken && !cleanTrend && rMultiple >= 2) {
      await this.executePartial(state, partialQty);
      state.partialTwoTaken = true;
    }
  }

  private async executePartial(
    state: ManagedPositionState,
    quantity: number,
  ): Promise<void> {
    if (quantity <= 0) {
      return;
    }

    await this.tradingService.reducePosition(
      state.symbol,
      state.direction,
      Math.min(quantity, state.totalQuantity),
    );
    state.totalQuantity = Math.max(state.totalQuantity - quantity, 0);
    if (state.totalQuantity <= 1e-6) {
      await this.closePosition(state, 'Position fully exited');
      return;
    }

    await this.tradingService.replaceStopLoss(
      state.symbol,
      state.direction,
      state.totalQuantity,
      state.stopPrice,
    );
    state.riskAmount = state.totalQuantity * state.slDistance;
  }

  private async handleAdds(
    state: ManagedPositionState,
    childMetric: SymbolTimeframeMetric,
    currentPrice: number,
  ): Promise<void> {
    if (state.addCount >= 2 || !state.beMoved) {
      return;
    }

    const childScores = this.computeScores(childMetric);
    if (
      state.cleanScore < 0.65 ||
      state.gateScore < 0.7 ||
      childScores.efficiency < 55
    ) {
      return;
    }

    const rMultiple = this.calculateRMultiple(state, currentPrice);

    if (state.addCount === 0 && rMultiple >= 1) {
      await this.executeAdd(state, state.baseQuantity * 0.5);
    }

    if (state.addCount === 1 && rMultiple >= 2) {
      await this.executeAdd(state, state.baseQuantity * 0.33);
    }
  }

  private async executeAdd(
    state: ManagedPositionState,
    quantity: number,
  ): Promise<void> {
    if (quantity <= 0) {
      return;
    }

    await this.tradingService.increasePosition(
      state.symbol,
      state.direction,
      quantity,
    );
    state.totalQuantity += quantity;
    state.addCount += 1;
    await this.tradingService.replaceStopLoss(
      state.symbol,
      state.direction,
      state.totalQuantity,
      state.stopPrice,
    );
    state.riskAmount = state.totalQuantity * state.slDistance;
  }

  private async handleTimeStop(state: ManagedPositionState): Promise<void> {
    const childMinutes = state.childMinutes || 10;
    const parentMinutes = state.parentMinutes || 30;
    const thresholdCandles = Math.max(1, Math.ceil(3 * (parentMinutes / childMinutes)));
    const elapsedCandles = Math.floor(
      (Date.now() - state.openedAt) / (childMinutes * 60_000),
    );

    if (state.timeStopStage === 0 && elapsedCandles >= thresholdCandles) {
      if (state.maxR < 0.5) {
        const dirSign = state.direction === 'LONG' ? 1 : -1;
        const targetStop =
          state.entryPrice - dirSign * 0.5 * state.initialSlDistance;
        await this.tradingService.replaceStopLoss(
          state.symbol,
          state.direction,
          state.totalQuantity,
          targetStop,
        );
        state.stopPrice = targetStop;
        state.slDistance = Math.abs(targetStop - state.entryPrice);
        state.riskAmount = state.totalQuantity * state.slDistance;
      }
      state.timeStopStage = 1;
      state.timeStopTimestamp = Date.now();
    } else if (
      state.timeStopStage === 1 &&
      state.timeStopTimestamp &&
      Date.now() - state.timeStopTimestamp >= thresholdCandles * childMinutes * 60_000 &&
      state.maxR < 0.5
    ) {
      await this.closePosition(state, 'Time stop');
    }
  }

  private async handleStructureBreak(
    state: ManagedPositionState,
    childMetric: SymbolTimeframeMetric,
  ): Promise<void> {
    const trailBase = state.trailPrice ?? state.stopPrice;
    const atrChild = childMetric.atrValue ?? state.childAtr ?? 0;
    if (!trailBase || atrChild <= 0) {
      return;
    }

    const closes = childMetric.closeHistory ?? [];
    if (closes.length < 2) {
      return;
    }

    const threshold =
      trailBase + (state.direction === 'LONG' ? 1 : -1) * 0.3 * atrChild;
    const recent = closes.slice(-2);
    const breach = recent.every((close) =>
      state.direction === 'LONG' ? close <= threshold : close >= threshold,
    );

    if (breach) {
      state.structureBreakCounter += 1;
    } else {
      state.structureBreakCounter = 0;
    }

    if (state.structureBreakCounter >= 2) {
      await this.closePosition(state, 'Structure break');
    }
  }

  private async closePosition(
    state: ManagedPositionState,
    reason: string,
  ): Promise<void> {
    state.unsubscribePrice?.();
    state.unsubscribePrice = undefined;
    await this.tradingService.cancelAllOpenOrders(state.symbol);
    if (state.totalQuantity > 1e-6) {
      await this.tradingService.reducePosition(
        state.symbol,
        state.direction,
        state.totalQuantity,
      );
    }
    this.logger.log(`Closed ${state.symbol}: ${reason}`);
    this.managedPositions.delete(state.symbol);
  }

  private extractQuantityForSide(
    summary: PositionSummary,
    direction: PositionDirection,
  ): number {
    if (direction === 'LONG') {
      return Math.abs(summary.long ?? summary.net);
    }
    return Math.abs(summary.short ?? summary.net);
  }

  private isDegrading(series: number[]): boolean {
    if (series.length < 10) {
      return false;
    }
    const last = series.slice(-10);
    for (let i = 1; i < last.length; i += 1) {
      if (last[i] > last[i - 1]) {
        return false;
      }
    }
    return true;
  }

  private isMomentumTurning(series: number[]): boolean {
    if (series.length < 3) {
      return false;
    }
    const slice = series.slice(-3);
    return slice[2] - slice[0] < 0;
  }

  private extractNumber(value: unknown): number {
    const num = Number(value);
    return Number.isFinite(num) ? num : 0;
  }

  private cloneMetric(metric: SymbolTimeframeMetric): SymbolTimeframeMetric {
    return {
      ...metric,
      efficiencyHistory: [...(metric.efficiencyHistory ?? [])],
      momentumHistory: [...(metric.momentumHistory ?? [])],
      closeHistory: [...(metric.closeHistory ?? [])],
    };
  }

  private withLivePrice(
    metric: SymbolTimeframeMetric,
    price: number,
  ): SymbolTimeframeMetric {
    const clone = this.cloneMetric(metric);
    clone.latestClose = price;
    clone.highestClose = Math.max(
      clone.highestClose ?? price,
      price,
    );
    clone.lowestClose = Math.min(
      clone.lowestClose ?? price,
      price,
    );
    const history = clone.closeHistory ?? [];
    history.push(price);
    if (history.length > 240) {
      history.splice(0, history.length - 240);
    }
    clone.closeHistory = history;
    return clone;
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
