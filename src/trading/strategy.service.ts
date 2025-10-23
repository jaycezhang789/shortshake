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
  private readonly kSlBuffer: number;

  constructor(private readonly tradingService: TradingService) {
    this.kSlBuffer = this.computeKSlBuffer();
  }

  async process(result: MoversResult): Promise<void> {
    if (!this.tradingService.isEnabled()) {
      return;
    }

    this.syncPositionStates();
    await this.tradingService.flattenResidualPositions();
    // 仅在周期刷新时进行分批和加仓的检查（不启用动态移动止损/时间止损）
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
    // 再次根据最新指标进行一次检查
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

    const baseKSl = 1.2 + 0.9 * cleanParent + 0.3 * gateChild;
    const minBuffered = 1.2 * this.kSlBuffer;
    const maxBuffered = 2.8 * this.kSlBuffer;
    const kSl = this.clamp(
      baseKSl * this.kSlBuffer,
      Math.min(minBuffered, maxBuffered),
      Math.max(minBuffered, maxBuffered),
    );
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
    const momentumMagnitude = metric.momentumAtr ?? 0;
    const momentumTrigger = gate >= 0.65 && momentumMagnitude >= 0.5;

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

      // 仅更新用于分批/加仓判定所需的状态（不做移动止损/时间止损）
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

      const rMultiple = this.calculateRMultiple(state, currentPrice);
      state.maxR = Math.max(state.maxR, rMultiple);

      // 仅启用：分批止盈、加仓
      await this.handlePartials(state, childMetric, currentPrice);
      if (!this.managedPositions.has(symbol)) {
        continue;
      }
      await this.handleAdds(state, childMetric, currentPrice);
      if (!this.managedPositions.has(symbol)) {
        continue;
      }

      if (state.totalQuantity <= 1e-6) {
        state.unsubscribePrice?.();
        this.managedPositions.delete(symbol);
      }
    }
  }

  private ensurePriceWatcher(_: ManagedPositionState): void {
    // 实时价格订阅仅服务于移动止损与动态仓位调整，现已关闭。
  }

  private async handleLivePriceTick(
    _symbol: string,
    _tick: PriceTick,
  ): Promise<void> {
    // 实时价格回调仅服务于移动止损与动态仓位调整，现已关闭。
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
    // 动态移动止损已禁用
    void state;
    void childMetric;
    void currentPrice;
    return;
  }

  private async moveStopToBreakEven(
    state: ManagedPositionState,
    currentPrice: number,
  ): Promise<void> {
    // 动态移动止损已禁用
    void state;
    void currentPrice;
    return;
  }

  private async handleTrailingStop(
    state: ManagedPositionState,
    parentMetric: SymbolTimeframeMetric,
    childMetric: SymbolTimeframeMetric,
    currentPrice: number,
  ): Promise<void> {
    // 动态移动止损已禁用
    void state;
    void parentMetric;
    void childMetric;
    void currentPrice;
    return;
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
      const triggeredGeneral = !cleanTrend && !strongVolume && rMultiple >= 1.5;
      if (triggeredCleanTrend || triggeredGeneral) {
        await this.executePartial(state, partialQty);
        if (!this.managedPositions.has(state.symbol)) {
          return;
        }
        state.partialOneTaken = true;
        // 不再调用 moveStopToBreakEven，避免动态移动止损
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

    // 重新挂同价止损，数量按剩余头寸调整（不改变止损价）
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
    // 允许加仓：不再依赖 beMoved（保本），仅保留次数与评分阈值限制
    if (state.addCount >= 2) {
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
    // 重新挂同价止损，数量按新增后头寸调整（不改变止损价）
    await this.tradingService.replaceStopLoss(
      state.symbol,
      state.direction,
      state.totalQuantity,
      state.stopPrice,
    );
    state.riskAmount = state.totalQuantity * state.slDistance;
  }

  private async handleTimeStop(state: ManagedPositionState): Promise<void> {
    // 时间止损已禁用
    void state;
    return;
  }

  private async handleStructureBreak(
    state: ManagedPositionState,
    childMetric: SymbolTimeframeMetric,
  ): Promise<void> {
    // 结构性止损已禁用
    void state;
    void childMetric;
    return;
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
    if (series.length < 4) {
      return false;
    }
    const last = series.slice(-4);
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

  private computeKSlBuffer(): number {
    const raw = Number(process.env.STRATEGY_KSL_BUFFER ?? 1);
    if (!Number.isFinite(raw) || raw <= 0) {
      return 1;
    }
    return this.clamp(raw, 0.5, 2);
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
