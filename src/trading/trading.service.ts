import { Injectable, Logger } from '@nestjs/common';
import axios, { AxiosInstance, Method } from 'axios';
import * as crypto from 'crypto';

const BINANCE_BASE_URL = 'https://fapi.binance.com';
const MAX_POSITIONS = 5;
const DEFAULT_LEVERAGE = Number(process.env.BINANCE_LEVERAGE ?? 5);

interface PositionSummary {
  symbol: string;
  net: number;
  long?: number;
  short?: number;
}

interface SymbolFilters {
  stepSize: number;
  minQty: number;
  minNotional?: number;
  pricePrecision: number;
  quantityPrecision: number;
}

@Injectable()
export class TradingService {
  private readonly logger = new Logger(TradingService.name);
  private readonly apiKey = process.env.BINANCE_API_KEY ?? '';
  private readonly apiSecret = process.env.BINANCE_API_SECRET ?? '';
  private readonly recvWindow = Number(process.env.BINANCE_RECV_WINDOW ?? 5000);
  private readonly leverage = DEFAULT_LEVERAGE > 0 ? DEFAULT_LEVERAGE : 5;
  private readonly isTradingEnabled: boolean;

  private readonly client: AxiosInstance;
  private readonly publicClient: AxiosInstance;

  private totalWalletBalance = 0;
  private availableBalance = 0;
  private positions = new Map<string, PositionSummary>();
  private configuredSymbols = new Set<string>();
  private dualSideConfigured = false;

  private exchangeInfoCache:
    | { expiresAt: number; filters: Map<string, SymbolFilters> }
    | null = null;

  constructor() {
    this.client = axios.create({
      baseURL: BINANCE_BASE_URL,
      timeout: 10_000,
    });

    this.publicClient = axios.create({
      baseURL: BINANCE_BASE_URL,
      timeout: 10_000,
    });

    this.isTradingEnabled = Boolean(this.apiKey && this.apiSecret);
    if (!this.isTradingEnabled) {
      this.logger.warn(
        'BINANCE_API_KEY or BINANCE_API_SECRET missing. Trading operations disabled.',
      );
    }
  }

  isEnabled(): boolean {
    return this.isTradingEnabled;
  }

  getWalletBalance(): number {
    return this.totalWalletBalance;
  }

  getAvailableBalance(): number {
    return this.availableBalance;
  }

  getActiveSymbols(): string[] {
    return Array.from(this.positions.keys());
  }

  getPositionSummaries(): PositionSummary[] {
    return Array.from(this.positions.values()).map((summary) => ({
      symbol: summary.symbol,
      net: summary.net,
      long: summary.long,
      short: summary.short,
    }));
  }

  getRemainingSlots(): number {
    return Math.max(0, MAX_POSITIONS - this.positions.size);
  }

  canOpenPosition(symbol: string): boolean {
    if (!this.isTradingEnabled) {
      return false;
    }
    if (this.positions.has(symbol)) {
      return false;
    }
    return this.positions.size < MAX_POSITIONS;
  }

  async initialize(): Promise<void> {
    if (!this.isTradingEnabled) {
      return;
    }

    this.logger.log('Starting trading service initialization sequence.');
    try {
      await this.ensureDualSidePosition();
      this.logger.log(
        `Dual-side position mode status: ${this.dualSideConfigured ? 'enabled' : 'unable to confirm'}.`,
      );
      await this.refreshState();
      this.logger.log(
        `Initialized trading state. Wallet balance: ${this.totalWalletBalance.toFixed(
          2,
        )} USDT. Active symbols: ${this.positions.size}/${MAX_POSITIONS}.`,
      );
    } catch (error) {
      this.logger.error(
        `Failed to initialize trading service: ${(error as Error).message}`,
      );
    }
  }

  async refreshState(): Promise<void> {
    if (!this.isTradingEnabled) {
      return;
    }

    this.logger.log('Refreshing trading state from Binance.');
    const [balances, positions] = await Promise.all([
      this.fetchFuturesBalances(),
      this.fetchOpenPositions(),
    ]);

    this.positions = positions;
    this.totalWalletBalance = balances.totalWalletBalance;
    this.availableBalance = balances.availableBalance;
    this.logger.log(
      `Trading state refreshed. Wallet: ${this.totalWalletBalance.toFixed(
        2,
      )} USDT, Available: ${this.availableBalance.toFixed(
        2,
      )} USDT, Active symbols: ${this.positions.size}`,
    );
  }

  async createMarketOrder(
    symbol: string,
    direction: 'LONG' | 'SHORT',
    options?: { sizeScale?: number },
  ): Promise<any | null> {
    if (!this.isTradingEnabled) {
      this.logger.warn('Trading disabled. Order cancelled.');
      return null;
    }

    if (!this.canOpenPosition(symbol)) {
      this.logger.warn(
        `Cannot open new position for ${symbol}. Active symbols: ${this.positions.size}/${MAX_POSITIONS}.`,
      );
      return null;
    }

    this.logger.log(
      `Preparing ${direction} market order for ${symbol}.`,
    );
    const markPrice = await this.fetchMarkPrice(symbol);
    this.logger.log(
      `Mark price for ${symbol}: ${markPrice}.`,
    );
    const filters = await this.getSymbolFilters(symbol);
    await this.ensureSymbolConfiguration(symbol);

    const sizeScale = this.normalizeSizeScale(options?.sizeScale ?? 1);
    const margin = (this.totalWalletBalance / 5) * sizeScale;
    if (margin <= 0) {
      this.logger.warn('Insufficient wallet balance to allocate margin.');
      return null;
    }

    const notional = margin * this.leverage;
    const rawQty = notional / markPrice;
    let quantity = Math.max(rawQty, filters.minQty);
    quantity = this.applyStepSize(quantity, filters.stepSize);

    const notionalCheck = quantity * markPrice;
    if (filters.minNotional && notionalCheck < filters.minNotional) {
      quantity = this.applyStepSize(
        filters.minNotional / markPrice,
        filters.stepSize,
      );
    }

    if (quantity <= 0) {
      this.logger.warn(`Calculated quantity is invalid for ${symbol}.`);
      return null;
    }

    this.logger.log(
      `Calculated order quantity for ${symbol}: ${quantity} (notional ≈ ${(quantity * markPrice).toFixed(
        filters.pricePrecision,
      )} USDT).`,
    );
    const side = direction === 'LONG' ? 'BUY' : 'SELL';
    const positionSide = direction;

    const payload = {
      symbol,
      side,
      positionSide,
      type: 'MARKET',
      quantity: this.formatQuantity(quantity, filters.quantityPrecision),
      newOrderRespType: 'RESULT',
    };

    try {
      this.logger.log(
        `Sending market order request for ${symbol}: ${JSON.stringify(payload)}.`,
      );
      const response = await this.signedRequest(
        'POST',
        '/fapi/v1/order',
        payload,
      );
      await this.refreshState();
      this.logger.log(
        `Placed ${direction} order for ${symbol}. Qty: ${quantity}.`,
      );
      return response;
    } catch (error) {
      this.logger.error(
        `Order placement failed for ${symbol}: ${(error as Error).message}`,
      );
      return null;
    }
  }

  async placeStopLoss(
    symbol: string,
    direction: 'LONG' | 'SHORT',
    quantity: number,
    stopPrice: number,
  ): Promise<any | null> {
    if (!this.isTradingEnabled) {
      return null;
    }

    try {
      const filters = await this.getSymbolFilters(symbol);
      const formattedQty = this.formatQuantity(
        quantity,
        filters.quantityPrecision,
      );
      const formattedStop = this.formatPrice(stopPrice, filters.pricePrecision);
      const side = direction === 'LONG' ? 'SELL' : 'BUY';

      const response = await this.signedRequest(
        'POST',
        '/fapi/v1/order',
        {
          symbol,
          side,
          positionSide: direction,
          type: 'STOP_MARKET',
          stopPrice: formattedStop,
          quantity: formattedQty,
          timeInForce: 'GTC',
          workingType: 'CONTRACT_PRICE',
        },
      );

      this.logger.log(
        `Placed stop loss for ${symbol} @ ${formattedStop}.`,
      );
      return response;
    } catch (error) {
      this.logger.error(
        `Failed to place stop for ${symbol}: ${(error as Error).message}`,
      );
      return null;
    }
  }

  private async ensureDualSidePosition(): Promise<void> {
    if (this.dualSideConfigured) {
      return;
    }

    this.logger.log('Verifying dual-side position configuration.');
    try {
      const status = await this.signedRequest<{ dualSidePosition: boolean }>(
        'GET',
        '/fapi/v1/positionSide/dual',
      );
      if (!status.dualSidePosition) {
        this.logger.log('Dual-side mode disabled. Enabling now.');
        await this.signedRequest('POST', '/fapi/v1/positionSide/dual', {
          dualSidePosition: 'true',
        });
      }
      this.dualSideConfigured = true;
    } catch (error) {
      this.logger.warn(
        `Dual-side check failed, attempting to enable: ${(error as Error).message}`,
      );
      try {
        await this.signedRequest('POST', '/fapi/v1/positionSide/dual', {
          dualSidePosition: 'true',
        });
        this.dualSideConfigured = true;
      } catch (innerError) {
        this.logger.error(
          `Failed to enforce dual-side position mode: ${(innerError as Error).message}`,
        );
      }
    }
  }

  private async ensureSymbolConfiguration(symbol: string): Promise<void> {
    if (this.configuredSymbols.has(symbol)) {
      return;
    }

    this.logger.log(`Ensuring symbol configuration for ${symbol}.`);
    try {
      await this.signedRequest('POST', '/fapi/v1/marginType', {
        symbol,
        marginType: 'CROSSED',
      });
    } catch (error) {
      const err = error as any;
      if (!(err?.response?.data?.code === -4046)) {
        this.logger.warn(
          `Failed to set margin type for ${symbol}: ${err?.message ?? err}`,
        );
      }
    }

    try {
      await this.signedRequest('POST', '/fapi/v1/leverage', {
        symbol,
        leverage: this.leverage,
      });
    } catch (error) {
      this.logger.warn(
        `Failed to set leverage for ${symbol}: ${(error as Error).message}`,
      );
    }

    this.configuredSymbols.add(symbol);
    this.logger.log(
      `Symbol ${symbol} configuration ensured (margin type + leverage).`,
    );
  }

  private async fetchFuturesBalances(): Promise<{
    totalWalletBalance: number;
    availableBalance: number;
  }> {
    const balances = (await this.signedRequest<any[]>(
      'GET',
      '/fapi/v2/balance',
    )) as Array<Record<string, string>>;

    let totalWalletBalance = 0;
    let availableBalance = 0;

    for (const balance of balances) {
      if (balance.asset === 'USDT') {
        totalWalletBalance = parseFloat(balance.balance ?? '0');
        availableBalance = parseFloat(balance.availableBalance ?? '0');
        break;
      }
    }

    this.logger.log(
      `Fetched USDT futures balances -> total: ${totalWalletBalance.toFixed(
        2,
      )}, available: ${availableBalance.toFixed(2)}.`,
    );
    return {
      totalWalletBalance,
      availableBalance,
    };
  }

  private async fetchOpenPositions(): Promise<Map<string, PositionSummary>> {
    const positions = (await this.signedRequest<any[]>(
      'GET',
      '/fapi/v2/positionRisk',
    )) as Array<Record<string, string>>;

    const map = new Map<string, PositionSummary>();

    for (const position of positions) {
      const positionAmt = parseFloat(position.positionAmt ?? '0');
      if (Math.abs(positionAmt) < 1e-6) {
        continue;
      }

      const symbol = position.symbol;
      const positionSide = (position.positionSide ?? 'BOTH') as
        | 'LONG'
        | 'SHORT'
        | 'BOTH';
      const summary =
        map.get(symbol) ?? ({
          symbol,
          net: 0,
        } as PositionSummary);

      if (positionSide === 'LONG') {
        summary.long = positionAmt;
      } else if (positionSide === 'SHORT') {
        summary.short = positionAmt;
      } else {
        summary.net = positionAmt;
      }

      summary.net += positionAmt;
      map.set(symbol, summary);
    }

    this.logger.log(
      `Fetched ${map.size} active position summaries.`,
    );
    return map;
  }

  private async fetchMarkPrice(symbol: string): Promise<number> {
    const response = await this.publicClient.get<{
      symbol: string;
      price: string;
    }>('/fapi/v1/ticker/price', {
      params: { symbol },
    });
    const price = parseFloat(response.data?.price ?? '0');
    if (!Number.isFinite(price) || price <= 0) {
      throw new Error(`Invalid mark price received for ${symbol}`);
    }
    return price;
  }

  private async getSymbolFilters(symbol: string): Promise<SymbolFilters> {
    const now = Date.now();
    if (
      this.exchangeInfoCache &&
      this.exchangeInfoCache.expiresAt > now &&
      this.exchangeInfoCache.filters.has(symbol)
    ) {
      this.logger.log(`Using cached exchange filters for ${symbol}.`);
      return this.exchangeInfoCache.filters.get(symbol) as SymbolFilters;
    }

    this.logger.log('Refreshing exchange filters from Binance.');
    const response = await this.publicClient.get<{
      symbols: Array<{
        symbol: string;
        pricePrecision: number;
        quantityPrecision: number;
        filters: Array<{
          filterType: string;
          stepSize?: string;
          tickSize?: string;
          minQty?: string;
          notional?: string;
          minNotional?: string;
        }>;
      }>;
    }>('/fapi/v1/exchangeInfo');

    const filtersMap = new Map<string, SymbolFilters>();

    for (const item of response.data.symbols ?? []) {
      const lotFilter = item.filters.find(
        (filter) => filter.filterType === 'LOT_SIZE',
      );
      const minNotionalFilter =
        item.filters.find((filter) => filter.filterType === 'MIN_NOTIONAL') ??
        item.filters.find((filter) => filter.filterType === 'NOTIONAL');

      const stepSize = parseFloat(lotFilter?.stepSize ?? '0.001');
      const minQty = parseFloat(lotFilter?.minQty ?? '0.001');
      const minNotional = parseFloat(
        minNotionalFilter?.minNotional ?? minNotionalFilter?.notional ?? '0',
      );

      filtersMap.set(item.symbol, {
        stepSize,
        minQty,
        minNotional: Number.isFinite(minNotional) ? minNotional : undefined,
        pricePrecision: item.pricePrecision ?? 2,
        quantityPrecision: item.quantityPrecision ?? 3,
      });
    }

    this.exchangeInfoCache = {
      filters: filtersMap,
      expiresAt: now + 30 * 60 * 1000,
    };

    this.logger.log(
      `Exchange filters refreshed. Cached ${filtersMap.size} symbols.`,
    );
    const filters = filtersMap.get(symbol);
    if (!filters) {
      throw new Error(`Symbol filters not found for ${symbol}`);
    }
    return filters;
  }

  private applyStepSize(value: number, stepSize: number): number {
    if (!Number.isFinite(value) || value <= 0) {
      return 0;
    }
    if (!Number.isFinite(stepSize) || stepSize <= 0) {
      return value;
    }
    const factor = Math.round(1 / stepSize);
    if (!Number.isFinite(factor) || factor <= 0) {
      return Math.floor(value / stepSize) * stepSize;
    }
    return Math.floor(value * factor) / factor;
  }

  private formatQuantity(quantity: number, precision: number): string {
    return quantity.toFixed(precision);
  }

  private formatPrice(price: number, precision: number): string {
    return price.toFixed(precision);
  }

  private normalizeSizeScale(value: number): number {
    if (!Number.isFinite(value)) {
      return 1;
    }
    return Math.min(Math.max(value, 0.1), 1);
  }

  async flattenResidualPositions(threshold = 0.001): Promise<void> {
    if (!this.isTradingEnabled) {
      return;
    }

    for (const summary of this.positions.values()) {
      const symbol = summary.symbol;

      const longQty = Math.abs(summary.long ?? 0);
      if (longQty > 0 && longQty < threshold) {
        await this.submitReduceOnlyOrder(symbol, 'LONG', longQty);
      }

      const shortQty = Math.abs(summary.short ?? 0);
      if (shortQty > 0 && shortQty < threshold) {
        await this.submitReduceOnlyOrder(symbol, 'SHORT', shortQty);
      }
    }
  }

  private async submitReduceOnlyOrder(
    symbol: string,
    positionSide: 'LONG' | 'SHORT',
    quantity: number,
  ): Promise<void> {
    try {
      const filters = await this.getSymbolFilters(symbol);
      const adjusted = this.applyStepSize(quantity, filters.stepSize);
      if (adjusted <= 0) {
        this.logger.debug(
          `Residual position for ${symbol} below step size, skipping cleanup.`,
        );
        return;
      }

      const formattedQty = this.formatQuantity(
        adjusted,
        filters.quantityPrecision,
      );
      const side = positionSide === 'LONG' ? 'SELL' : 'BUY';

      await this.signedRequest('POST', '/fapi/v1/order', {
        symbol,
        side,
        positionSide,
        type: 'MARKET',
        quantity: formattedQty,
      });

      this.logger.log(
        `Flattened residual ${positionSide} position for ${symbol}, qty ${formattedQty}.`,
      );
    } catch (error) {
      this.logger.warn(
        `Failed to flatten residual ${positionSide} position for ${symbol}: ${(error as Error).message}`,
      );
    }
  }

  private async signedRequest<T>(
    method: Method,
    path: string,
    params: Record<string, any> = {},
  ): Promise<T> {
    if (!this.isTradingEnabled) {
      throw new Error('Trading credentials not configured.');
    }

    const timestamp = Date.now();
    const query = new URLSearchParams();

    for (const [key, value] of Object.entries(params)) {
      if (value === undefined || value === null) {
        continue;
      }
      query.append(key, String(value));
    }
    query.append('timestamp', timestamp.toString());
    query.append('recvWindow', String(this.recvWindow));

    const signature = crypto
      .createHmac('sha256', this.apiSecret)
      .update(query.toString())
      .digest('hex');

    query.append('signature', signature);

    const url = `${path}?${query.toString()}`;

    this.logger.log(`Dispatching signed request ${method} ${path}.`);
    const response = await this.client.request<T>({
      url,
      method,
      headers: {
        'X-MBX-APIKEY': this.apiKey,
      },
    });

    return response.data;
  }
}
