import { Injectable, Logger } from '@nestjs/common';
import axios, { AxiosInstance } from 'axios';
import { MOVERS_TIMEFRAMES, MoversSnapshot } from '../binance/binance.service';

const TELEGRAM_MESSAGE_LIMIT = 4000;

@Injectable()
export class TelegramService {
  private readonly logger = new Logger(TelegramService.name);
  private readonly token = process.env.TELEGRAM_BOT_TOKEN;
  private readonly chatId = process.env.TELEGRAM_CHAT_ID;
  private readonly client: AxiosInstance | null;

  constructor() {
    if (!this.token || !this.chatId) {
      this.logger.warn(
        'Telegram notifications disabled. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID to enable.',
      );
      this.client = null;
      return;
    }

    this.client = axios.create({
      baseURL: `https://api.telegram.org/bot${this.token}`,
      timeout: 10_000,
    });
  }

  async sendMoversReport(
    snapshots: Record<string, MoversSnapshot>,
  ): Promise<void> {
    if (!this.client || !this.chatId) {
      return;
    }

    const timestamp = new Date();
    const messages = this.buildMessages(timestamp, snapshots);

    for (const message of messages) {
      await this.sendMessage(message);
      await this.delay(400); // avoid flood limits
    }
  }

  private buildMessages(
    timestamp: Date,
    snapshots: Record<string, MoversSnapshot>,
  ): string[] {
    const header = `币安合约异动更新\n${timestamp.toISOString()}`;
    const messages = [header];
    const symbolChanges = this.collectSymbolChanges(snapshots);

    for (const { label } of MOVERS_TIMEFRAMES) {
      const snapshot = snapshots[label];
      if (!snapshot) {
        continue;
      }

      const lines: string[] = [];
      const windowLabel = this.buildWindowLabel(
        label,
        snapshot.window?.start,
        snapshot.window?.end,
      );
      lines.push(`=== ${windowLabel} ===`);

      if (snapshot.topGainers.length === 0 && snapshot.topLosers.length === 0) {
        lines.push('无数据');
        messages.push(lines.join('\n'));
        continue;
      }

      lines.push('涨幅榜：');
      lines.push(
        ...snapshot.topGainers.map((entry, index) =>
          this.formatEntry(entry, index + 1, label, symbolChanges),
        ),
      );

      lines.push('');
      lines.push('跌幅榜：');
      lines.push(
        ...snapshot.topLosers.map((entry, index) =>
          this.formatEntry(entry, index + 1, label, symbolChanges),
        ),
      );

      messages.push(lines.join('\n'));
    }

    return messages.flatMap((message) => this.splitMessage(message));
  }

  private formatEntry(
    entry: MoversSnapshot['topGainers'][number],
    rank: number,
    currentLabel: string,
    symbolChanges: Map<string, Record<string, number>>,
  ): string {
    const change = this.formatSignedPercent(entry.changePercent);
    const flowPercent =
      entry.flowPercent !== undefined
        ? `${this.formatNumber(entry.flowPercent)}%`
        : '未知';
    const flowLabel = entry.flowLabel ?? '流向未知';
    const scores = entry.scores;
    const finalScore = this.formatScore(scores.final);
    const coreScore = this.formatScore(scores.core);
    const confirmScore = this.formatScore(scores.confirm);
    const liquidityPenalty = this.formatScore(scores.liquidityPenalty);

    const behaviour = [
      `效率 ${this.formatScore(scores.efficiency)}`,
      `趋势 ${this.formatScore(1 - scores.chop)}`,
      `动量 ${this.formatScore(scores.momentumAtr)}`,
      `同向 ${this.formatScore(scores.align)}`,
      `多周期 ${this.formatScore(scores.mtfConsistency)}`,
      `门槛 ${this.formatScore(scores.gate)}`,
    ].join(' | ');

    const confirmation = [
      `量能 ${this.formatScore(scores.volumeBoost)}`,
      `主动 ${this.formatScore(scores.flowActive)}`,
      `持续 ${this.formatScore(scores.flowPersistence)}`,
    ].join(' | ');

    const extraTimeframes = this.buildAdditionalTimeframesLine(
      entry.symbol,
      currentLabel,
      symbolChanges,
    );

    return [
      `${rank}. ${entry.symbol} ${change}`,
      `综合 ${finalScore} | 核心 ${coreScore} | 确认 ${confirmScore} | 流动性惩罚 ${liquidityPenalty}`,
      `${behaviour}`,
      `${confirmation} | 主动成交 ${flowPercent} ${flowLabel}`,
      extraTimeframes,
    ]
      .filter((line) => line.length > 0)
      .join('\n');
  }

  private splitMessage(text: string): string[] {
    if (text.length <= TELEGRAM_MESSAGE_LIMIT) {
      return [text];
    }

    const chunks: string[] = [];
    let remaining = text;

    while (remaining.length > TELEGRAM_MESSAGE_LIMIT) {
      let end = remaining.lastIndexOf('\n', TELEGRAM_MESSAGE_LIMIT);
      if (end === -1 || end < TELEGRAM_MESSAGE_LIMIT / 2) {
        end = TELEGRAM_MESSAGE_LIMIT;
      }
      chunks.push(remaining.slice(0, end));
      remaining = remaining.slice(end).trimStart();
    }

    if (remaining.length > 0) {
      chunks.push(remaining);
    }

    return chunks;
  }

  private async sendMessage(text: string): Promise<void> {
    if (!this.client || !this.chatId) {
      return;
    }

    try {
      const response = await this.client.post('/sendMessage', {
        chat_id: this.chatId,
        text,
        disable_web_page_preview: true,
      });

      if (!response.data?.ok) {
        this.logger.warn(
          `Telegram API responded with non-ok status: ${JSON.stringify(response.data)}`,
        );
      }
    } catch (error) {
      this.logger.error(
        `Failed to send Telegram message: ${(error as Error).message}`,
      );
    }
  }

  private formatSignedPercent(value: number): string {
    if (!Number.isFinite(value)) {
      return '未知';
    }
    const formatted = this.formatNumber(Math.abs(value));
    return `${value >= 0 ? '+' : '-'}${formatted}%`;
  }

  private buildWindowLabel(
    label: string,
    start?: string,
    end?: string,
  ): string {
    if (start && end) {
      return `${label} 时间框（UTC+8 ${start}-${end}）`;
    }
    return `${label} 时间框`;
  }

  private formatScore(value: number | undefined): string {
    if (!Number.isFinite(value ?? NaN)) {
      return '未知';
    }
    return this.formatNumber((value ?? 0) * 100);
  }

  private formatNumber(value: number): string {
    if (!Number.isFinite(value)) {
      return '未知';
    }
    return value.toFixed(2);
  }

  private async delay(ms: number): Promise<void> {
    if (ms <= 0) {
      return;
    }
    await new Promise<void>((resolve) => setTimeout(resolve, ms));
  }

  private collectSymbolChanges(
    snapshots: Record<string, MoversSnapshot>,
  ): Map<string, Record<string, number>> {
    const symbolChanges = new Map<string, Record<string, number>>();

    for (const { label } of MOVERS_TIMEFRAMES) {
      const snapshot = snapshots[label];
      if (!snapshot) {
        continue;
      }

      for (const [symbol, change] of Object.entries(snapshot.changes ?? {})) {
        const map = symbolChanges.get(symbol) ?? {};
        map[label] = change;
        symbolChanges.set(symbol, map);
      }
    }

    return symbolChanges;
  }

  private buildAdditionalTimeframesLine(
    symbol: string,
    currentLabel: string,
    symbolChanges: Map<string, Record<string, number>>,
  ): string {
    const changeMap = symbolChanges.get(symbol);
    if (!changeMap) {
      return '';
    }

    const segments: string[] = [];
    for (const { label } of MOVERS_TIMEFRAMES) {
      if (label === currentLabel) {
        continue;
      }
      const value = changeMap[label];
      segments.push(
        `${label} ${value !== undefined ? this.formatSignedPercent(value) : '暂无'}`,
      );
    }

    if (segments.length === 0) {
      return '';
    }

    return `其他周期：${segments.join(' | ')}`;
  }
}
