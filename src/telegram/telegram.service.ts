import { Injectable, Logger } from '@nestjs/common';
import axios, { AxiosInstance } from 'axios';
import {
  MOVERS_TIMEFRAMES,
  AggregatedMoversEntry,
  MoversResult,
} from '../binance/binance.service';

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

  async sendMoversReport(report: MoversResult): Promise<void> {
    if (!this.client || !this.chatId) {
      return;
    }

    const timestamp = new Date();
    const messages = this.buildMessages(timestamp, report);

    for (const message of messages) {
      await this.sendMessage(message);
      await this.delay(400); // avoid flood limits
    }
  }

  async sendWalletSummary(
    balance: number,
    available: number,
    unrealized: number,
    positions: Array<{
      symbol: string;
      net: number;
      long?: number;
      short?: number;
      unrealizedPnl: number;
    }>,
  ): Promise<void> {
    if (!this.client || !this.chatId) {
      return;
    }

    const timestamp = new Date();
    const header = `账户快照 ${timestamp.toISOString()}`;
    const lines: string[] = [
      header,
      `总权益: ${balance.toFixed(2)} USDT`,
      `可用余额: ${available.toFixed(2)} USDT`,
      `未实现盈亏: ${unrealized.toFixed(2)} USDT`,
      `持仓数量: ${positions.length}`,
    ];

    if (positions.length > 0) {
      const positionLines = positions.map((position) => {
        const longQty = position.long ?? 0;
        const shortQty = position.short ?? 0;
        return `${position.symbol} | 净仓 ${position.net.toFixed(4)} | 多 ${longQty.toFixed(4)} | 空 ${shortQty.toFixed(4)} | 浮盈 ${position.unrealizedPnl.toFixed(2)}`;
      });
      lines.push('持仓详情:');
      lines.push(...positionLines.slice(0, 15));
      if (positionLines.length > 15) {
        lines.push(`... 其余 ${positionLines.length - 15} 条`);
      }
    }

    await this.sendMessage(lines.join('\n'));
  }

  private buildMessages(timestamp: Date, report: MoversResult): string[] {
    const header = `币安合约异动更新\n${timestamp.toISOString()}`;
    const messages = [header];
    const lines: string[] = [];

    if (report.aggregatedTop.length === 0) {
      lines.push('暂无符合条件的合约。');
    } else {
      lines.push('综合得分前20（去重）如下：');
      report.aggregatedTop.forEach((item, index) => {
        lines.push(
          this.formatAggregatedEntry(item, index + 1),
        );
      });
    }

    if (lines.length > 0) {
      messages.push(lines.join('\n\n'));
    }

    return messages.flatMap((message) => this.splitMessage(message));
  }

  private formatAggregatedEntry(
    aggregated: AggregatedMoversEntry,
    rank: number,
  ): string {
    const { entry, timeframe, changes, window } = aggregated;
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
      timeframe,
      changes,
    );

    return [
      `${rank}. ${entry.symbol} (${timeframe} ${window.start}-${window.end}) ${change}`,
      `综合 ${finalScore} | 核心 ${coreScore} | 确认 ${confirmScore} | 流动性惩罚 ${liquidityPenalty}`,
      `${behaviour}`,
      `${confirmation} | 主动成交 ${flowPercent} ${flowLabel}`,
      extraTimeframes,
    ].filter((line) => line.length > 0).join('\n');
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

  private buildAdditionalTimeframesLine(
    currentLabel: string,
    changes: Record<string, number>,
  ): string {
    const segments: string[] = [];
    for (const { label } of MOVERS_TIMEFRAMES) {
      if (label === currentLabel) {
        continue;
      }
      const value = changes[label];
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
