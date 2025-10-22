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
    const header = `Binance movers update\n${timestamp.toISOString()}`;
    const messages = [header];

    for (const { label } of MOVERS_TIMEFRAMES) {
      const snapshot = snapshots[label];
      if (!snapshot) {
        continue;
      }

      const lines: string[] = [];
      lines.push(`=== ${label} ===`);

      if (snapshot.topGainers.length === 0 && snapshot.topLosers.length === 0) {
        lines.push('无数据');
        messages.push(lines.join('\n'));
        continue;
      }

      lines.push('Top Gainers:');
      lines.push(
        ...snapshot.topGainers.map((entry, index) =>
          this.formatEntry(entry, index + 1),
        ),
      );

      lines.push('');
      lines.push('Top Losers:');
      lines.push(
        ...snapshot.topLosers.map((entry, index) =>
          this.formatEntry(entry, index + 1),
        ),
      );

      messages.push(lines.join('\n'));
    }

    return messages.flatMap((message) => this.splitMessage(message));
  }

  private formatEntry(
    entry: MoversSnapshot['topGainers'][number],
    rank: number,
  ): string {
    const change = this.formatSignedPercent(entry.changePercent);
    const flowPercent =
      entry.flowPercent !== undefined
        ? `${this.formatNumber(entry.flowPercent)}%`
        : 'N/A';
    const flowLabel = entry.flowLabel ?? '未知';
    const scores = entry.scores;
    const finalScore = this.formatScore(scores.final);
    const coreScore = this.formatScore(scores.core);
    const confirmScore = this.formatScore(scores.confirm);
    const liquidityPenalty = this.formatScore(scores.liquidityPenalty);

    const behaviour = [
      `Eff ${this.formatScore(scores.efficiency)}`,
      `Chop ${this.formatScore(1 - scores.chop)}`,
      `ATR ${this.formatScore(scores.momentumAtr)}`,
      `Align ${this.formatScore(scores.align)}`,
      `Gate ${this.formatScore(scores.gate)}`,
    ].join(' | ');

    const confirmation = [
      `Vol ${this.formatScore(scores.volumeBoost)}`,
      `Flow ${this.formatScore(scores.flowBoost)}`,
    ].join(' | ');

    return [
      `${rank}. ${entry.symbol} ${change}`,
      `Final ${finalScore} | Core ${coreScore} | Confirm ${confirmScore} | LiqPenalty ${liquidityPenalty}`,
      `${behaviour}`,
      `${confirmation} | Flow ${flowPercent} ${flowLabel}`,
    ].join('\n');
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
      return 'N/A';
    }
    const formatted = this.formatNumber(Math.abs(value));
    return `${value >= 0 ? '+' : '-'}${formatted}%`;
  }

  private formatScore(value: number | undefined): string {
    if (!Number.isFinite(value ?? NaN)) {
      return 'N/A';
    }
    return this.formatNumber((value ?? 0) * 100);
  }

  private formatNumber(value: number): string {
    if (!Number.isFinite(value)) {
      return 'N/A';
    }
    return value.toFixed(2);
  }

  private async delay(ms: number): Promise<void> {
    if (ms <= 0) {
      return;
    }
    await new Promise<void>((resolve) => setTimeout(resolve, ms));
  }
}
