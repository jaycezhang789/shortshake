# Binance USDT Perpetual Movers API

Fastify-powered NestJS service that aggregates Binance USDT-margined perpetual contracts and surfaces the top 15 gainers and losers across four timeframes: 10 minutes, 30 minutes, 1 hour, and 4 hours.

## Prerequisites

- Node.js 20+
- npm 10+

## Installation

```bash
npm install
```

## Running

```bash
# development
npm run start

# watch mode
npm run start:dev
```

The application listens on port `3000` by default (override with the `PORT` environment variable).

## API

### `GET /futures/movers`

Query parameters:

- `timeframe` (optional) — one of `10m`, `30m`, `1h`, `4h`.  
  When omitted the service returns data for every timeframe.

Example response (`/futures/movers?timeframe=1h`):

```json
{
  "timeframe": "1h",
  "topGainers": [
    { "symbol": "BTCUSDT", "price": 60123.45, "changePercent": 2.34 },
    { "symbol": "ETHUSDT", "price": 3250.12, "changePercent": 1.98 }
  ],
  "topLosers": [
    { "symbol": "BNBUSDT", "price": 520.44, "changePercent": -1.52 }
  ]
}
```

### Notes

- Contract metadata is cached for 6 hours. Candle statistics are cached for 30 seconds to avoid breaching Binance rate limits while keeping the data fresh.
- The service derives all intervals from 1-minute candlestick data to ensure consistent calculations.

## Testing

```bash
npm test
npm run test:e2e   # uses mocked Binance responses
```
