import { Test, TestingModule } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import request from 'supertest';
import { App } from 'supertest/types';
import { AppModule } from './../src/app.module';
import {
  BinanceService,
  Timeframe,
} from '../src/binance/binance.service';

describe('Binance movers (e2e)', () => {
  let app: INestApplication<App>;
  const mockService = {
    getMovers: jest.fn(),
  };

  beforeEach(async () => {
    mockService.getMovers.mockReset();
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    })
      .overrideProvider(BinanceService)
      .useValue(mockService)
      .compile();

    app = moduleFixture.createNestApplication();
    await app.init();
  });

  afterEach(async () => {
    await app.close();
  });

  it('/futures/movers?timeframe=1h (GET)', async () => {
    const payload = {
      timeframe: Timeframe.ONE_HOUR,
      topGainers: [],
      topLosers: [],
    };
    mockService.getMovers.mockResolvedValueOnce(payload);

    return request(app.getHttpServer())
      .get('/futures/movers')
      .query({ timeframe: Timeframe.ONE_HOUR })
      .expect(200)
      .expect(payload);
  });

  it('/futures/movers (GET) without timeframe', async () => {
    const payload = {
      [Timeframe.TEN_MINUTES]: {
        timeframe: Timeframe.TEN_MINUTES,
        topGainers: [],
        topLosers: [],
      },
    };
    mockService.getMovers.mockResolvedValueOnce(payload);

    return request(app.getHttpServer())
      .get('/futures/movers')
      .expect(200)
      .expect(payload);
  });
});
