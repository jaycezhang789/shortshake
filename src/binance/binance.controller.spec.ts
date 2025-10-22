import { Test, TestingModule } from '@nestjs/testing';
import { BinanceController } from './binance.controller';
import { BinanceService, Timeframe } from './binance.service';

describe('BinanceController', () => {
  let controller: BinanceController;
  let service: { getMovers: jest.Mock };

  beforeEach(async () => {
    service = {
      getMovers: jest.fn(),
    };

    const module: TestingModule = await Test.createTestingModule({
      controllers: [BinanceController],
      providers: [
        {
          provide: BinanceService,
          useValue: service,
        },
      ],
    }).compile();

    controller = module.get<BinanceController>(BinanceController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });

  it('should forward timeframe argument to service', async () => {
    service.getMovers.mockResolvedValueOnce('data');
    const result = await controller.getMovers(Timeframe.ONE_HOUR);
    expect(service.getMovers).toHaveBeenCalledWith(Timeframe.ONE_HOUR);
    expect(result).toBe('data');
  });

  it('should request all timeframes when no argument provided', async () => {
    service.getMovers.mockResolvedValueOnce('all');
    const result = await controller.getMovers();
    expect(service.getMovers).toHaveBeenCalledWith(undefined);
    expect(result).toBe('all');
  });
});
