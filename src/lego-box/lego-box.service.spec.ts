import { Test, TestingModule } from '@nestjs/testing';
import { getRepositoryToken } from '@nestjs/typeorm';
import { CACHE_MANAGER } from '@nestjs/cache-manager';
import { DataSource, QueryRunner, Repository } from 'typeorm';
import { LegoBoxService } from './lego-box.service';
import { LegoBox } from './entities/lego-box.entity';
import { LegoPiece } from './entities/lego-piece.entity';
import { Transaction } from './entities/transaction.entity';
import { TransactionBox } from './entities/transaction-box.entity';
import { BatchComponentUpdateProducer } from './batch-imports/batch-legobox-component-update/batch-component-update-producer';
import { BatchLegoBoxPriceUpdateProducer } from './batch-imports/lego-box-price-batch-update/batch-price-update-producer';
import { DuplicateBoxNameException } from '../common/exceptions/custom-exceptions';
import { NotFoundException } from '@nestjs/common';

describe('LegoBoxService', () => {
  let service: LegoBoxService;
  let legoBoxRepository: Repository<LegoBox>;
  let legoPieceRepository: Repository<LegoPiece>;
  let transactionRepository: Repository<Transaction>;
  let transactionBoxRepository: Repository<TransactionBox>;
  let dataSource: DataSource;
  let queryRunner: QueryRunner;
  let cacheManager: any;
  let batchComponentUpdateProducer: BatchComponentUpdateProducer;
  let batchLegoBoxPriceUpdateProducer: BatchLegoBoxPriceUpdateProducer;

  const mockQueryRunner = {
    connect: jest.fn(),
    startTransaction: jest.fn(),
    commitTransaction: jest.fn(),
    rollbackTransaction: jest.fn(),
    release: jest.fn(),
    manager: {
      save: jest.fn(),
      update: jest.fn(),
      findOne: jest.fn(),
    },
    isTransactionActive: true,
  };

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        LegoBoxService,
        {
          provide: getRepositoryToken(LegoBox),
          useValue: {
            create: jest.fn(),
            save: jest.fn(),
            findOne: jest.fn(),
            createQueryBuilder: jest.fn(() => ({
              where: jest.fn().mockReturnThis(),
              getOne: jest.fn(),
              leftJoinAndSelect: jest.fn().mockReturnThis(),
              select: jest.fn().mockReturnThis(),
              orderBy: jest.fn().mockReturnThis(),
              getMany: jest.fn(),
            })),
          },
        },
        {
          provide: getRepositoryToken(LegoPiece),
          useValue: {
            findOne: jest.fn(),
          },
        },
        {
          provide: getRepositoryToken(Transaction),
          useValue: {
            findOne: jest.fn(),
            createQueryBuilder: jest.fn(() => ({
              where: jest.fn().mockReturnThis(),
              getOne: jest.fn(),
              leftJoinAndSelect: jest.fn().mockReturnThis(),
              select: jest.fn().mockReturnThis(),
              orderBy: jest.fn().mockReturnThis(),
              getMany: jest.fn(),
            })),
          },
        },
        {
          provide: getRepositoryToken(TransactionBox),
          useValue: {
            createQueryBuilder: jest.fn(() => ({
              where: jest.fn().mockReturnThis(),
              getOne: jest.fn(),
              leftJoinAndSelect: jest.fn().mockReturnThis(),
              select: jest.fn().mockReturnThis(),
              orderBy: jest.fn().mockReturnThis(),
              getMany: jest.fn(),
            })),
          },
        },
        {
          provide: DataSource,
          useValue: {
            createQueryRunner: jest.fn().mockReturnValue(mockQueryRunner),
          },
        },
        {
          provide: CACHE_MANAGER,
          useValue: {
            get: jest.fn(),
            set: jest.fn(),
            del: jest.fn(),
          },
        },
        {
          provide: BatchComponentUpdateProducer,
          useValue: {
            queueBatchImport: jest.fn(),
          },
        },
        {
          provide: BatchLegoBoxPriceUpdateProducer,
          useValue: {
            queueBatchImport: jest.fn(),
          },
        },
      ],
    }).compile();

    service = module.get<LegoBoxService>(LegoBoxService);
    legoBoxRepository = module.get<Repository<LegoBox>>(
      getRepositoryToken(LegoBox),
    );
    legoPieceRepository = module.get<Repository<LegoPiece>>(
      getRepositoryToken(LegoPiece),
    );
    transactionRepository = module.get<Repository<Transaction>>(
      getRepositoryToken(Transaction),
    );
    transactionBoxRepository = module.get<Repository<TransactionBox>>(
      getRepositoryToken(TransactionBox),
    );
    dataSource = module.get<DataSource>(DataSource);
    cacheManager = module.get(CACHE_MANAGER);
    batchComponentUpdateProducer = module.get<BatchComponentUpdateProducer>(
      BatchComponentUpdateProducer,
    );
    batchLegoBoxPriceUpdateProducer =
      module.get<BatchLegoBoxPriceUpdateProducer>(
        BatchLegoBoxPriceUpdateProducer,
      );
  });

  describe('createLegoBox', () => {
    it('should create a new lego box successfully', async () => {
      const createLegoBoxDto = { name: 'Test Box' };
      const newLegoBox = { id: 1, name: 'Test Box', price: 0 };

      jest.spyOn(legoBoxRepository, 'findOne').mockResolvedValue(null);
      jest
        .spyOn(legoBoxRepository, 'create')
        .mockReturnValue(newLegoBox as LegoBox);
      jest
        .spyOn(legoBoxRepository, 'save')
        .mockResolvedValue(newLegoBox as LegoBox);

      const result = await service.createLegoBox(createLegoBoxDto);

      expect(result).toEqual(newLegoBox);
      expect(legoBoxRepository.findOne).toHaveBeenCalledWith({
        where: { name: createLegoBoxDto.name },
      });
      expect(legoBoxRepository.create).toHaveBeenCalledWith({
        ...createLegoBoxDto,
        price: 0,
      });
    });

    it('should throw DuplicateBoxNameException when box name already exists', async () => {
      const createLegoBoxDto = { name: 'Test Box' };
      const existingBox = { id: 1, name: 'Test Box' };

      jest
        .spyOn(legoBoxRepository, 'findOne')
        .mockResolvedValue(existingBox as LegoBox);

      await expect(service.createLegoBox(createLegoBoxDto)).rejects.toThrow(
        DuplicateBoxNameException,
      );
    });
  });

  describe('addComponents', () => {
    const mockBox = {
      id: 1,
      name: 'Test Box',
      price: 10,
    };

    const mockPiece = {
      id: 1,
      price: 5,
    };

    const addComponentsDto = {
      parent_item_id: 1,
      components: [
        {
          component_id: 1,
          component_type: 'piece' as const,
          amount: 2,
        },
      ],
    };

    beforeEach(() => {
      jest
        .spyOn(mockQueryRunner.manager, 'findOne')
        .mockResolvedValue(mockBox as LegoBox);
      jest
        .spyOn(legoPieceRepository, 'findOne')
        .mockResolvedValue(mockPiece as LegoPiece);
    });

    it('should add components successfully', async () => {
      const updatedBox = { ...mockBox, price: 20 };
      jest
        .spyOn(legoBoxRepository, 'findOne')
        .mockResolvedValue(updatedBox as LegoBox);
      jest.spyOn(mockQueryRunner.manager, 'save').mockResolvedValue({});

      const result = await service.addComponents(addComponentsDto);

      expect(result).toEqual(updatedBox);
      expect(mockQueryRunner.startTransaction).toHaveBeenCalled();
      expect(mockQueryRunner.commitTransaction).toHaveBeenCalled();
      expect(
        batchLegoBoxPriceUpdateProducer.queueBatchImport,
      ).toHaveBeenCalledWith(updatedBox);
    });

    it('should throw NotFoundException when box not found', async () => {
      jest.spyOn(mockQueryRunner.manager, 'findOne').mockResolvedValue(null);

      await expect(service.addComponents(addComponentsDto)).rejects.toThrow(
        NotFoundException,
      );
    });
  });

  describe('getTransactionHistory', () => {
    const mockBox = { id: 1, name: 'Test Box' };
    const mockTransactions = [
      { id: 1, box_id: 1, amount: 2, created_at: new Date() },
    ];

    it('should return cached transactions if available', async () => {
      jest.spyOn(cacheManager, 'get').mockResolvedValue(mockTransactions);

      const result = await service.getTransactionHistory(1);

      expect(result).toEqual(mockTransactions);
      expect(cacheManager.get).toHaveBeenCalledWith('transaction_history:1');
    });

    it('should fetch and cache transactions if not cached', async () => {
      const queryBuilder = {
        where: jest.fn().mockReturnThis(),
        getOne: jest.fn().mockResolvedValue(mockBox),
        leftJoinAndSelect: jest.fn().mockReturnThis(),
        select: jest.fn().mockReturnThis(),
        orderBy: jest.fn().mockReturnThis(),
        getMany: jest.fn().mockResolvedValue(mockTransactions),
      };

      jest.spyOn(cacheManager, 'get').mockResolvedValue(null);
      jest
        .spyOn(legoBoxRepository, 'createQueryBuilder')
        .mockImplementation(() => queryBuilder as any);

      const result = await service.getTransactionHistory(1);

      expect(result).toEqual(mockTransactions);
      expect(cacheManager.set).toHaveBeenCalledWith(
        'transaction_history:1',
        mockTransactions,
        0,
      );
    });

    it('should throw NotFoundException when box not found', async () => {
      const queryBuilder = {
        where: jest.fn().mockReturnThis(),
        getOne: jest.fn().mockResolvedValue(null),
        leftJoinAndSelect: jest.fn().mockReturnThis(),
        select: jest.fn().mockReturnThis(),
        orderBy: jest.fn().mockReturnThis(),
        getMany: jest.fn(),
      };

      jest.spyOn(cacheManager, 'get').mockResolvedValue(null);
      jest
        .spyOn(legoBoxRepository, 'createQueryBuilder')
        .mockImplementation(() => queryBuilder as any);

      await expect(service.getTransactionHistory(999)).rejects.toThrow(
        NotFoundException,
      );
    });
  });

  describe('createTransactionBox', () => {
    const mockBox = { id: 1, name: 'Test Box', price: 10 };
    const mockTransaction = { id: 1, total_price: 20 };

    it('should create a new transaction box successfully', async () => {
      const createTransactionBoxDto = { box_id: 1, amount: 2 };

      jest
        .spyOn(mockQueryRunner.manager, 'findOne')
        .mockResolvedValueOnce(mockBox) // for legoBox
        .mockResolvedValueOnce(null) // for transactionBox
        .mockResolvedValueOnce(mockTransaction); // for transaction

      jest.spyOn(transactionRepository, 'findOne').mockResolvedValue({
        ...(mockTransaction as Transaction),
      });

      const result = await service.createTransactionBox(
        createTransactionBoxDto,
      );

      expect(result).toBeDefined();
      expect(mockQueryRunner.startTransaction).toHaveBeenCalled();
      expect(mockQueryRunner.commitTransaction).toHaveBeenCalled();
      expect(mockQueryRunner.release).toHaveBeenCalled();
    });

    it('should update existing transaction box successfully', async () => {
      const createTransactionBoxDto = { box_id: 1, amount: 2 };
      const mockExistingTransactionBox = {
        id: 1,
        transaction_id: 1,
        lego_box_id: 1,
        amount: 1,
      } as TransactionBox;

      jest
        .spyOn(mockQueryRunner.manager, 'findOne')
        .mockResolvedValueOnce(mockBox) // for legoBox
        .mockResolvedValueOnce(mockExistingTransactionBox) // for transactionBox
        .mockResolvedValueOnce(mockTransaction); // for transaction

      jest.spyOn(transactionRepository, 'findOne').mockResolvedValue({
        ...(mockTransaction as Transaction),
        transactionBoxes: [mockExistingTransactionBox],
      });

      const result = await service.createTransactionBox(
        createTransactionBoxDto,
      );

      expect(result).toBeDefined();
      expect(mockQueryRunner.startTransaction).toHaveBeenCalled();
      expect(mockQueryRunner.commitTransaction).toHaveBeenCalled();
      expect(mockQueryRunner.release).toHaveBeenCalled();
    });

    it('should throw NotFoundException when box not found', async () => {
      const createTransactionBoxDto = { box_id: 999, amount: 2 };

      jest
        .spyOn(mockQueryRunner.manager, 'findOne')
        .mockRejectedValueOnce(new NotFoundException());

      await expect(
        service.createTransactionBox(createTransactionBoxDto),
      ).rejects.toThrow(NotFoundException);
    });
  });
});
