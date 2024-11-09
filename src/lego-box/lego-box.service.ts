import {
  Injectable,
  NotFoundException,
  Inject,
  ForbiddenException,
} from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, DataSource, QueryRunner } from 'typeorm';
import { LegoBox } from './entities/lego-box.entity';
import { LegoPiece } from './entities/lego-piece.entity';
import { LegoBoxComponent } from './entities/lego-box-component.entity';
import { Transaction } from './entities/transaction.entity';
import { TransactionBox } from './entities/transaction-box.entity';
import { CreateLegoBoxDto } from './dtos/create-lego-box.dto';
import { AddComponentsDto } from './dtos/add-components.dto';
import { BatchComponentDto, BatchImportDto } from './dtos/batch-import.dto';
import {
  ComponentNotFoundException,
  DuplicateBoxNameException,
} from '../common/exceptions/custom-exceptions';
import { CACHE_MANAGER } from '@nestjs/cache-manager';
import { Cache } from 'cache-manager';
import { CreateTransactionBoxDto } from './dtos/create-transaction-box.dto';

import { BatchComponentUpdateProducer } from './batch-imports/batch-legobox-component-update/batch-component-update-producer';
import { BatchLegoBoxPriceUpdateProducer } from './batch-imports/lego-box-price-batch-update/batch-price-update-producer';
import { SingleComponentImport } from './dtos/single-componet-import.dto';
import { BatchImportResponseDto } from './dtos/batch-import-response.dto';

@Injectable()
export class LegoBoxService {
  private readonly CACHE_KEY_PREFIX = 'transaction_history:';

  constructor(
    @Inject(CACHE_MANAGER) private cacheManager: Cache,
    @InjectRepository(LegoBox)
    private readonly legoBoxRepository: Repository<LegoBox>,
    @InjectRepository(LegoPiece)
    private readonly legoPieceRepository: Repository<LegoPiece>,
    @InjectRepository(Transaction)
    private readonly transactionRepository: Repository<Transaction>,
    @InjectRepository(TransactionBox)
    private readonly transactionBoxRepository: Repository<TransactionBox>,
    private readonly dataSource: DataSource,
    private readonly batchLegoBoxPriceUpdateProducer: BatchLegoBoxPriceUpdateProducer,
    private readonly batchComponentUpdateProducer: BatchComponentUpdateProducer,
  ) {}

  async createLegoBox(createLegoBoxDto: CreateLegoBoxDto): Promise<LegoBox> {
    const existingBox = await this.legoBoxRepository.findOne({
      where: { name: createLegoBoxDto.name },
    });

    if (existingBox) {
      throw new DuplicateBoxNameException(createLegoBoxDto.name);
    }

    const legoBox = this.legoBoxRepository.create({
      ...createLegoBoxDto,
      price: 0,
    });

    return await this.legoBoxRepository.save(legoBox);
  }

  private async ensureTransactionActive(queryRunner: QueryRunner) {
    if (!queryRunner.isTransactionActive) {
      await queryRunner.startTransaction();
    }
  }

  private async safeRollback(queryRunner: QueryRunner) {
    try {
      if (queryRunner.isTransactionActive) {
        await queryRunner.rollbackTransaction();
      }
    } catch (error) {
      console.error('Error during rollback:', error);
    }
  }

  async addComponents(addComponentsDto: AddComponentsDto): Promise<LegoBox> {
    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    try {
      await this.ensureTransactionActive(queryRunner);
      const box = await this.legoBoxRepository.findOne({
        where: { id: addComponentsDto.parent_item_id },
      });

      if (!box) {
        throw new NotFoundException('Lego box not found');
      }

      let price = 0;

      for (const component of addComponentsDto.components) {
        if (component.component_type === 'piece') {
          const piece = await this.legoPieceRepository.findOne({
            where: { id: component.component_id },
          });

          if (!piece) {
            throw new ComponentNotFoundException(component.component_id);
          }

          price += Number(piece.price) * Number(component.amount);

          const components = Array.from({ length: component.amount }, () => ({
            parent_box_id: box.id,
            component_type: component.component_type,
            lego_piece_component_id: piece.id,
          }));

          await queryRunner.manager.save(LegoBoxComponent, components);
        } else {
          const childBox = await this.legoBoxRepository.findOne({
            where: { id: component.component_id },
            relations: ['components'],
          });

          if (!childBox || !childBox.components[0]) {
            throw new ComponentNotFoundException(component.component_id);
          }

          if (box.id === childBox.id) {
            throw new ForbiddenException(
              `parent box id ${box.id} is thesame with child box`,
            );
          }

          const existingBox = childBox.components.find(
            (boxTemp) => boxTemp.lego_box_component_id === box.id,
          );

          if (existingBox) {
            throw new ForbiddenException(
              ` box id ${box.id} already in box ${existingBox.parent_box_id} `,
            );
          }

          price += Number(childBox.price) * Number(component.amount);

          const components = Array.from({ length: component.amount }, () => ({
            parent_box_id: box.id,
            component_type: component.component_type,
            lego_box_component_id: childBox.id,
          }));

          await queryRunner.manager.save(LegoBoxComponent, components);
        }
      }

      await queryRunner.manager.update(
        LegoBox,
        { id: box.id },
        { price: Number((Number(box.price) + price).toFixed(2)) },
      );

      await queryRunner.commitTransaction();
      const updatedBox = await this.legoBoxRepository.findOne({
        where: { id: box.id },
        relations: ['components'],
      });
      await this.batchLegoBoxPriceUpdateProducer.queueBatchImport(updatedBox);
      this.invalidateTransactionHistoryCache(updatedBox.id);
      return updatedBox;
    } catch (err) {
      await this.safeRollback(queryRunner);
      throw err;
    } finally {
      await queryRunner.release();
    }
  }

  async batchImport(
    batchImportDto: BatchImportDto,
  ): Promise<BatchImportResponseDto> {
    const boxes = batchImportDto.boxes;
    for (const box of boxes) {
      for (const component of box.components) {
        await this.validateBoxComponent(component, box.name);
      }
    }
    for (const box of boxes) {
      for (const component of box.components) {
        for (let i = 0; i < box.amount; i++) {
          this.queueComponent(box.name, component);
        }
      }
    }
    return { message: 'Batch Queuing Successful' };
  }

  private async queueComponent(
    boxName: string,
    component: BatchComponentDto,
  ): Promise<void> {
    const componentImport: SingleComponentImport = {
      name: boxName,
      component_id: component.component_id,
      component_type: component.component_type,
    };

    await this.batchComponentUpdateProducer.queueBatchImport(componentImport);
  }

  async validateBoxComponent(
    component: BatchComponentDto,
    boxName: string,
  ): Promise<void> {
    if (component.component_type === 'piece') {
      const piece = await this.legoPieceRepository.findOne({
        where: { id: component.component_id },
      });

      if (!piece) {
        throw new ComponentNotFoundException(component.component_id);
      }
    } else {
      const box = await this.legoBoxRepository.findOne({
        where: { id: component.component_id },
        relations: ['components'],
      });

      if (!box || !box.components[0]) {
        throw new ComponentNotFoundException(component.component_id);
      }

      const parentBox = await this.legoBoxRepository.findOne({
        where: { name: boxName },
        relations: ['components'],
      });

      if (parentBox.id === box.id) {
        throw new ForbiddenException(
          `parent box id ${component.component_id} is thesame with child box`,
        );
      }

      const existingBox = box.components.find(
        (boxTemp) => boxTemp.lego_box_component_id === parentBox.id,
      );

      if (existingBox) {
        throw new ForbiddenException(
          `box id ${parentBox.id}  already in box ${existingBox.parent_box_id}`,
        );
      }
    }
  }

  async getTransactionHistory(boxId: number): Promise<LegoBox[]> {
    const cacheKey = `${this.CACHE_KEY_PREFIX}${boxId}`;

    // Try to get from cache first
    const cachedTransactions = await this.cacheManager.get<LegoBox[]>(cacheKey);
    if (cachedTransactions) {
      return cachedTransactions;
    }

    // First verify the box exists
    const boxExists = await this.legoBoxRepository
      .createQueryBuilder('legoBox')
      .where('legoBox.id = :id', { id: boxId })
      .getOne();

    if (!boxExists) {
      throw new NotFoundException(`Lego box with ID ${boxId} not found`);
    }

    // Now get the transactions
    const queryBuilder = this.legoBoxRepository
      .createQueryBuilder('legoBox')
      .leftJoinAndSelect('legoBox.transactionBoxes', 'transactionBox')
      .leftJoinAndSelect('transactionBox.transaction', 'transaction')
      .where('legoBox.id = :id', { id: boxId })
      .select(['legoBox', 'transactionBox', 'transaction'])
      .orderBy('transaction.created_at', 'DESC');

    const transactions = await queryBuilder.getMany();

    await this.cacheManager.set(cacheKey, transactions, 0);

    return transactions;
  }

  async invalidateTransactionHistoryCache(boxId: number): Promise<void> {
    const cacheKey = `${this.CACHE_KEY_PREFIX}${boxId}`;
    await this.cacheManager.del(cacheKey);
  }

  async createTransactionBox(
    createTransactionBoxDto: CreateTransactionBoxDto,
  ): Promise<Transaction> {
    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    try {
      const { box_id, amount } = createTransactionBoxDto;

      const legoBox = await queryRunner.manager.findOne(LegoBox, {
        where: { id: box_id },
      });

      if (!legoBox) {
        throw new NotFoundException('Lego box not found');
      }

      let transaction: Transaction;
      const transactionBox = await queryRunner.manager.findOne(TransactionBox, {
        where: { lego_box_id: box_id },
      });

      if (!transactionBox) {
        // Create new transaction
        transaction = await queryRunner.manager.save(Transaction, {
          total_price: Number((legoBox.price * amount).toFixed(2)),
        });

        await queryRunner.manager.save(TransactionBox, {
          transaction_id: transaction.id,
          lego_box_id: box_id,
          amount,
        });
      } else {
        // Update existing transaction
        transaction = await queryRunner.manager.findOne(Transaction, {
          where: { id: transactionBox.transaction_id },
        });

        if (!transaction) {
          throw new Error('Transaction not found');
        }

        transaction.total_price = Number(
          (
            Number(legoBox.price * amount) + Number(transaction.total_price)
          ).toFixed(2),
        );

        await queryRunner.manager.save(transaction);

        await queryRunner.manager.save(TransactionBox, {
          transaction_id: transaction.id,
          lego_box_id: box_id,
          amount,
        });
      }

      await queryRunner.commitTransaction();

      const updatedTransaction = await this.transactionRepository.findOne({
        where: { id: transaction.id },
        relations: ['transactionBoxes'],
      });

      await this.invalidateTransactionHistoryCache(box_id);
      return updatedTransaction;
    } catch (error) {
      await queryRunner.rollbackTransaction();
      throw error;
    } finally {
      await queryRunner.release();
    }
  }
}
