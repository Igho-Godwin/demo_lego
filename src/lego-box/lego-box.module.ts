import { Module } from '@nestjs/common';
import { ClientsModule, Transport, RmqOptions } from '@nestjs/microservices';
import * as redisStore from 'cache-manager-redis-store';
import { TypeOrmModule } from '@nestjs/typeorm';

import { LegoBoxController } from './lego-box.controller';
import { LegoBoxService } from './lego-box.service';
import { LegoBox } from './entities/lego-box.entity';
import { LegoPiece } from './entities/lego-piece.entity';
import { LegoBoxComponent } from './entities/lego-box-component.entity';
import { Transaction } from './entities/transaction.entity';
import { TransactionBox } from './entities/transaction-box.entity';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { LegoBoxJobLog } from './entities/lego-box-job-log.entity';
import { LegoBoxComponentUpdateJobLog } from './entities/legobox-component-update-job-log.entity';
import { CacheModule } from '@nestjs/cache-manager';
import { createClient } from '@redis/client';

import { BatchLegoBoxPriceUpdateProducer } from './batch-imports/lego-box-price-batch-update/batch-price-update-producer';
import { BatchComponentUpdateProducer } from './batch-imports/batch-legobox-component-update/batch-component-update-producer';
import { BatchLegoBoxComponentUpdateConsumer } from './batch-imports/batch-legobox-component-update/batch-component-update-consumer';
import { BatchLegoBoxComponentUpdateService } from './batch-imports/batch-legobox-component-update/batch-legobox-component-update-service';
import { BatchLegoBoxPriceUpdateConsumer } from './batch-imports/lego-box-price-batch-update/batch-price-update-consumer';
import { BatchLegoBoxPriceUpdateService } from './batch-imports/lego-box-price-batch-update/batch-lego-box-price-update-service';

@Module({
  imports: [
    CacheModule.registerAsync({
      imports: [ConfigModule],
      useFactory: async (configService: ConfigService) => {
        const redisClient = createClient({
          url: `redis://${configService.get('REDIS_HOST')}:${configService.get(
            'REDIS_PORT',
          )}`,
          socket: {
            reconnectStrategy: (retries) => {
              if (retries > 10) return new Error('Redis max retries reached');
              return Math.min(retries * 50, 1000);
            },
          },
        });

        redisClient.on('error', (err) =>
          console.error('Redis Client Error:', err),
        );
        redisClient.on('connect', () => console.log('Redis Client Connected'));
        redisClient.on('ready', () => console.log('Redis Client Ready'));

        await redisClient.connect();

        return {
          store: redisStore,
          client: redisClient,
          ttl: 300,
          max: 1000000000,
          isGlobal: true,
        };
      },
      inject: [ConfigService],
    }),
    ClientsModule.registerAsync([
      {
        imports: [ConfigModule],
        name: 'BATCH_LEGBOX_PRICE_UPDATE',
        useFactory: async (
          configService: ConfigService,
        ): Promise<RmqOptions> => ({
          transport: Transport.RMQ,
          options: {
            urls: [configService.get<string>('RABBITMQ_URL')],
            queue: 'legobox_price_update_queue',
            queueOptions: {
              durable: true,
            },
          },
        }),
        inject: [ConfigService],
      },
      {
        imports: [ConfigModule],
        name: 'BATCH_LEGBOX_COMPONENT_UPDATE',
        useFactory: async (
          configService: ConfigService,
        ): Promise<RmqOptions> => ({
          transport: Transport.RMQ,
          options: {
            urls: [configService.get<string>('RABBITMQ_URL')],
            queue: 'batch_legobox_component_update_queue',
            queueOptions: {
              durable: true,
            },
            persistent: true,
            noAck: false,
            prefetchCount: 1,
          },
        }),
        inject: [ConfigService],
      },
    ]),
    TypeOrmModule.forFeature([
      LegoBox,
      LegoPiece,
      LegoBoxComponent,
      Transaction,
      TransactionBox,
      LegoBoxJobLog,
      LegoBoxComponentUpdateJobLog,
    ]),
  ],
  controllers: [
    LegoBoxController,
    BatchLegoBoxComponentUpdateConsumer,
    BatchLegoBoxPriceUpdateConsumer,
  ],
  providers: [
    LegoBoxService,
    BatchLegoBoxPriceUpdateProducer,
    BatchComponentUpdateProducer,
    BatchLegoBoxComponentUpdateService,
    BatchLegoBoxPriceUpdateService,
  ],
})
export class LegoBoxModule {}
