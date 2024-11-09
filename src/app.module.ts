import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { LegoBoxModule } from './lego-box/lego-box.module';
import { CacheModule } from '@nestjs/cache-manager';
import { createClient } from '@redis/client';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { TypeOrmModule } from '@nestjs/typeorm';
import { LegoBox } from './lego-box/entities/lego-box.entity';
import { LegoPiece } from './lego-box/entities/lego-piece.entity';
import { LegoBoxComponent } from './lego-box/entities/lego-box-component.entity';
import { Transaction } from './lego-box/entities/transaction.entity';
import { TransactionBox } from './lego-box/entities/transaction-box.entity';
import { LegoBoxJobLog } from './lego-box/entities/lego-box-job-log.entity';
import { LegoBoxComponentUpdateJobLog } from './lego-box/entities/legobox-component-update-job-log.entity';

@Module({
  imports: [
    LegoBoxModule,
    ConfigModule.forRoot({
      isGlobal: true,
    }),
    TypeOrmModule.forRootAsync({
      imports: [ConfigModule],
      useFactory: (configService: ConfigService) => ({
        type: 'postgres',
        host: configService.get('DB_HOST'),
        port: configService.get('DB_PORT'),
        username: configService.get('DB_USER'),
        password: configService.get('DB_PASSWORD'),
        database: configService.get('DB_NAME'),
        entities: [
          LegoBox,
          LegoPiece,
          LegoBoxComponent,
          Transaction,
          TransactionBox,
          LegoBoxJobLog,
          LegoBoxComponentUpdateJobLog,
        ],
        migrations: ['dist/database/migrations/*{.ts,.js}'],
        migrationsTableName: 'migrations_typeorm',
        migrationsRun: true,
      }),
      inject: [ConfigService],
    }),
    CacheModule.registerAsync({
      imports: [ConfigModule],
      useFactory: async (configService: ConfigService) => {
        const client = createClient({
          socket: {
            host: configService.get('REDIS_HOST'),
            port: configService.get('REDIS_PORT'),
          },
        });

        await client.connect();

        return {
          store: 'redis',
          client,
          ttl: 0,
        };
      },
      inject: [ConfigService],
    }),
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
