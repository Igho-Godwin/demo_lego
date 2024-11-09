import { Injectable, Inject, Logger } from '@nestjs/common';
import { ClientProxy } from '@nestjs/microservices';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { v4 as uuidv4 } from 'uuid';
import { LegoBoxJobLog } from '../../entities/lego-box-job-log.entity';
import { firstValueFrom } from 'rxjs';
import { LegoBox } from 'src/lego-box/entities/lego-box.entity';

@Injectable()
export class BatchLegoBoxPriceUpdateProducer {
  private readonly logger = new Logger(BatchLegoBoxPriceUpdateProducer.name);

  constructor(
    @Inject('BATCH_LEGBOX_PRICE_UPDATE') private client: ClientProxy,
    @InjectRepository(LegoBoxJobLog)
    private legoBoxJobLogRepository: Repository<LegoBoxJobLog>,
  ) {}

  async queueBatchImport(legoBox: LegoBox): Promise<string> {
    const batchId = uuidv4();
    try {
      // Create import log
      const importLog = this.legoBoxJobLogRepository.create({
        batch_id: batchId,
        payload: legoBox,
        status: 'pending',
      });
      await this.legoBoxJobLogRepository.save(importLog);

      await firstValueFrom(
        this.client.emit('batch_lego_box_price_update', {
          batchId,
          data: legoBox,
        }),
      );

      return batchId;
    } catch (error) {
      this.logger.error(`Error processing data: ${error.message}`, error.stack);
      // Update the job log to failed status
      await this.legoBoxJobLogRepository.update(
        { batch_id: batchId },
        { status: 'failed', error: error.message },
      );
      throw error;
    }
  }

  async getBatchStatus(batchId: string): Promise<LegoBoxJobLog> {
    return this.legoBoxJobLogRepository.findOne({
      where: { batch_id: batchId },
    });
  }
}
