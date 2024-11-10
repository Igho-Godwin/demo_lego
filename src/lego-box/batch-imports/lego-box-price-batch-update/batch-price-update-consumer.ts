import { Controller, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, DataSource } from 'typeorm';
import { Ctx, EventPattern, Payload, RmqContext } from '@nestjs/microservices';
import { LegoBoxJobLog } from '../../entities/lego-box-job-log.entity';
import { LegoBox } from '../../entities/lego-box.entity';
import { BatchLegoBoxPriceUpdateService } from './batch-lego-box-price-update-service';

interface JobData {
  batchId: string;
  data: LegoBox;
}

@Controller()
export class BatchLegoBoxPriceUpdateConsumer {
  private readonly logger = new Logger(BatchLegoBoxPriceUpdateConsumer.name);
  private isProcessing = false;

  constructor(
    private readonly batchLegoBoxPriceUpdateService: BatchLegoBoxPriceUpdateService,
    private readonly dataSource: DataSource,
    @InjectRepository(LegoBoxJobLog)
    private legoBoxJobLogRepository: Repository<LegoBoxJobLog>,
  ) {}

  @EventPattern('batch_lego_box_price_update')
  async processBatchPriceUpdate(
    @Payload() jobData: JobData,
    @Ctx() context: RmqContext,
  ): Promise<void> {
    const channel = context.getChannelRef();
    const originalMsg = context.getMessage();

    const { batchId, data } = jobData;

    try {
      this.logger.log(`Received message for batch ${batchId}`);

      if (this.isProcessing) {
        this.logger.warn('Already processing a message, requeuing...');
        channel.nack(originalMsg, false, true);
        return;
      }

      this.isProcessing = true;

      // Verify the job exists and is in correct state using queryRunner
      const job = await this.legoBoxJobLogRepository.findOne({
        where: { batch_id: batchId },
      });

      if (!job) {
        throw new Error(`No job found for batch ${batchId}`);
      }

      if (job.status !== 'pending') {
        this.logger.warn(`Job ${batchId} is in ${job.status} state, skipping`);
        channel.ack(originalMsg);
        return;
      }

      // Update job status to processing
      await this.legoBoxJobLogRepository.update(
        { batch_id: batchId },
        { status: 'processing' },
      );

      // Process the data
      await this.batchLegoBoxPriceUpdateService.processData(data);

      // Update job status to completed
      await this.legoBoxJobLogRepository.update(
        { batch_id: batchId },
        {
          status: 'completed',
          completed_at: new Date(),
        },
      );

      this.logger.log(`Successfully completed batch ${batchId}`);
      channel.ack(originalMsg);
    } catch (error) {
      this.logger.error(
        `Error processing batch ${batchId}: ${error.message}`,
        error.stack,
      );

      const shouldRequeue = this.isTemporaryError(error);
      channel.nack(originalMsg, false, shouldRequeue);
      throw error;
    } finally {
      this.isProcessing = false;
    }
  }

  private isTemporaryError(error: Error): boolean {
    return (
      error.message.includes('ETIMEDOUT') ||
      error.message.includes('ECONNREFUSED')
    );
  }
}
