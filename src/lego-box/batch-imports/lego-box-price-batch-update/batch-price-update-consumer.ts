import { Controller, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, DataSource, QueryRunner } from 'typeorm';
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
    let queryRunner: QueryRunner | null = null;

    try {
      this.logger.log(`Received message for batch ${batchId}`);

      if (this.isProcessing) {
        this.logger.warn('Already processing a message, requeuing...');
        channel.nack(originalMsg, false, true);
        return;
      }

      this.isProcessing = true;

      // Create and start transaction
      queryRunner = this.dataSource.createQueryRunner();
      await queryRunner.connect();
      await queryRunner.startTransaction();

      // Verify the job exists and is in correct state using queryRunner
      const job = await queryRunner.manager.findOne(LegoBoxJobLog, {
        where: { batch_id: batchId },
      });

      if (!job) {
        throw new Error(`No job found for batch ${batchId}`);
      }

      if (job.status !== 'pending') {
        this.logger.warn(`Job ${batchId} is in ${job.status} state, skipping`);
        await queryRunner.commitTransaction();
        channel.ack(originalMsg);
        return;
      }

      // Update job status to processing
      await queryRunner.manager.update(
        LegoBoxJobLog,
        { batch_id: batchId },
        { status: 'processing' },
      );

      // Process the data
      await this.batchLegoBoxPriceUpdateService.processData(data, queryRunner);

      // Update job status to completed
      await queryRunner.manager.update(
        LegoBoxJobLog,
        { batch_id: batchId },
        {
          status: 'completed',
          completed_at: new Date(),
        },
      );

      // Commit the transaction
      await queryRunner.commitTransaction();

      this.logger.log(`Successfully completed batch ${batchId}`);
      channel.ack(originalMsg);
    } catch (error) {
      this.logger.error(
        `Error processing batch ${batchId}: ${error.message}`,
        error.stack,
      );

      if (queryRunner) {
        try {
          // Rollback the transaction
          await queryRunner.rollbackTransaction();

          // Update job status to failed using repository directly
          await this.legoBoxJobLogRepository.update(
            { batch_id: batchId },
            {
              status: 'failed',
              error: error.message,
              completed_at: new Date(),
            },
          );
        } catch (rollbackError) {
          this.logger.error(
            `Error during rollback: ${rollbackError.message}`,
            rollbackError.stack,
          );
        }
      }

      const shouldRequeue = this.isTemporaryError(error);
      channel.nack(originalMsg, false, shouldRequeue);
      throw error;
    } finally {
      if (queryRunner) {
        try {
          // Release the query runner
          await queryRunner.release();
        } catch (releaseError) {
          this.logger.error(
            `Error releasing query runner: ${releaseError.message}`,
            releaseError.stack,
          );
        }
      }
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
