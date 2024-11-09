import { Controller } from '@nestjs/common';
import { EventPattern, Payload, Ctx, RmqContext } from '@nestjs/microservices';
import { Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, DataSource, QueryRunner } from 'typeorm';
import { LegoBoxComponentUpdateJobLog } from '../../entities/legobox-component-update-job-log.entity';
import { BatchLegoBoxComponentUpdateService } from './batch-legobox-component-update-service';
import { SingleComponentImport } from 'src/lego-box/dtos/single-componet-import.dto';

interface JobData {
  batchId: string;
  data: SingleComponentImport;
}

type JobStatus = 'pending' | 'processing' | 'completed' | 'failed';

@Controller()
export class BatchLegoBoxComponentUpdateConsumer {
  private readonly logger = new Logger(
    BatchLegoBoxComponentUpdateConsumer.name,
  );
  private isProcessing = false;

  constructor(
    private readonly batchLegoBoxComponentUpdateService: BatchLegoBoxComponentUpdateService,
    private readonly dataSource: DataSource,
    @InjectRepository(LegoBoxComponentUpdateJobLog)
    private legoBoxComponentUpdateJobLogRepository: Repository<LegoBoxComponentUpdateJobLog>,
  ) {
    // Log when consumer is initialized
    this.logger.log('Consumer initialized');
  }

  @EventPattern('batch_lego_box_component_update')
  async processBatchImport(
    @Payload() jobData: JobData,
    @Ctx() context: RmqContext,
  ): Promise<void> {
    const channel = context.getChannelRef();
    const originalMsg = context.getMessage();

    // Log received message
    this.logger.log(`Received message for batch ${jobData.batchId}`);

    if (this.isProcessing) {
      this.logger.warn('Already processing a message, requeuing...');
      channel.nack(originalMsg, false, true);
      return;
    }

    this.isProcessing = true;
    let queryRunner: QueryRunner | null = null;

    try {
      const { batchId, data } = jobData;

      // Verify the job exists and is in correct state
      const job = await this.legoBoxComponentUpdateJobLogRepository.findOne({
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

      this.logger.log(`Processing batch ${batchId}`);
      await this.updateJobStatus(batchId, 'processing');

      queryRunner = this.dataSource.createQueryRunner();
      await queryRunner.connect();
      await queryRunner.startTransaction();

      await this.batchLegoBoxComponentUpdateService.processData(
        data,
        queryRunner,
      );

      await queryRunner.commitTransaction();
      await this.updateJobStatus(batchId, 'completed');

      this.logger.log(`Successfully completed batch ${batchId}`);
      channel.ack(originalMsg);
    } catch (error) {
      this.logger.error(
        `Error processing batch ${jobData?.batchId}: ${error.message}`,
        error.stack,
      );

      if (queryRunner?.isTransactionActive) {
        await queryRunner.rollbackTransaction();
      }

      await this.updateJobStatus(jobData?.batchId, 'failed', error.message);

      // Only requeue if it's a temporary error
      const shouldRequeue = this.isTemporaryError(error);
      channel.nack(originalMsg, false, shouldRequeue);
    } finally {
      if (queryRunner) {
        await queryRunner.release();
      }
      this.isProcessing = false;
    }
  }

  private isTemporaryError(error: Error): boolean {
    // Add logic to determine if error is temporary (e.g., network issues)
    // Return true if the message should be requeued
    return (
      error.message.includes('ETIMEDOUT') ||
      error.message.includes('ECONNREFUSED')
    );
  }

  private async updateJobStatus(
    batchId: string,
    status: JobStatus,
    error?: string,
  ): Promise<void> {
    try {
      const updateData: Partial<LegoBoxComponentUpdateJobLog> = {
        status,
        completed_at: ['completed', 'failed'].includes(status)
          ? new Date()
          : null,
        error: error || null,
      };

      await this.legoBoxComponentUpdateJobLogRepository.update(
        { batch_id: batchId },
        updateData,
      );

      this.logger.log(`Updated job ${batchId} status to ${status}`);
    } catch (error) {
      this.logger.error(
        `Failed to update job status for batch ${batchId}: ${error.message}`,
      );
      throw error;
    }
  }
}
