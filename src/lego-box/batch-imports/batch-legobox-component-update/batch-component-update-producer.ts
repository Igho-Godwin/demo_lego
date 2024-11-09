import { Injectable, Inject } from '@nestjs/common';
import { ClientProxy } from '@nestjs/microservices';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { v4 as uuidv4 } from 'uuid';
import { firstValueFrom } from 'rxjs';
import { LegoBoxComponentUpdateJobLog } from '../../entities/legobox-component-update-job-log.entity';
import { SingleComponentImport } from '../../dtos/single-componet-import.dto';

@Injectable()
export class BatchComponentUpdateProducer {
  constructor(
    @Inject('BATCH_LEGBOX_COMPONENT_UPDATE') private client: ClientProxy,
    @InjectRepository(LegoBoxComponentUpdateJobLog)
    private legoBoxComponentUpdateJobLog: Repository<LegoBoxComponentUpdateJobLog>,
  ) {}

  async queueBatchImport(
    singleComponentImport: SingleComponentImport,
  ): Promise<void> {
    const batchId = uuidv4();
    try {
      const importLog = this.legoBoxComponentUpdateJobLog.create({
        batch_id: batchId,
        payload: singleComponentImport,
        status: 'pending',
      });
      await this.legoBoxComponentUpdateJobLog.save(importLog);

      // Changed from emit to send for request-response pattern
      await firstValueFrom(
        this.client.emit('batch_lego_box_component_update', {
          batchId,
          data: singleComponentImport,
        }),
      );
      //console.log(`Message sent successfully for batch ${batchId}`);
      return batchId;
    } catch (error) {
      console.error(`Failed to send message for batch ${batchId}:`, error);
      // Update the job log to failed status
      await this.legoBoxComponentUpdateJobLog.update(
        { batch_id: batchId },
        { status: 'failed', error: error.message },
      );
      throw error;
    }
  }
}
