import { Injectable, Logger } from '@nestjs/common';
import { QueryRunner } from 'typeorm';
import { LegoBox } from '../../entities/lego-box.entity';
import { ComponentNotFoundException } from 'src/common/exceptions/custom-exceptions';
import { LegoPiece } from 'src/lego-box/entities/lego-piece.entity';
import { LegoBoxComponent } from '../../entities/lego-box-component.entity';
import { SingleComponentImport } from 'src/lego-box/dtos/single-componet-import.dto';
import { BatchLegoBoxPriceUpdateProducer } from '../lego-box-price-batch-update/batch-price-update-producer';
import { LegoBoxService } from 'src/lego-box/lego-box.service';

@Injectable()
export class BatchLegoBoxComponentUpdateService {
  private readonly logger = new Logger(BatchLegoBoxComponentUpdateService.name);

  constructor(
    private readonly batchLegoBoxPriceUpdateProducer: BatchLegoBoxPriceUpdateProducer,
    private readonly legoBoxService: LegoBoxService,
  ) {}

  async processData(
    singleComponentImport: SingleComponentImport,
    queryRunner: QueryRunner,
  ): Promise<void> {
    try {
      this.logger.debug(
        `Processing component for box: ${singleComponentImport.name}`,
      );

      // Find or create lego box
      let legoBox = await queryRunner.manager.findOne(LegoBox, {
        where: { name: singleComponentImport.name },
      });

      if (!legoBox) {
        this.logger.debug(`Creating new box: ${singleComponentImport.name}`);
        legoBox = queryRunner.manager.create(LegoBox, {
          name: singleComponentImport.name,
          price: 0,
        });
        legoBox = await queryRunner.manager.save(LegoBox, legoBox);
      }

      if (!legoBox.id) {
        throw new Error(
          `Failed to create/find box: ${singleComponentImport.name}`,
        );
      }

      // Process based on component type
      if (singleComponentImport.component_type === 'piece') {
        await this.processPieceComponent(
          queryRunner,
          legoBox,
          singleComponentImport,
        );
      } else {
        await this.processBoxComponent(
          queryRunner,
          legoBox,
          singleComponentImport,
        );
      }
      const newBox = await queryRunner.manager.findOne(LegoBox, {
        where: { id: legoBox.id },
      });
      await this.batchLegoBoxPriceUpdateProducer.queueBatchImport(newBox);
      this.legoBoxService.invalidateTransactionHistoryCache(newBox.id);
    } catch (err) {
      this.logger.error(`Error processing data: ${err.message}`, err.stack);
      throw err;
    }
  }

  private async processPieceComponent(
    queryRunner: QueryRunner,
    legoBox: LegoBox,
    singleComponentImport: SingleComponentImport,
  ): Promise<void> {
    this.logger.debug(
      `Processing piece component ${singleComponentImport.component_id} for box ${legoBox.name}`,
    );

    const piece = await queryRunner.manager.findOne(LegoPiece, {
      where: { id: singleComponentImport.component_id },
    });

    if (!piece) {
      throw new ComponentNotFoundException(singleComponentImport.component_id);
    }

    const legoBoxComponent = queryRunner.manager.create(LegoBoxComponent, {
      parent_box_id: legoBox.id,
      component_type: 'piece',
      lego_piece_component_id: piece.id,
    });

    await queryRunner.manager.save(LegoBoxComponent, legoBoxComponent);

    // Update box price
    const updatedPrice = Number(legoBox.price) + Number(piece.price);
    await queryRunner.manager.update(
      LegoBox,
      { id: legoBox.id },
      { price: Number(updatedPrice.toFixed(2)) },
    );
  }

  private async processBoxComponent(
    queryRunner: QueryRunner,
    legoBox: LegoBox,
    singleComponentImport: SingleComponentImport,
  ): Promise<void> {
    this.logger.debug(
      `Processing box component ${singleComponentImport.component_id} for box ${legoBox.name}`,
    );

    const childBox = await queryRunner.manager.findOne(LegoBox, {
      where: { id: singleComponentImport.component_id },
    });

    if (!childBox) {
      throw new ComponentNotFoundException(singleComponentImport.component_id);
    }

    // Check for circular reference
    if (childBox.id === legoBox.id) {
      throw new Error(
        'Circular reference detected: A box cannot contain itself',
      );
    }

    const legoBoxComponent = queryRunner.manager.create(LegoBoxComponent, {
      parent_box_id: legoBox.id,
      component_type: 'box',
      lego_box_component_id: childBox.id,
    });

    await queryRunner.manager.save(LegoBoxComponent, legoBoxComponent);

    // Update box price
    const updatedPrice = Number(legoBox.price) + Number(childBox.price);
    await queryRunner.manager.update(
      LegoBox,
      { id: legoBox.id },
      { price: Number(updatedPrice.toFixed(2)) },
    );
    this.legoBoxService.invalidateTransactionHistoryCache(childBox.id);
  }
}
