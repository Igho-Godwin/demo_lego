import { Injectable } from '@nestjs/common';
import { QueryRunner } from 'typeorm';
import { LegoBox } from '../../entities/lego-box.entity';
import { LegoBoxComponent } from '../../entities/lego-box-component.entity';
import { TransactionBox } from 'src/lego-box/entities/transaction-box.entity';
import { LegoBoxService } from 'src/lego-box/lego-box.service';

@Injectable()
export class BatchLegoBoxPriceUpdateService {
  constructor(private readonly legoBoxService: LegoBoxService) {}

  async processData(legoBox: LegoBox, queryRunner: QueryRunner): Promise<void> {
    try {
      // Check if queryRunner is still valid
      if (queryRunner.isReleased) {
        throw new Error('QueryRunner is already released');
      }

      // Find box components
      const boxComponents = await queryRunner.manager.find(LegoBoxComponent, {
        where: {
          lego_box_component_id: legoBox.id,
          component_type: 'box',
        },
      });

      await this.updateTransaction(
        legoBox.id,
        Number(legoBox.price),
        queryRunner,
      );
      let lastBoxIdWorkedOn = 0;
      // Process each box component
      for (const boxComponent of boxComponents) {
        // Check queryRunner status before each major operation
        if (queryRunner.isReleased) {
          throw new Error('QueryRunner was released during processing');
        }

        if (boxComponent.parent_box_id === lastBoxIdWorkedOn) {
          continue;
        }

        const components = await queryRunner.manager.find(LegoBoxComponent, {
          where: {
            parent_box_id: boxComponent.parent_box_id,
          },
          relations: ['piece_component', 'box_component'],
        });

        let boxTotalPrice = 0;

        // Calculate total price
        for (const component of components) {
          if (
            component.component_type === 'piece' &&
            component.piece_component
          ) {
            boxTotalPrice += Number(component.piece_component.price);
          } else if (component.box_component) {
            boxTotalPrice += Number(component.box_component.price);
          }
        }

        const box = await queryRunner.manager.findOne(LegoBox, {
          where: {
            id: boxComponent.parent_box_id,
          },
        });

        box.price = Number(boxTotalPrice.toFixed(2));

        lastBoxIdWorkedOn = box.id;

        await queryRunner.manager.save(LegoBox, box);
        await this.updateTransaction(box.id, boxTotalPrice, queryRunner);

        this.legoBoxService.invalidateTransactionHistoryCache(box.id);
      }
    } catch (error) {
      // Log the error for debugging
      console.error('Error in processData:', error);
      throw error;
    }
  }

  private async updateTransaction(
    lego_box_id: number | undefined,
    boxTotalPrice: number,
    queryRunner: QueryRunner,
  ): Promise<void> {
    if (!lego_box_id) return;

    try {
      // Check queryRunner status
      if (queryRunner.isReleased) {
        throw new Error('QueryRunner is already released');
      }

      const transactionBoxes = await queryRunner.manager.find(TransactionBox, {
        where: {
          lego_box_id: lego_box_id,
        },
        relations: ['transaction'],
      });

      if (!transactionBoxes.length) return;

      let transactionTotalPrice = 0;

      // Process each transaction box
      for (const transactionBox of transactionBoxes) {
        if (!transactionBox.transaction) continue;

        // Calculate new transaction total
        transactionTotalPrice +=
          Number(transactionBox.amount) * Number(boxTotalPrice);
      }
      // Update transaction
      transactionBoxes[0].transaction.total_price = Number(
        transactionTotalPrice.toFixed(2),
      );
      await queryRunner.manager.save(transactionBoxes[0].transaction);
    } catch (error) {
      console.error('Error in updateTransaction:', error);
      throw error;
    }
  }
}
