import { Injectable } from '@nestjs/common';
import { QueryRunner, Repository } from 'typeorm';
import { LegoBox } from '../../entities/lego-box.entity';
import { LegoBoxComponent } from '../../entities/lego-box-component.entity';
import { TransactionBox } from 'src/lego-box/entities/transaction-box.entity';
import { LegoBoxService } from 'src/lego-box/lego-box.service';
import { InjectRepository } from '@nestjs/typeorm';

@Injectable()
export class BatchLegoBoxPriceUpdateService {
  constructor(
    private readonly legoBoxService: LegoBoxService,
    @InjectRepository(LegoBox)
    private readonly legoBoxRepository: Repository<LegoBox>,
    @InjectRepository(LegoBoxComponent)
    private readonly legoBoxComponentRepository: Repository<LegoBoxComponent>,
    @InjectRepository(TransactionBox)
    private readonly transactionBoxRepository: Repository<TransactionBox>,
  ) {}

  async processData(legoBox: LegoBox): Promise<void> {
    try {
      await this.updateTransaction(legoBox.id, Number(legoBox.price));
      this.processBox(legoBox);
    } catch (error) {
      // Log the error for debugging
      console.error('Error in processData:', error);
      throw error;
    }
  }

  private async updateTransaction(
    lego_box_id: number | undefined,
    boxTotalPrice: number,
  ): Promise<void> {
    if (!lego_box_id) return;

    try {
      const transactionBoxes = await this.transactionBoxRepository.find({
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
      await this.transactionBoxRepository.save(transactionBoxes[0].transaction);
    } catch (error) {
      console.error('Error in updateTransaction:', error);
      throw error;
    }
  }

  private async processBox(legoBox: LegoBox): Promise<void> {
    try {
      const boxComponents = await this.legoBoxComponentRepository.find({
        where: {
          lego_box_component_id: legoBox.id,
          component_type: 'box',
        },
      });

      let lastBoxIdWorkedOn = 0;
      for (const boxComponent of boxComponents) {
        if (boxComponent.parent_box_id === lastBoxIdWorkedOn) {
          continue;
        }

        const components = await this.legoBoxComponentRepository.find({
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

        const box = await this.legoBoxRepository.findOne({
          where: {
            id: boxComponent.parent_box_id,
          },
        });

        box.price = Number(boxTotalPrice.toFixed(2));

        lastBoxIdWorkedOn = box.id;

        await this.legoBoxRepository.save(box);
        await this.updateTransaction(box.id, boxTotalPrice);

        this.legoBoxService.invalidateTransactionHistoryCache(box.id);
        this.processBox(box);
      }
    } catch (error) {
      console.error('Error in processBox:', error);
      throw error;
    }
  }
}
