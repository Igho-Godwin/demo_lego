import {
  Entity,
  Column,
  ManyToOne,
  JoinColumn,
  PrimaryGeneratedColumn,
  Index,
  CreateDateColumn,
  UpdateDateColumn,
} from 'typeorm';
import { Transaction } from './transaction.entity';
import { LegoBox } from './lego-box.entity';

@Entity('transaction_boxes')
@Index(['lego_box', 'transaction'])
export class TransactionBox {
  @PrimaryGeneratedColumn()
  id: number;

  @Column('int', { nullable: false, name: 'lego_box_id' })
  lego_box_id: number;

  @ManyToOne(() => LegoBox, (legoBox) => legoBox.transactionBoxes)
  @JoinColumn({ name: 'lego_box_id' })
  lego_box: LegoBox;

  @Column('int', { nullable: false, name: 'transaction_id' })
  transaction_id: number;

  @ManyToOne(() => Transaction, (transaction) => transaction.transactionBoxes)
  @JoinColumn({ name: 'transaction_id' })
  transaction: Transaction;

  @Column('decimal', { precision: 10, scale: 2, nullable: false })
  amount: number;

  @CreateDateColumn({
    type: 'timestamptz',
    precision: 0,
    default: () => 'CURRENT_TIMESTAMP',
    nullable: false,
  })
  created_at: Date;

  @UpdateDateColumn({
    type: 'timestamptz',
    precision: 0,
    default: () => 'CURRENT_TIMESTAMP',
    onUpdate: 'CURRENT_TIMESTAMP',
    nullable: false,
  })
  updated_at: Date;
}
