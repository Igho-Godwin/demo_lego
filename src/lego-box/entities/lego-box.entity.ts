import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  OneToMany,
  Index,
  CreateDateColumn,
  UpdateDateColumn,
} from 'typeorm';

import { LegoBoxComponent } from './lego-box-component.entity';
import { TransactionBox } from './transaction-box.entity';

@Entity('lego_boxes')
@Index(['name'], { unique: true })
export class LegoBox {
  @PrimaryGeneratedColumn()
  id: number;

  @Column('varchar', { unique: true, nullable: false })
  name: string;

  @Column('decimal', { precision: 10, scale: 2 })
  price: number;

  @OneToMany(() => LegoBoxComponent, (component) => component.parent_box)
  components?: LegoBoxComponent[];

  @OneToMany(() => LegoBoxComponent, (component) => component.box_component)
  box_components: LegoBoxComponent[];

  @OneToMany(() => TransactionBox, (transactionBox) => transactionBox.lego_box)
  transactionBoxes: TransactionBox[];

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
