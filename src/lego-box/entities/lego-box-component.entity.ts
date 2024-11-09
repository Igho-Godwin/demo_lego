import {
  Entity,
  Column,
  ManyToOne,
  JoinColumn,
  PrimaryGeneratedColumn,
  CreateDateColumn,
  UpdateDateColumn,
} from 'typeorm';
import { LegoBox } from './lego-box.entity';
import { LegoPiece } from './lego-piece.entity';

@Entity('lego_box_components')
export class LegoBoxComponent {
  @PrimaryGeneratedColumn()
  id: number;

  @ManyToOne(() => LegoBox, (box) => box.components)
  @JoinColumn({ name: 'parent_box_id' })
  parent_box: LegoBox;

  @Column('varchar', { length: 10, nullable: false })
  component_type: 'piece' | 'box';

  @Column('int', { nullable: false })
  parent_box_id: number;

  @Column('int', { nullable: true })
  lego_box_component_id: number;

  @Column('int', { nullable: true })
  lego_piece_component_id: number;

  @ManyToOne(() => LegoBox, {
    onDelete: 'CASCADE',
    nullable: true,
  })
  @JoinColumn([
    {
      name: 'lego_box_component_id',
    },
  ])
  box_component: LegoBox;

  @ManyToOne(() => LegoPiece, {
    onDelete: 'CASCADE',
    nullable: true,
  })
  @JoinColumn([
    {
      name: 'lego_piece_component_id',
    },
  ])
  piece_component: LegoPiece;
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
