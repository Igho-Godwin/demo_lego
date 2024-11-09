import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  Index,
  OneToMany,
  CreateDateColumn,
  UpdateDateColumn,
} from 'typeorm';
import { LegoBoxComponent } from './lego-box-component.entity';

@Entity('lego_pieces')
@Index(['name'], { unique: true })
export class LegoPiece {
  @PrimaryGeneratedColumn()
  id: number;

  @Column('varchar', { length: 150, nullable: false })
  name: string;

  @Column('decimal', { precision: 10, scale: 2, nullable: false })
  price: number;

  @OneToMany(() => LegoBoxComponent, (component) => component.piece_component)
  components: LegoBoxComponent[];

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
