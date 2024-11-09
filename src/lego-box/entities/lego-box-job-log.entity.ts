import {
  Entity,
  Column,
  PrimaryGeneratedColumn,
  CreateDateColumn,
  UpdateDateColumn,
} from 'typeorm';
import { LegoBox } from './lego-box.entity';

@Entity('lego_box_job_logs')
export class LegoBoxJobLog {
  @PrimaryGeneratedColumn()
  id: number;

  @Column('jsonb')
  payload: LegoBox;

  @Column('varchar', { length: 20, nullable: false })
  status: 'pending' | 'processing' | 'completed' | 'failed';

  @Column('text', { nullable: true })
  error?: string;

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

  @Column('timestamptz', { nullable: true })
  completed_at?: Date;

  @Column('uuid', { nullable: false })
  batch_id: string;
}
