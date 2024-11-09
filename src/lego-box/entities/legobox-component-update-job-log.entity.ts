import {
  Entity,
  Column,
  PrimaryGeneratedColumn,
  CreateDateColumn,
  UpdateDateColumn,
} from 'typeorm';
import { LegoBox } from './lego-box.entity';

@Entity('legobox_component_update_job_logs')
export class LegoBoxComponentUpdateJobLog {
  @PrimaryGeneratedColumn()
  id: number;

  @Column('jsonb')
  payload: LegoBox;

  @Column('varchar')
  status: 'pending' | 'processing' | 'completed' | 'failed';

  @Column('text', { nullable: true })
  error?: string;

  @Column('timestamptz', { nullable: true })
  completed_at?: Date;

  @Column('uuid')
  batch_id: string;

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
