import { MigrationInterface, QueryRunner, Table } from 'typeorm';

export class CreateLegoBoxComponentUpdateJobLog1731119405640
  implements MigrationInterface
{
  public async up(queryRunner: QueryRunner): Promise<void> {
    // Create enum type for status
    await queryRunner.query(`
          CREATE TYPE job_status_enum_box AS ENUM (
            'pending',
            'processing',
            'completed',
            'failed'
          );
        `);

    await queryRunner.createTable(
      new Table({
        name: 'legobox_component_update_job_logs',
        columns: [
          {
            name: 'id',
            type: 'integer',
            isPrimary: true,
            isGenerated: true,
            generationStrategy: 'increment',
          },
          {
            name: 'payload',
            type: 'jsonb',
            isNullable: false,
          },
          {
            name: 'status',
            type: 'job_status_enum_box',
            isNullable: false,
          },
          {
            name: 'error',
            type: 'text',
            isNullable: true,
          },
          {
            name: 'completed_at',
            type: 'timestamptz',
            isNullable: true,
          },
          {
            name: 'batch_id',
            type: 'uuid',
            isNullable: false,
          },
          {
            name: 'created_at',
            type: 'timestamptz',
            precision: 0,
            isNullable: false,
            default: 'CURRENT_TIMESTAMP',
          },
          {
            name: 'updated_at',
            type: 'timestamptz',
            precision: 0,
            isNullable: false,
            default: 'CURRENT_TIMESTAMP',
          },
        ],
      }),
      true,
    );

    // Create index on batch_id for better query performance
    await queryRunner.query(`
          CREATE INDEX "IDX_LEGOBOX_UPDATE_JOB_BATCH_ID_COMPONENT" 
          ON "legobox_component_update_job_logs" ("batch_id");
        `);

    // Create index on status for better query performance
    await queryRunner.query(`
          CREATE INDEX "IDX_LEGOBOX_UPDATE_JOB_STATUS_COMPONENT" 
          ON "legobox_component_update_job_logs" ("status");
        `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    // Drop indexes first
    await queryRunner.query(
      `DROP INDEX "IDX_LEGOBOX_UPDATE_JOB_BATCH_ID_COMPONENT"`,
    );
    await queryRunner.query(
      `DROP INDEX "IDX_LEGOBOX_UPDATE_JOB_STATUS_COMPONENT"`,
    );

    // Drop the table
    await queryRunner.dropTable('legobox_component_update_job_log');

    // Drop the enum type
    await queryRunner.query(`DROP TYPE job_status_enum_box`);
  }
}
