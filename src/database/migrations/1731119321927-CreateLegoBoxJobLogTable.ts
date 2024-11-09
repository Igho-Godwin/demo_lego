import { MigrationInterface, QueryRunner, Table } from 'typeorm';

export class CreateLegoBoxJobLogTable1731119321927
  implements MigrationInterface
{
  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
        CREATE TYPE job_status_enum AS ENUM (
          'pending',
          'processing',
          'completed',
          'failed'
        );
      `);
    await queryRunner.createTable(
      new Table({
        name: 'lego_box_job_logs',
        columns: [
          {
            name: 'id',
            type: 'int',
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
            type: 'job_status_enum',
            isNullable: false,
          },
          {
            name: 'error',
            type: 'text',
            isNullable: true,
          },
          {
            name: 'created_at',
            type: 'timestamptz',
            precision: 0,
            default: 'CURRENT_TIMESTAMP',
            isNullable: false,
          },
          {
            name: 'updated_at',
            type: 'timestamptz',
            precision: 0,
            default: 'CURRENT_TIMESTAMP',
            isNullable: false,
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
        ],
      }),
      true,
    );
    // Create index on batch_id for better query performance
    await queryRunner.query(`
            CREATE INDEX "IDX_LEGOBOX_UPDATE_JOB_BATCH_ID" 
            ON "lego_box_job_logs" ("batch_id");
          `);

    // Create index on status for better query performance
    await queryRunner.query(`
            CREATE INDEX "IDX_LEGOBOX_UPDATE_JOB_STATUS" 
            ON "lego_box_job_logs" ("status");
          `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    // Drop the check constraint first
    // Drop indexes first
    await queryRunner.query(
      `DROP INDEX IF EXISTS "IDX_LEGOBOX_UPDATE_JOB_STATUS"`,
    );
    await queryRunner.query(
      `DROP INDEX IF EXISTS "IDX_LEGOBOX_UPDATE_JOB_BATCH_ID"`,
    );
    // Drop the enum type
    await queryRunner.query(`DROP TYPE IF EXISTS job_status_enum`);
    // Drop the table
    await queryRunner.dropTable('lego_box_job_logs');
  }
}
