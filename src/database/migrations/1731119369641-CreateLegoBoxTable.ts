import { MigrationInterface, QueryRunner, Table, TableIndex } from 'typeorm';

export class CreateLegoBoxTable1731119369641 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.createTable(
      new Table({
        name: 'lego_boxes',
        columns: [
          {
            name: 'id',
            type: 'int',
            isPrimary: true,
            isGenerated: true,
            generationStrategy: 'increment',
          },
          {
            name: 'name',
            type: 'varchar',
            isNullable: false,
            isUnique: true,
          },
          {
            name: 'price',
            type: 'decimal',
            precision: 10,
            scale: 2,
            isNullable: false,
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
        ],
      }),
      true,
    );

    // Create unique index for name
    await queryRunner.createIndex(
      'lego_boxes',
      new TableIndex({
        name: 'IDX_LEGO_BOXES_NAME',
        columnNames: ['name'],
        isUnique: true,
      }),
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    const table = await queryRunner.getTable('lego_boxes');
    if (table) {
      // Drop index
      await queryRunner.dropIndex('lego_boxes', 'IDX_LEGO_BOXES_NAME');
    }

    // Drop table
    await queryRunner.dropTable('lego_boxes');
  }
}
