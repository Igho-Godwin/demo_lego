import {
  MigrationInterface,
  QueryRunner,
  Table,
  TableIndex,
  TableForeignKey,
} from 'typeorm';

export class CreateTransactionBoxTable1731148302631
  implements MigrationInterface
{
  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.createTable(
      new Table({
        name: 'transaction_boxes',
        columns: [
          {
            name: 'id',
            type: 'integer',
            isPrimary: true,
            isGenerated: true,
            generationStrategy: 'increment',
          },
          {
            name: 'lego_box_id',
            type: 'int',
            isNullable: false,
          },
          {
            name: 'transaction_id',
            type: 'int',
            isNullable: false,
          },
          {
            name: 'amount',
            type: 'decimal',
            precision: 10,
            scale: 2,
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

    // Create composite index on lego_box_id and transaction_id
    await queryRunner.createIndex(
      'transaction_boxes',
      new TableIndex({
        name: 'IDX_TRANSACTION_BOXES_LEGO_BOX_TRANSACTION',
        columnNames: ['lego_box_id', 'transaction_id'],
      }),
    );

    // Create foreign key for lego_box_id
    await queryRunner.createForeignKey(
      'transaction_boxes',
      new TableForeignKey({
        name: 'FK_TRANSACTION_BOXES_LEGO_BOX',
        columnNames: ['lego_box_id'],
        referencedColumnNames: ['id'],
        referencedTableName: 'lego_boxes',
      }),
    );

    // Create foreign key for transaction_id
    await queryRunner.createForeignKey(
      'transaction_boxes',
      new TableForeignKey({
        name: 'FK_TRANSACTION_BOXES_TRANSACTION',
        columnNames: ['transaction_id'],
        referencedColumnNames: ['id'],
        referencedTableName: 'transactions',
      }),
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    // Drop foreign keys first
    await queryRunner.dropForeignKey(
      'transaction_boxes',
      'FK_TRANSACTION_BOXES_TRANSACTION',
    );
    await queryRunner.dropForeignKey(
      'transaction_boxes',
      'FK_TRANSACTION_BOXES_LEGO_BOX',
    );

    // Drop index
    await queryRunner.dropIndex(
      'transaction_boxes',
      'IDX_TRANSACTION_BOXES_LEGO_BOX_TRANSACTION',
    );

    // Drop the table
    await queryRunner.dropTable('transaction_boxes');
  }
}
