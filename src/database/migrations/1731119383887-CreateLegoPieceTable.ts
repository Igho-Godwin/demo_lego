import { MigrationInterface, QueryRunner, Table, TableIndex } from 'typeorm';

export class CreateLegoPieceTable1731119383887 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.createTable(
      new Table({
        name: 'lego_pieces',
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
            length: '150',
            isNullable: false,
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

    // Create index on name column
    await queryRunner.createIndex(
      'lego_pieces',
      new TableIndex({
        name: 'IDX_LEGO_PIECES_NAME',
        columnNames: ['name'],
        isUnique: true,
      }),
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    // Drop index first
    await queryRunner.dropIndex('lego_pieces', 'IDX_LEGO_PIECES_NAME');

    // Drop the table
    await queryRunner.dropTable('lego_pieces');
  }
}
