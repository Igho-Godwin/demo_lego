import {
  MigrationInterface,
  QueryRunner,
  Table,
  TableForeignKey,
} from 'typeorm';

export class CreateLegoBoxComponentTable1731146340063
  implements MigrationInterface
{
  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.createTable(
      new Table({
        name: 'lego_box_components',
        columns: [
          {
            name: 'id',
            type: 'int',
            isPrimary: true,
            isGenerated: true,
            generationStrategy: 'increment',
          },
          {
            name: 'component_type',
            type: 'varchar',
            length: '10',
            isNullable: false,
          },
          {
            name: 'parent_box_id',
            type: 'int',
            isNullable: false,
          },
          {
            name: 'lego_box_component_id',
            type: 'int',
            isNullable: true,
          },
          {
            name: 'lego_piece_component_id',
            type: 'int',
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
        ],
      }),
      true,
    );

    // Foreign key for parent_box_id
    await queryRunner.createForeignKey(
      'lego_box_components',
      new TableForeignKey({
        columnNames: ['parent_box_id'],
        referencedColumnNames: ['id'],
        referencedTableName: 'lego_boxes',
      }),
    );

    // Foreign key for lego_box_component_id
    await queryRunner.createForeignKey(
      'lego_box_components',
      new TableForeignKey({
        columnNames: ['lego_box_component_id'],
        referencedColumnNames: ['id'],
        referencedTableName: 'lego_boxes',
        onDelete: 'CASCADE',
      }),
    );

    // Foreign key for lego_piece_component_id
    await queryRunner.createForeignKey(
      'lego_box_components',
      new TableForeignKey({
        columnNames: ['lego_piece_component_id'],
        referencedColumnNames: ['id'],
        referencedTableName: 'lego_pieces',
        onDelete: 'CASCADE',
      }),
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    const table = await queryRunner.getTable('lego_box_components');
    if (table) {
      const foreignKeys = table.foreignKeys;
      // Drop all foreign keys
      for (const foreignKey of foreignKeys) {
        await queryRunner.dropForeignKey('lego_box_components', foreignKey);
      }
    }
    await queryRunner.dropTable('lego_box_components');
  }
}
