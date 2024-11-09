import { MigrationInterface, QueryRunner } from 'typeorm';

export class AddDataToLegoPiecesTable1731152338850
  implements MigrationInterface
{
  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            INSERT INTO lego_pieces (name, price) VALUES
            ('Square', 0.10),
            ('Rectangle', 0.08),
            ('Circle', 0.07),
            ('Triangle', 0.06),
            ('Hexagon', 0.09);
        `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
            DELETE FROM lego_pieces 
            WHERE name IN ('Square', 'Rectangle', 'Circle', 'Triangle', 'Hexagon');
        `);
  }
}
