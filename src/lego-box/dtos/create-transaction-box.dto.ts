import { IsNumber, Min } from 'class-validator';

export class CreateTransactionBoxDto {
  @IsNumber()
  @Min(1)
  box_id: number;

  @IsNumber()
  @Min(0.1)
  amount: number;
}
