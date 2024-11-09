import { IsNumber, Min } from 'class-validator';

export class GetTransactionHistoryDto {
  @IsNumber()
  @Min(1)
  box_id: number;
}
