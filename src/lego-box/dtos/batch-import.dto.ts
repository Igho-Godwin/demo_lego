import {
  IsArray,
  ValidateNested,
  IsNotEmpty,
  IsEnum,
  IsNumber,
  IsString,
  MinLength,
  MaxLength,
  Min,
} from 'class-validator';
import { Type } from 'class-transformer';

export class BatchComponentDto {
  @IsEnum(['piece', 'box'])
  component_type: 'piece' | 'box';

  @IsNumber()
  @Min(1)
  component_id: number;
}

export class BatchBoxDto {
  @IsString()
  @IsNotEmpty()
  @MinLength(3)
  @MaxLength(100)
  name: string;

  @ValidateNested({ each: true })
  @Type(() => BatchComponentDto)
  components: BatchComponentDto[];

  @IsNumber()
  @Min(0.1)
  amount: number;
}

export class BatchImportDto {
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => BatchBoxDto)
  boxes: BatchBoxDto[];
}
