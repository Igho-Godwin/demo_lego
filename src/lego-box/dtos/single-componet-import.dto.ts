import {
  IsNotEmpty,
  IsEnum,
  IsNumber,
  IsString,
  MinLength,
  MaxLength,
  Min,
} from 'class-validator';

export class SingleComponentImport {
  @IsString()
  @IsNotEmpty()
  @MinLength(3)
  @MaxLength(100)
  name: string;

  @IsEnum(['piece', 'box'])
  component_type: 'piece' | 'box';

  @IsNumber()
  @Min(1)
  component_id: number;
}
