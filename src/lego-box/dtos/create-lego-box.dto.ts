import { IsString, IsNotEmpty, MinLength, MaxLength } from 'class-validator';

export class CreateLegoBoxDto {
  @IsString()
  @IsNotEmpty()
  @MinLength(3)
  @MaxLength(100)
  name: string;
}
