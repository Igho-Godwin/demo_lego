import {
  IsNumber,
  IsArray,
  ValidateNested,
  IsEnum,
  Min,
} from 'class-validator';
import { Type } from 'class-transformer';

export class ComponentDto {
  @IsEnum(['piece', 'box'])
  component_type: 'piece' | 'box';

  @IsNumber()
  @Min(1)
  component_id: number;

  @IsNumber()
  @Min(0.1)
  amount: number;
}

export class AddComponentsDto {
  @IsNumber()
  @Min(1)
  parent_item_id: number;

  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => ComponentDto)
  components: ComponentDto[];
}
