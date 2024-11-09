import { Controller, Post, Body, Get, Param } from '@nestjs/common';
import { LegoBoxService } from './lego-box.service';
import { CreateLegoBoxDto } from './dtos/create-lego-box.dto';
import { AddComponentsDto } from './dtos/add-components.dto';
import { BatchImportDto } from './dtos/batch-import.dto';
import { LegoBox } from './entities/lego-box.entity';
import { Transaction } from './entities/transaction.entity';
import { CreateTransactionBoxDto } from './dtos/create-transaction-box.dto';
import { TransactionBox } from './entities/transaction-box.entity';
import { BatchImportResponseDto } from './dtos/batch-import-response.dto';

@Controller('lego-box')
export class LegoBoxController {
  constructor(private readonly legoBoxService: LegoBoxService) {}

  @Post()
  createLegoBox(@Body() createLegoBoxDto: CreateLegoBoxDto): Promise<LegoBox> {
    return this.legoBoxService.createLegoBox(createLegoBoxDto);
  }

  @Post('create/components')
  addComponents(@Body() addComponentsDto: AddComponentsDto): Promise<LegoBox> {
    return this.legoBoxService.addComponents(addComponentsDto);
  }

  @Post('batch-import')
  batchImport(
    @Body() batchImportDto: BatchImportDto,
  ): Promise<BatchImportResponseDto> {
    return this.legoBoxService.batchImport(batchImportDto);
  }

  @Get('transaction-history/:boxId')
  getTransactionHistory(@Param('boxId') boxId: number): Promise<LegoBox[]> {
    return this.legoBoxService.getTransactionHistory(boxId);
  }

  @Post('create/transactionBox')
  async createTransactionBox(
    @Body() createTransactionBoxDto: CreateTransactionBoxDto,
  ): Promise<Transaction> {
    return this.legoBoxService.createTransactionBox(createTransactionBoxDto);
  }
}
