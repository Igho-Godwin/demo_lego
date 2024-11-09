import { HttpException, HttpStatus } from '@nestjs/common';

export class ComponentNotFoundException extends HttpException {
  constructor(componentId: number) {
    super(`Component with ID ${componentId} not found`, HttpStatus.NOT_FOUND);
  }
}

export class DuplicateBoxNameException extends HttpException {
  constructor(name: string) {
    super(`Lego box with name ${name} already exists`, HttpStatus.CONFLICT);
  }
}
