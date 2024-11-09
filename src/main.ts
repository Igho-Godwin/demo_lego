import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { ValidationPipe } from '@nestjs/common';
import * as compression from 'compression';
import helmet from 'helmet';
import { Transport } from '@nestjs/microservices';

// Queue configuration interface
interface QueueConfig {
  name: string;
  queue: string;
}

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  // Security
  app.use(helmet());

  // Compression
  app.use(compression());

  // CORS
  app.enableCors({
    origin: process.env.ALLOWED_ORIGINS?.split(',') || '*',
    methods: 'GET,HEAD,PUT,PATCH,POST,DELETE',
    credentials: true,
  });

  // Validation
  app.useGlobalPipes(
    new ValidationPipe({
      transform: true,
      whitelist: true,
      forbidNonWhitelisted: true,
      transformOptions: {
        enableImplicitConversion: true,
      },
    }),
  );

  const globalPrefix = 'api';
  app.setGlobalPrefix(globalPrefix);

  // Define queue configurations
  const queueConfigs: QueueConfig[] = [
    {
      name: 'batch_legobox_component_update',
      queue: 'batch_legobox_component_update_queue',
    },
    {
      name: 'legobox_price_update',
      queue: 'legobox_price_update_queue',
    },
  ];

  for (const queueConfig of queueConfigs) {
    app.connectMicroservice({
      transport: Transport.RMQ,
      options: {
        urls: [process.env.RABBITMQ_URL],
        queue: queueConfig.queue,
        queueOptions: {
          durable: true,
        },
        noAck: false,
        prefetchCount: 1,
        persistent: true,
      },
    });
  }

  await app.startAllMicroservices();

  await app.listen(process.env.PORT || 3000);
}
bootstrap();
