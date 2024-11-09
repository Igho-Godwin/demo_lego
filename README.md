# LegoApi BACKEND

A Node application that handles legobox requests.


## API Documentation

https://documenter.getpostman.com/view/1599757/2sAY52cf7v

## Tools

- [Node](https://nodejs.org/)
- [Nest js](https://nestjs.com/)
- [typeorm](https://docs.nestjs.com/recipes/sql-typeorm)
- [jest](https://jestjs.io/)
- [RabbitMq](https://www.rabbitmq.com/)
- [Redis](https://redis.io)
- [Postgre](https://www.postgresql.org/)

## Prerequisites

The following should be installed in your machine

- Node v20 and above 

## To Run tests
- npm test

## How To Install And Run The Application on Local

- Clone this [Repo]('https://github.com/Igho-Godwin/lego-api.git') and `cd` into it
- create the .env file in your root directory and add

DB_NAME=lego_database
DB_USER=lego_user
DB_PASSWORD=lego_password
DB_PORT=5440
DB_HOST=localhost
REDIS_HOST=localhost
REDIS_PORT=6379
RABBITMQ_USER=user
RABBITMQ_PASSWORD=password
RABBITMQ_PORT=5672
RABBITMQ_URL=amqp://user:password@localhost:5672

- Install all the dependancies by running the `npm install`
- install postgre, redis and rabbitmq by running `docker compose up`
- check to make sure postgre, redis and rabbitmq are  running properly
- if you specified database name in .env the database should be already created for you 
- Start the application on development mode by running `npm run start:dev`
- to start the application `npm run start`
- migrations (src/database/migrations) will automatically install
- to run test  `npm test`
- there is test only for the lego service



## Issues

Issues are always very welcome. Please be sure to create a constructive issue when neccessary.
