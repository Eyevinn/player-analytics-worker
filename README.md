# player-analytics-worker

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![Slack](http://slack.streamingtech.se/badge.svg)](http://slack.streamingtech.se)

The Eyevinn Player Analytics (EPAS) is an open sourced framework and specification for tracking events from video players. It is a modular framework where you can pick and choose the modules you need. This is the worker module that process the data from the eventsink and processing queue and stores it in a database.

## Setup

To be able to run the project locally a few things need to be set first.

- Environment Variables need to be set. You can use the `dotenv.template` file as a guide as to what needs to be set.

## Development

To run two workers locally with an SQS queue and a DynamoDB in AWS:

```typescript
import { Worker } from '@eyevinn/player-analytics-worker';
import Logger from './logging/logger';

const workers: Worker[] = [];

for (let i = 0; i < 2; i++) {
  workers.push(new Worker({ logger: Logger }));
}

workers.map((worker) => worker.startAsync());
```

The workers should start polling the SQS queue for messages and writing them to the DynamoDB.

## Docker Container

We also provide a ready-to-run Docker container on Docker Hub.

```
docker run --rm -e QUEUE_TYPE=SQS DB_TYPE=DYNAMODB AWS_REGION=eu-north-1 SQS_QUEUE_URL=<sqs-queue-url> eyevinntechnology/epas-worker:latest
```

# About Eyevinn Technology

Eyevinn Technology is an independent consultant firm specialized in video and streaming. Independent in a way that we are not commercially tied to any platform or technology vendor.

At Eyevinn, every software developer consultant has a dedicated budget reserved for open source development and contribution to the open source community. This give us room for innovation, team building and personal competence development. And also gives us as a company a way to contribute back to the open source community.

Want to know more about Eyevinn and how it is to work here. Contact us at work@eyevinn.se!
