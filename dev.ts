import { Worker } from './index';
import Logger from './logging/logger';

const NUMBER_OF_WORKERS: number = process.env.NUMBER_OF_WORKERS ? parseInt(process.env.NUMBER_OF_WORKERS) : 2;
const myWorkers: Worker[] = [];

for (let i = 0; i < NUMBER_OF_WORKERS; i++) {
  myWorkers.push(new Worker({ logger: Logger }));
}

myWorkers.map((worker) => worker.startAsync());
