import { Worker } from './index';
import Logger from './logging/logger';

// ===For Demo===
const NUMBER_OF_WORKERS: number = process.env.NUMBER_OF_WORKERS ? parseInt(process.env.NUMBER_OF_WORKERS) : 2;
const myWorkers: Worker[] = [];
// Recruit Workers
for (let i = 0; i < NUMBER_OF_WORKERS; i++) {
  myWorkers.push(new Worker({ logger: Logger }));
}
// Make them work!
myWorkers.map((worker) => worker.startAsync());
