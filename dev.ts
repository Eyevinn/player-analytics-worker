import { Worker } from './index';
import Logger from './logging/logger';

if (!process.env.QUEUE_TYPE) {
  Logger.error('QUEUE_TYPE not set');
  process.exit(1);
}

const NUMBER_OF_WORKERS: number = process.env.NUMBER_OF_WORKERS ? parseInt(process.env.NUMBER_OF_WORKERS) : 2;
const myWorkers: Worker[] = [];

for (let i = 0; i < NUMBER_OF_WORKERS; i++) {
  myWorkers.push(new Worker({ logger: Logger }));
}

myWorkers.map((worker) => worker.startAsync());

// Graceful shutdown on SIGTERM/SIGINT (e.g., Docker stop, Ctrl+C)
async function gracefulShutdown(signal: string) {
  Logger.info(`Received ${signal}. Stopping ${myWorkers.length} worker(s)...`);
  try {
    await Promise.all(myWorkers.map((w) => w.stop()));
    Logger.info('All workers stopped gracefully.');
  } catch (err) {
    Logger.error('Error during graceful shutdown:', err);
  }
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
