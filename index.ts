import winston from 'winston';
import EventDB from './lib/EventDB';
import Queue from './lib/Queue';
import { TABLE_PREFIX } from '@eyevinn/player-analytics-shared';
import { v4 as uuidv4 } from 'uuid';
//import { TABLE_PREFIX } from '@eyevinn/player-analytics-shared/util/constants';

require('dotenv').config();

export interface IWorkerOptions {
  logger: winston.Logger;
}

export enum WorkerState {
  IDLE = 'idle',
  ACTIVE = 'active',
  INACTIVE = 'inactive',
}
export class Worker {
  logger: winston.Logger;
  queue: any;
  db: any;
  state: string;
  workerId: string;
  tablePrefix: string;
  iterations: number;
  maxAge: number;

  constructor(opts: IWorkerOptions) {
    this.logger = opts.logger;
    this.iterations = -1;
    this.tablePrefix = TABLE_PREFIX;
    this.workerId = uuidv4();
    this.state = WorkerState.IDLE;
    this.queue = new Queue(opts.logger, this.workerId);
    this.db = new EventDB(opts.logger, this.workerId);
    this.maxAge = process.env.MAX_AGE ? parseInt(process.env.MAX_AGE) : 60000;
  }

  // For unit tests only
  setLoopIterations(iterations: number) {
    this.logger.warn(`[${this.workerId}]: Setting worker iterations to: ${iterations}`);
    this.iterations = iterations;
  }

  private async removeFromQueue(message: any) {
    try {
      await this.queue.remove(message);
      this.logger.debug(`[${this.workerId}]: Removed message from Queue!`);
    } catch (err) {
      this.logger.error(`[${this.workerId}]: Error Removing item from Queue!`, err);
    }
  }

  async startAsync() {
    this.state = WorkerState.ACTIVE;
    this.logger.debug(`[${this.workerId}]: Worker is Active...`);

    while (this.state === WorkerState.ACTIVE) {
      // For Unit tests
      if (this.iterations > 0) this.iterations--;
      if (this.iterations === 0) this.state = WorkerState.INACTIVE;

      this.logger.debug(`[${this.workerId}]: Worker is fetching from Queue...`);
      const writePromises: PromiseSettledResult<any>[] = [];
      try {
        const collectedMessages: any[] = await this.queue.receive();
        if (!Array.isArray(collectedMessages)) {
          this.logger.warn(`[${this.workerId}]: Error collecting messages from queue`);
          continue;
        }
        if (!collectedMessages || collectedMessages.length === 0) {
          this.logger.debug(`[${this.workerId}]: Received No Messages from Queue. Going to Try Again`);
          continue;
        }
        const allEvents: any[] = this.queue.getEventJSONsFromMessages(collectedMessages);
        const validMessages: any[] = [];
        for (let i = 0; i < allEvents.length; i++) {
          const eventJson = allEvents[i];
          const tableName: string = this.tablePrefix + eventJson.host;
          const result: boolean = await this.db.TableExists(tableName);
          if (!result) {
            this.logger.warn(`[${this.workerId}]: No Table named:'${tableName}' was found`);
            if (Date.now() - eventJson.timestamp > this.maxAge) {
              this.logger.warn(`[${this.workerId}]: Event has expired. Removing event from queue`);
              this.removeFromQueue(collectedMessages[i]);
            }
            continue;
          }
          validMessages.push(collectedMessages[i]);
          writePromises.push(this.db.write(eventJson, tableName));
        }
        const writeResults = await Promise.allSettled(writePromises);
        const pushedMessages = validMessages.filter(
          (_, index) => writeResults[index].status !== 'rejected'
        );
        for (let i = 0; i < pushedMessages.length; i++) {
          this.removeFromQueue(pushedMessages[i]);
        }

        writeResults.map((result) => {
          if (result.status === 'rejected' && result.reason === 'abort') {
            this.state = WorkerState.INACTIVE;
          }
        });
      } catch (err) {
        this.logger.error(`[${this.workerId}]: Error: ${err}`);
      }
    }
  }
}
