import winston from 'winston';
import EventDB from './EventDB';
import Queue from './Queue';
import { TABLE_PREFIX } from '@eyevinn/player-analytics-shared';
import { v4 as uuidv4 } from 'uuid';

require('dotenv').config();

const delay = (ms: number) => new Promise((res) => setTimeout(res, ms));

export interface IWorkerOptions {
  logger: winston.Logger;
}

export enum WorkerState {
  IDLE = 'idle',
  ACTIVE = 'active',
  INACTIVE = 'inactive'
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

    let failedIterations = 0;
    let isPaused = false;
    while (this.state === WorkerState.ACTIVE) {
      if (isPaused) {
        // This will pause the worker but not terminate the process.
        // Issue for failed writes needs to be fixed first and then the worker can be
        // restarted.
        await delay(30000);
        continue;
      }
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
          await delay(3000);
          continue;
        }
        const allEvents: any[] = this.queue.getEventJSONsFromMessages(collectedMessages);
        const validMessages: any[] = [];
        for (let i = 0; i < allEvents.length; i++) {
          const eventJson = allEvents[i];
          const tableName: string = this.tablePrefix + (eventJson.host || 'default');
          const result: boolean = await this.db.TableExists(tableName);
          if (!result) {
            this.logger.warn(`[${this.workerId}]: No Table named:'${tableName}' was found`);
            if (Date.now() - eventJson.timestamp > this.maxAge) {
              this.logger.warn(`[${this.workerId}]: Event has expired. Removing event from queue`);
              await this.removeFromQueue(collectedMessages[i]);
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
        if (pushedMessages.length == 0 && validMessages.length > 0) {
          this.logger.warn(`[${this.workerId}]: No messages were pushed to the database but we have ${validMessages.length} valid messages.`);
          failedIterations++;
          if (failedIterations > 5) {
            this.logger.error(`[${this.workerId}]: Too many failed iterations (${failedIterations}). Pausing worker.`);
            isPaused = true;
          }
        } else {
          this.logger.debug(`[${this.workerId}]: Successfully pushed ${pushedMessages.length} messages to the database.`);
          failedIterations = 0;
          for (let i = 0; i < pushedMessages.length; i++) {
            await this.removeFromQueue(pushedMessages[i]);
          }
          writeResults.map((result) => {
            if (result.status === 'rejected' && result.reason === 'abort') {
              this.state = WorkerState.INACTIVE;
            }
          });
        }
      } catch (err) {
        this.logger.error(`[${this.workerId}]: Error: ${err}`);
        await delay(20000);
      }
    }
  }
}
