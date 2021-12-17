import winston from 'winston';
import EventDB from './lib/EventDB';
import Queue from './lib/Queue';
import Logger from './logging/logger';
import { v4 as uuidv4 } from 'uuid';

require('dotenv').config();

export interface IWorkerOptions {
  logger: winston.Logger;
  max?: any;
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
  tenant: string;
  tablePrefix: string;

  constructor(opts: IWorkerOptions) {
    this.logger = opts.logger;
    this.queue = new Queue(opts.logger);
    this.db = new EventDB(opts.logger);
    this.state = WorkerState.IDLE;
    this.tablePrefix = 'epas_';
    this.workerId = uuidv4();
  }

  async start() {
    this.state = WorkerState.ACTIVE;
    this.logger.info(`[${this.workerId}]: Worker is Active...`);

    while (this.state === WorkerState.ACTIVE) {
      this.logger.info(`[${this.workerId}]: Worker is fetching from Queue...`);
      try {
        // ===Receive Queue Messages===
        const collectedMessages: any[] = await this.queue.receive();
        if (!collectedMessages || collectedMessages.length === 0) {
          this.logger.info(
            `[${this.workerId}]: Received No Messages from Queue. Going to Try Again`
          );
          continue;
        }

        // ===Put Messages to DB===
        let writePromises: any[] = [];
        //  - New List with only event obj
        const allEvents: any[] =
          this.queue.getEventJSONsFromMessages(collectedMessages);

        for (let i = 0; i < allEvents.length; i++) {
          const eventJson = allEvents[i];
          const tableName: string = this.tablePrefix + eventJson.host;
          const result: boolean = await this.db.TableExists(tableName); // has a cache
          if (!result) {
            // - Create Table if needed
            try {
              await this.db.createTable(tableName);
            } catch (err) {
              this.logger.error(
                `[${this.workerId}]: Failed to create table '${tableName}'`,
                err
              );
            }
          }
          writePromises.push(this.db.write(eventJson, tableName));
        }
        const writeResults = await Promise.allSettled(writePromises);

        // ===Remove Messages from Queue===
        // - Only Messages that were not rejected
        const filtered = collectedMessages.filter(
          (_, index) => writeResults[index].status !== 'rejected'
        );
        filtered.forEach(async (message) => {
          try {
            await this.queue.remove(message);
            this.logger.info(`[${this.workerId}]: Removed message from Queue!`);
          } catch (err) {
            this.logger.error(
              `[${this.workerId}]: Error Removing from Queue!`,
              err
            );
          }
        });
      } catch (err) {
        this.logger.error(
          `[${this.workerId}]: Stopping Worker! Unexpected Error: ${err}`
        );
        this.state = WorkerState.INACTIVE;
      }
    }
  }
}
