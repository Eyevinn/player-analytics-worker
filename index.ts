import winston from "winston";
import EventDB from "./lib/EventDB";
import Queue from "./lib/Queue";
import Logger from "./logging/logger";
import { v4 as uuidv4 } from "uuid";

require("dotenv").config();

export interface IWorkerOptions {
  logger: winston.Logger;
  max?: any;
}

export enum WorkerState {
  IDLE = "idle",
  ACTIVE = "active",
  INACTIVE = "inactive",
}
export class Worker {
  logger: winston.Logger;
  queue: any;
  db: any;
  state: string;
  workerId: string;
  tenant: string;

  constructor(opts: IWorkerOptions) {
    this.logger = opts.logger;
    this.queue = new Queue(opts.logger);
    this.db = new EventDB(opts.logger);
    this.state = WorkerState.IDLE;
    this.workerId = uuidv4();
  }

  async start() {
    this.state = WorkerState.ACTIVE;

    while (this.state === WorkerState.ACTIVE) {
      try {
        // ===Receive Queue Messages===
        const collectedMessages: any[] = await this.queue.receive();
        if (collectedMessages.length === 0) {
          this.logger.info(
            `[${this.workerId}]: Received No Messages from Queue. Going to Try Again`
          );
          continue;
        }

        // ===Put Messages to DB===
        //  - New List with only event obj
        let writePromises: any[] = [];
        const allEvents: any[] =
          this.queue.getEventJSONsFromMessages(collectedMessages);
        // - Create Table if needed
        if (this.db.table === undefined && allEvents.length > 0) {
          try {
            await this.db.createTable("epas_" + allEvents[0].host);
          } catch (err) {
            this.logger.error(`[${this.workerId}]: Error Caught: ${err}`);
          }
        }
        //  - Put each event obj in store
        allEvents.forEach(async (event) =>
          writePromises.push(this.db.write(event))
        );
        let results = await Promise.allSettled(writePromises);

        // ===Remove Messages from Queue===
        // - Only Messages that were not rejected
        const filtered = collectedMessages.filter(
          (_, index) => results[index].status !== "rejected"
        );
        filtered.forEach(async (message) => {
          try {
            await this.queue.remove(message);
            this.logger.info(`[${this.workerId}]: Removed message from Queue!`);
          } catch (err) {
            this.logger.error(
              `[${this.workerId}]: Error Removing from DB!`,
              err
            );
          }
        });
      } catch (err) {
        this.logger.error(`[${this.workerId}]: Error Caught: ${err}`);
        this.state = WorkerState.INACTIVE;
      }
    }
  }
}

// Only For Debugging...
const myWorker = new Worker({ logger: Logger });
myWorker.start();
