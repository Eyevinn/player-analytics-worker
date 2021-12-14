import winston from "winston";
import EventDB from "./lib/EventDB";
import Queue from "./lib/Queue";
import Logger from "./logging/logger";

require("dotenv").config();

export interface IWorkerOptions {
  logger: winston.Logger;
  max?: any;
}

export class Worker {
  logger: winston.Logger;
  queue: any;
  db: any;

  constructor(opts: IWorkerOptions) {
    this.logger = opts.logger;
    this.queue = new Queue(opts.logger);
    this.db = new EventDB(opts.logger);
  }

  async start() {
    // ===Recieve Queue Messages===
    // TODO
    const collectedMessages: any[] = await this.queue.receive();
    console.log("FROM SQS", JSON.stringify(collectedMessages, null, 2));

    // ===Put Messages to DB===
    // TODO

    // ===Remove Messages from Queue===
    // TODO
  }
}

// For Debugging...
const myWorker = new Worker({ logger: Logger });
myWorker.start();
