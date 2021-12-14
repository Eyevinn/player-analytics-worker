import winston from "winston";

export interface IWorkerOptions {
  logger: winston.Logger;
  max?: any;
}

export class Worker {
  logger: winston.Logger;
  constructor(opts: IWorkerOptions) {
    this.logger = opts.logger;
  }

  // Recieve Queue Messages

  // Put Messages to DB

  // Remove Messages from Queue
}
