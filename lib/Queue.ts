import winston from "winston";

export default class Queue {
  logger: winston.Logger;
  QueueAdapter: any;

  constructor(logger: winston.Logger) {
    this.logger = logger;
  }

  private async _getQueueAdapter(): Promise<void> {
    if (this.QueueAdapter === undefined) {
      let queueAdapter: any;
      switch (process.env.QUEUE_TYPE) {
        case "SQS":
          queueAdapter = (await import("@eyevinn/player-analytics-shared"))
            .SqsQueueAdapter;
          break;
        default:
          this.logger.warn("No queue type specified");
          throw new Error("No queue type specified");
      }
      console.log("I made something")
      this.QueueAdapter = new queueAdapter(this.logger);
    }
  }

  public async receive(): Promise<Object> {
    await this._getQueueAdapter();
    return this.QueueAdapter.pullFromQueue();
  }

  public async remove(queueMessageObject: Object): Promise<Object> {
    await this._getQueueAdapter();
    return this.QueueAdapter.removeFromQueue(queueMessageObject);
  }
}
