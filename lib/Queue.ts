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
      this.QueueAdapter = new queueAdapter(this.logger);
    }
  }

  public async receive(): Promise<Object> {
    try {
      await this._getQueueAdapter();
      const queueResponse = await this.QueueAdapter.pullFromQueue();
      return queueResponse;
    } catch (err) {
      this.logger.error(err);
      throw new Error(err);
    }
  }

  public async remove(queueMessageObject: Object): Promise<Object> {
    try {
      await this._getQueueAdapter();
      const queueuResponse = await this.QueueAdapter.removeFromQueue(
        queueMessageObject
      );
      return queueuResponse;
    } catch (err) {
      this.logger.error(err);
      throw new Error(err);
    }
  }

  public getEventJSONsFromMessages(messages: any[]): any {
    try {
      const queueResponse: any[] =
        this.QueueAdapter.getEventJSONsFromMessages(messages);
      return queueResponse;
    } catch (err) {
      this.logger.error(err);
    }
  }
}
