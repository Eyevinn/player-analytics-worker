import winston from 'winston';

export default class Queue {
  logger: winston.Logger;
  QueueAdapter: any;
  instanceId: string;

  constructor(logger: winston.Logger, id: string) {
    this.logger = logger;
    this.instanceId = id;
  }

  private async _getQueueAdapter(): Promise<void> {
    if (this.QueueAdapter === undefined) {
      let queueAdapter: any;
      switch (process.env.QUEUE_TYPE) {
        case 'SQS':
          queueAdapter = (await import('@eyevinn/player-analytics-shared'))
            .SqsQueueAdapter;
          break;
        default:
          this.logger.warn(`[${this.instanceId}]: No queue type specified`);
          throw new Error('No queue type specified');
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
      this.logger.error(`[${this.instanceId}]: ${err}`);
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
      this.logger.error(`[${this.instanceId}]: ${err}`);
      throw new Error(err);
    }
  }

  public getEventJSONsFromMessages(messages: any[]): Object[] {
    return this.QueueAdapter.getEventJSONsFromMessages(messages);
  }
}
