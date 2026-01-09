import { AbstractQueueAdapter } from '@eyevinn/player-analytics-shared/types/interfaces';
import winston from 'winston';

export default class Queue {
  logger: winston.Logger;
  QueueAdapter: AbstractQueueAdapter;
  instanceId: string;

  constructor(logger: winston.Logger, id: string) {
    this.logger = logger;
    this.instanceId = id;
  }

  private async getQueueAdapter(): Promise<void> {
    if (this.QueueAdapter === undefined) {
      let queueAdapter: any;
      switch (process.env.QUEUE_TYPE) {
        case 'SQS':
          queueAdapter = (await import('@eyevinn/player-analytics-shared'))
            .SqsQueueAdapter;
          break;
        case 'beanstalkd':
          queueAdapter = (await import('@eyevinn/player-analytics-shared')).BeanstalkdAdapter;
          break;
        case 'redis':
          queueAdapter = (await import('@eyevinn/player-analytics-shared')).RedisAdapter;
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
      await this.getQueueAdapter();
      const queueResponse = await this.QueueAdapter.pullFromQueue();
      return queueResponse;
    } catch (err) {
      this.logger.error(`[${this.instanceId}]: ${err}`);
      throw new Error(err);
    }
  }

  public async remove(queueMessageObject: Object): Promise<Object> {
    try {
      await this.getQueueAdapter();
      const queueResponse = await this.QueueAdapter.removeFromQueue(
        queueMessageObject
      );
      return queueResponse;
    } catch (err) {
      this.logger.error(`[${this.instanceId}]: ${err}`);
      throw new Error(err);
    }
  }

  public getEventJSONsFromMessages(messages: any[]): any[] {
    return this.QueueAdapter.getEventJSONsFromMessages(messages);
  }
}
