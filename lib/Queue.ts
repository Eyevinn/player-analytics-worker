import { AbstractQueueAdapter } from '@eyevinn/player-analytics-shared/types/interfaces';
import winston from 'winston';

export interface ResponseTimeMetrics {
  receiveResponseTimes: number[];
  removeResponseTimes: number[];
  maxSamples: number;
}

export default class Queue {
  logger: winston.Logger;
  QueueAdapter: AbstractQueueAdapter;
  instanceId: string;
  private responseMetrics: ResponseTimeMetrics;

  constructor(logger: winston.Logger, id: string) {
    this.logger = logger;
    this.instanceId = id;
    this.responseMetrics = {
      receiveResponseTimes: [],
      removeResponseTimes: [],
      maxSamples: 10
    };
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
      
      const startTime = Date.now();
      const queueResponse = await this.QueueAdapter.pullFromQueue();
      const responseTime = Date.now() - startTime;
      
      this.recordResponseTime('receive', responseTime);
      
      return queueResponse;
    } catch (err) {
      this.logger.error(`[${this.instanceId}]: ${err}`);
      throw new Error(err);
    }
  }

  public async remove(queueMessageObject: Object): Promise<Object> {
    try {
      await this.getQueueAdapter();
      
      const startTime = Date.now();
      const queueResponse = await this.QueueAdapter.removeFromQueue(
        queueMessageObject
      );
      const responseTime = Date.now() - startTime;
      
      this.recordResponseTime('remove', responseTime);
      
      return queueResponse;
    } catch (err) {
      this.logger.error(`[${this.instanceId}]: ${err}`);
      throw new Error(err);
    }
  }

  public getEventJSONsFromMessages(messages: any[]): any[] {
    return this.QueueAdapter.getEventJSONsFromMessages(messages);
  }

  private recordResponseTime(operation: 'receive' | 'remove', responseTime: number): void {
    const targetArray = operation === 'receive' 
      ? this.responseMetrics.receiveResponseTimes 
      : this.responseMetrics.removeResponseTimes;
    
    targetArray.push(responseTime);
    
    if (targetArray.length > this.responseMetrics.maxSamples) {
      targetArray.shift();
    }
    
    if (process.env.DEBUG) {
      this.logger.debug(`[${this.instanceId}]: ${operation} response time: ${responseTime}ms`);
    }
  }

  public getAverageResponseTime(operation: 'receive' | 'remove'): number {
    const targetArray = operation === 'receive' 
      ? this.responseMetrics.receiveResponseTimes 
      : this.responseMetrics.removeResponseTimes;
    
    if (targetArray.length === 0) return 0;
    
    const sum = targetArray.reduce((acc, time) => acc + time, 0);
    return sum / targetArray.length;
  }

  public getResponseTimeMetrics(): ResponseTimeMetrics {
    return { ...this.responseMetrics };
  }
}
