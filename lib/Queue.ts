import winston from 'winston';

export default class Queue {
  logger: winston.Logger;
  QueueAdapter: any;

  constructor(logger: winston.Logger) {
    this.logger = logger;
    
  }

  /**
   *
   * @param event the event object to send
   * @returns an object with the response from the event sender or an empty object if there was an error
   */
  async send(event: Object): Promise<Object> {
    let QueueAdapter: any;

    switch (process.env.QUEUE_TYPE) {
      case 'SQS':
        QueueAdapter = (await import('@eyevinn/player-analytics-shared')).SqsQueueAdapter;
        break;
      default:
        this.logger.warn('No queue type specified');
        return { message: 'No queue type specified' };
    }
    const queue = new QueueAdapter(this.logger);
    return queue.pushToQueue(event);
  }
}
