import winston from 'winston';

interface QueuedMessage {
  message: any;
  event: any;
  tableName: string;
  messageIndex: number;
  retryCount: number;
  addedAt: number;
}

export default class InternalQueue {
  private logger: winston.Logger;
  private instanceId: string;
  private queue: QueuedMessage[] = [];
  private batchSize: number;
  private processingInterval: number;
  private maxRetries: number;
  public maxQueueSize: number;
  private isProcessing: boolean = false;

  constructor(logger: winston.Logger, instanceId: string) {
    this.logger = logger;
    this.instanceId = instanceId;
    this.batchSize = process.env.INTERNAL_QUEUE_BATCH_SIZE ? 
      parseInt(process.env.INTERNAL_QUEUE_BATCH_SIZE) : 50;
    this.processingInterval = process.env.INTERNAL_QUEUE_INTERVAL ? 
      parseInt(process.env.INTERNAL_QUEUE_INTERVAL) : 1000;
    this.maxRetries = process.env.INTERNAL_QUEUE_MAX_RETRIES ? 
      parseInt(process.env.INTERNAL_QUEUE_MAX_RETRIES) : 3;
    this.maxQueueSize = process.env.INTERNAL_QUEUE_MAX_SIZE ? 
      parseInt(process.env.INTERNAL_QUEUE_MAX_SIZE) : 1000;
  }

  public add(message: any, event: any, tableName: string, messageIndex: number): boolean {
    if (this.queue.length >= this.maxQueueSize) {
      this.logger.warn(`[${this.instanceId}]: Internal queue is full (${this.maxQueueSize}). Cannot add message.`);
      return false;
    }

    const queuedMessage: QueuedMessage = {
      message,
      event,
      tableName,
      messageIndex,
      retryCount: 0,
      addedAt: Date.now()
    };

    this.queue.push(queuedMessage);
    this.logger.debug(`[${this.instanceId}]: Added message to internal queue. Queue size: ${this.queue.length}`);
    return true;
  }

  public hasCapacity(count: number = 1): boolean {
    return (this.queue.length + count) <= this.maxQueueSize;
  }

  public getAvailableCapacity(): number {
    return Math.max(0, this.maxQueueSize - this.queue.length);
  }

  public getQueueSize(): number {
    return this.queue.length;
  }

  public getBatch(): QueuedMessage[] {
    if (this.queue.length === 0) {
      return [];
    }

    const batch = this.queue.splice(0, Math.min(this.batchSize, this.queue.length));
    this.logger.debug(`[${this.instanceId}]: Retrieved batch of ${batch.length} messages. Remaining: ${this.queue.length}`);
    return batch;
  }

  public requeue(messages: QueuedMessage[]): void {
    const requeueableMessages = messages.filter(msg => {
      if (msg.retryCount >= this.maxRetries) {
        this.logger.error(`[${this.instanceId}]: Message exceeded max retries (${this.maxRetries}), dropping`);
        return false;
      }
      msg.retryCount++;
      return true;
    });

    this.queue.unshift(...requeueableMessages);
    this.logger.debug(`[${this.instanceId}]: Requeued ${requeueableMessages.length} messages`);
  }

  public groupByTable(batch: QueuedMessage[]): { [tableName: string]: QueuedMessage[] } {
    const grouped: { [tableName: string]: QueuedMessage[] } = {};
    
    for (const queuedMessage of batch) {
      if (!grouped[queuedMessage.tableName]) {
        grouped[queuedMessage.tableName] = [];
      }
      grouped[queuedMessage.tableName].push(queuedMessage);
    }

    return grouped;
  }

  public isEmpty(): boolean {
    return this.queue.length === 0;
  }

  public clear(): void {
    this.queue = [];
    this.logger.debug(`[${this.instanceId}]: Internal queue cleared`);
  }

  public getStats(): { size: number, oldestMessage: number | null } {
    if (this.queue.length === 0) {
      return { size: 0, oldestMessage: null };
    }

    const oldestMessage = Math.min(...this.queue.map(msg => msg.addedAt));
    return { 
      size: this.queue.length, 
      oldestMessage: Date.now() - oldestMessage 
    };
  }
}