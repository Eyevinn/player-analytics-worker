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
  private headIndex: number = 0;
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
    // Use active size (head-index aware) to stay consistent with hasCapacity().
    // Otherwise hasCapacity() could report free space while add() rejects,
    // causing the worker to repeatedly re-fetch the same messages.
    if (this.size >= this.maxQueueSize) {
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
    this.logger.debug(`[${this.instanceId}]: Added message to internal queue. Queue size: ${this.size}`);
    return true;
  }

  private get size(): number {
    return this.queue.length - this.headIndex;
  }

  /**
   * Compact the internal array when the consumed portion exceeds half
   * the total array length. This keeps memory bounded while avoiding
   * O(n) shifts on every getBatch() call.
   */
  private compact(): void {
    if (this.headIndex > 0 && this.headIndex > this.queue.length / 2) {
      this.queue = this.queue.slice(this.headIndex);
      this.headIndex = 0;
    }
  }

  public hasCapacity(count: number = 1): boolean {
    return (this.size + count) <= this.maxQueueSize;
  }

  public getAvailableCapacity(): number {
    return Math.max(0, this.maxQueueSize - this.size);
  }

  public getQueueSize(): number {
    return this.size;
  }

  public getBatch(): QueuedMessage[] {
    if (this.size === 0) {
      return [];
    }

    const batchEnd = this.headIndex + Math.min(this.batchSize, this.size);
    const batch = this.queue.slice(this.headIndex, batchEnd);
    this.headIndex = batchEnd;

    this.compact();

    this.logger.debug(`[${this.instanceId}]: Retrieved batch of ${batch.length} messages. Remaining: ${this.size}`);
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

    // Prepend requeued messages before the head
    if (this.headIndex >= requeueableMessages.length) {
      // Space before head — fill it in
      for (let i = requeueableMessages.length - 1; i >= 0; i--) {
        this.queue[--this.headIndex] = requeueableMessages[i];
      }
    } else {
      // No space — compact first, then unshift
      this.queue = this.queue.slice(this.headIndex);
      this.headIndex = 0;
      this.queue.unshift(...requeueableMessages);
    }
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
    return this.size === 0;
  }

  public clear(): void {
    this.queue = [];
    this.headIndex = 0;
    this.logger.debug(`[${this.instanceId}]: Internal queue cleared`);
  }

  public getStats(): { size: number, oldestMessage: number | null } {
    if (this.size === 0) {
      return { size: 0, oldestMessage: null };
    }

    const activeQueue = this.queue.slice(this.headIndex);
    const oldestMessage = Math.min(...activeQueue.map(msg => msg.addedAt));
    return {
      size: this.size,
      oldestMessage: Date.now() - oldestMessage
    };
  }
}
