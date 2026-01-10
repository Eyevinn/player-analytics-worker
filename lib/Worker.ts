import winston from 'winston';
import EventDB from './EventDB';
import Queue from './Queue';
import InternalQueue from './InternalQueue';
import { TABLE_PREFIX } from '@eyevinn/player-analytics-shared';
import { v4 as uuidv4 } from 'uuid';

require('dotenv').config();

const delay = (ms: number) => new Promise((res) => setTimeout(res, ms));

export interface IWorkerOptions {
  logger: winston.Logger;
}

export enum WorkerState {
  IDLE = 'idle',
  ACTIVE = 'active',
  INACTIVE = 'inactive'
}

export class Worker {
  logger: winston.Logger;
  queue: any;
  db: any;
  internalQueue: InternalQueue;
  state: string;
  workerId: string;
  tablePrefix: string;
  iterations: number;
  queueIterations: number;
  maxAge: number;
  queuePullInterval: number;
  dbProcessInterval: number;
  isPaused: boolean;
  pauseDuration: number;
  startupJitterMs: number;
  queueConcurrentReceives: number;
  private pausedAt: number;
  private batchRemoveFailures: number;
  private useBatchRemove: boolean;

  constructor(opts: IWorkerOptions) {
    this.logger = opts.logger;
    this.iterations = -1;
    this.queueIterations = -1;
    this.tablePrefix = TABLE_PREFIX;
    this.workerId = uuidv4();
    this.state = WorkerState.IDLE;
    this.queue = new Queue(opts.logger, this.workerId);
    this.db = new EventDB(opts.logger, this.workerId);
    this.internalQueue = new InternalQueue(opts.logger, this.workerId);
    this.maxAge = process.env.MAX_AGE ? parseInt(process.env.MAX_AGE) : 60000;
    this.queuePullInterval = this.getEnvWithDeprecation('QUEUE_PULL_INTERVAL', 'SQS_PULL_INTERVAL', 1000);
    this.dbProcessInterval = process.env.DB_PROCESS_INTERVAL ? parseInt(process.env.DB_PROCESS_INTERVAL) : 2000;
    this.isPaused = false;
    this.pausedAt = 0;
    this.pauseDuration = process.env.PAUSE_DURATION ? parseInt(process.env.PAUSE_DURATION) : 300000; // 5 minutes default
    this.startupJitterMs = process.env.STARTUP_JITTER_MS ? parseInt(process.env.STARTUP_JITTER_MS) : 5000; // 5 seconds default
    this.queueConcurrentReceives = this.getEnvWithDeprecation('QUEUE_CONCURRENT_RECEIVES', 'SQS_CONCURRENT_RECEIVES', 5);
    this.batchRemoveFailures = 0;
    this.useBatchRemove = true;
  }

  private getEnvWithDeprecation(newName: string, deprecatedName: string, defaultValue: number): number {
    if (process.env[newName]) {
      return parseInt(process.env[newName] as string);
    }
    if (process.env[deprecatedName]) {
      this.logger.warn(`[${this.workerId}]: Environment variable ${deprecatedName} is deprecated, use ${newName} instead`);
      return parseInt(process.env[deprecatedName] as string);
    }
    return defaultValue;
  }

  resume() {
    if (this.isPaused) {
      this.logger.info(`[${this.workerId}]: Resuming worker from paused state`);
      this.isPaused = false;
      this.pausedAt = 0;
    }
  }

  pause() {
    if (!this.isPaused) {
      this.logger.info(`[${this.workerId}]: Pausing worker`);
      this.isPaused = true;
      this.pausedAt = Date.now();
    }
  }

  // For unit tests only
  setLoopIterations(iterations: number) {
    this.logger.warn(`[${this.workerId}]: Setting worker iterations to: ${iterations}`);
    this.iterations = iterations;
    this.queueIterations = iterations;
  }

  // For unit tests only - set faster intervals for testing
  setTestIntervals(queueInterval: number = 100, dbInterval: number = 200) {
    this.queuePullInterval = queueInterval;
    this.dbProcessInterval = dbInterval;
    this.queueConcurrentReceives = 1; // Use single receive for predictable test behavior
    this.logger.warn(`[${this.workerId}]: Test mode - Queue interval: ${queueInterval}ms, DB interval: ${dbInterval}ms`);
  }

  private async removeFromQueue(message: any, index: number): Promise<boolean> {
    const startTime = Date.now();
    this.logger.debug(`[${this.workerId}]: Queue remove #${index + 1} starting`);
    try {
      await this.queue.remove(message);
      this.logger.debug(`[${this.workerId}]: Queue remove #${index + 1} completed in ${Date.now() - startTime}ms`);
      return true;
    } catch (err) {
      this.logger.error(`[${this.workerId}]: Queue remove #${index + 1} failed after ${Date.now() - startTime}ms: ${err}`);
      return false;
    }
  }

  private async removeMessagesIndividually(messages: any[]): Promise<{ successful: number; failed: number }> {
    const results = await Promise.all(messages.map((msg, idx) => this.removeFromQueue(msg, idx)));
    const successful = results.filter(r => r).length;
    const failed = results.filter(r => !r).length;
    return { successful, failed };
  }

  private async removeMessagesFromQueue(messages: any[]): Promise<void> {
    const removeStartTime = Date.now();
    this.logger.debug(`[${this.workerId}]: Removing ${messages.length} messages from queue`);

    let successful = 0;
    let failed = 0;

    if (this.useBatchRemove) {
      let batchFailed = false;
      try {
        const result: any = await this.queue.removeBatch(messages);
        successful = result.successful?.length || 0;
        failed = result.failed?.length || 0;

        // If all messages failed, treat as batch removal not supported
        if (failed === messages.length && successful === 0) {
          batchFailed = true;
          this.logger.warn(`[${this.workerId}]: Batch remove returned all ${failed} messages as failed`);
        } else if (failed > 0) {
          this.logger.error(`[${this.workerId}]: Failed to remove ${failed} messages from queue`);
        }
      } catch (err) {
        batchFailed = true;
        this.logger.warn(`[${this.workerId}]: Batch remove threw error: ${err}`);
      }

      if (batchFailed) {
        this.batchRemoveFailures++;
        this.logger.warn(`[${this.workerId}]: Batch remove failed (attempt ${this.batchRemoveFailures}/3), falling back to individual removes`);

        if (this.batchRemoveFailures >= 3) {
          this.logger.warn(`[${this.workerId}]: Batch remove failed 3 times, permanently disabling batch removal`);
          this.useBatchRemove = false;
        }

        // Fall back to individual removes
        const fallbackResult = await this.removeMessagesIndividually(messages);
        successful = fallbackResult.successful;
        failed = fallbackResult.failed;
      }
    } else {
      // Batch remove is disabled, use individual removes
      const result = await this.removeMessagesIndividually(messages);
      successful = result.successful;
      failed = result.failed;
    }

    const removeDuration = Date.now() - removeStartTime;
    this.logger.debug(`[${this.workerId}]: Removed ${successful} messages from queue in ${removeDuration}ms${failed > 0 ? ` (${failed} failed)` : ''}`);
  }

  private async processQueueMessages() {
    try {
      // Check if internal queue has capacity before consuming from queue
      if (!this.internalQueue.hasCapacity(1)) {
        this.logger.debug(`[${this.workerId}]: Internal queue is full (${this.internalQueue.getQueueSize()}/${this.internalQueue.maxQueueSize}). Skipping queue consumption.`);
        return;
      }

      // Make concurrent receive calls to improve throughput
      const receiveStartTime = Date.now();
      const receivePromises = Array(this.queueConcurrentReceives)
        .fill(null)
        .map((_, idx) => {
          const startTime = Date.now();
          this.logger.debug(`[${this.workerId}]: Queue receive #${idx + 1} starting`);
          return this.queue.receive().then((result) => {
            this.logger.debug(`[${this.workerId}]: Queue receive #${idx + 1} completed in ${Date.now() - startTime}ms`);
            return result;
          });
        });

      const results = await Promise.all(receivePromises);
      const receiveDuration = Date.now() - receiveStartTime;
      const collectedMessages: any[] = results
        .filter(Array.isArray)
        .flat();

      if (collectedMessages.length === 0) {
        this.logger.debug(`[${this.workerId}]: No messages received from queue (${receiveDuration}ms)`);
        return;
      }

      this.logger.debug(`[${this.workerId}]: Retrieved ${collectedMessages.length} messages from queue in ${receiveDuration}ms (${this.queueConcurrentReceives} concurrent receives, ${(collectedMessages.length / (receiveDuration / 1000)).toFixed(1)} msgs/sec)`);

      const allEvents: any[] = this.queue.getEventJSONsFromMessages(collectedMessages);
      const messagesToRemove: any[] = [];
      let skippedCount = 0;

      // Add messages to internal queue
      // Table existence check is deferred to the DB consumer for true decoupling
      for (let i = 0; i < allEvents.length; i++) {
        if (!this.internalQueue.hasCapacity(1)) {
          skippedCount++;
          this.logger.warn(`[${this.workerId}]: Internal queue capacity exceeded. Skipping remaining ${allEvents.length - i} messages.`);
          break;
        }

        const eventJson = allEvents[i];
        const shardId = eventJson.shardId ? eventJson.shardId : (eventJson.host ? eventJson.host : 'default');
        const tableName: string = this.tablePrefix + shardId;

        const added = this.internalQueue.add(collectedMessages[i], eventJson, tableName, i);
        if (added) {
          messagesToRemove.push(collectedMessages[i]);
        } else {
          skippedCount++;
          this.logger.error(`[${this.workerId}]: Failed to add message to internal queue despite capacity check`);
        }
      }

      // Remove messages from queue
      if (messagesToRemove.length > 0) {
        await this.removeMessagesFromQueue(messagesToRemove);
      }

      if (skippedCount > 0) {
        this.logger.warn(`[${this.workerId}]: Could not process ${skippedCount} messages due to internal queue capacity. Messages remain in queue for retry.`);
      }

      this.logger.debug(`[${this.workerId}]: Added ${messagesToRemove.length} messages to internal queue and removed from queue. Queue size: ${this.internalQueue.getQueueSize()}`);
    } catch (err) {
      this.logger.error(`[${this.workerId}]: Error processing queue messages: ${err}`);
    }
  }

  private async processInternalQueue() {
    if (this.internalQueue.isEmpty()) {
      return;
    }

    try {
      const batch = this.internalQueue.getBatch();
      if (batch.length === 0) {
        return;
      }

      this.logger.debug(`[${this.workerId}]: Processing batch of ${batch.length} messages from internal queue`);

      const groupedByTable = this.internalQueue.groupByTable(batch);
      const eventsByTable: { [tableName: string]: any[] } = {};
      const messagesToDiscard: any[] = [];
      const messagesToRequeue: any[] = [];

      // Check table existence for each table group
      for (const [tableName, queuedMessages] of Object.entries(groupedByTable)) {
        const tableExists: boolean = await this.db.TableExists(tableName);

        if (tableExists) {
          eventsByTable[tableName] = queuedMessages.map(qm => qm.event);
        } else {
          this.logger.warn(`[${this.workerId}]: No Table named:'${tableName}' was found`);

          // Check if events have expired
          for (const qm of queuedMessages) {
            if (Date.now() - qm.event.timestamp > this.maxAge) {
              this.logger.warn(`[${this.workerId}]: Event has expired. Discarding.`);
              messagesToDiscard.push(qm);
            } else {
              // Table doesn't exist yet but event hasn't expired - requeue for retry
              messagesToRequeue.push(qm);
            }
          }
        }
      }

      let successCount = 0;
      const failedMessages: any[] = [];

      // Only write to tables that exist
      if (Object.keys(eventsByTable).length > 0) {
        const writeResults = await this.db.batchWriteByTable(eventsByTable);

        for (const result of writeResults) {
          const tableMessages = groupedByTable[result.tableName];
          if (result.success) {
            successCount += tableMessages.length;
          } else {
            this.logger.error(`[${this.workerId}]: Failed to write to table ${result.tableName}:`, result.error);
            failedMessages.push(...tableMessages);
          }
        }
      }

      // Requeue messages for tables that don't exist yet (and haven't expired)
      if (messagesToRequeue.length > 0) {
        this.internalQueue.requeue(messagesToRequeue);
        this.logger.debug(`[${this.workerId}]: Requeued ${messagesToRequeue.length} messages for non-existent tables`);
      }

      if (failedMessages.length > 0) {
        this.internalQueue.requeue(failedMessages);
      }

      this.logger.debug(`[${this.workerId}]: Processed batch: ${successCount} successful, ${failedMessages.length} failed, ${messagesToDiscard.length} discarded, ${messagesToRequeue.length} requeued for missing tables`);

    } catch (err) {
      this.logger.error(`[${this.workerId}]: Error processing internal queue batch: ${err}`);
    }
  }

  private async runQueueProducerLoop(): Promise<void> {
    this.logger.info(`[${this.workerId}]: Queue producer loop started - interval: ${this.queuePullInterval}ms`);

    let failedIterations = 0;
    let errorBackoffMs = 1000;
    const maxErrorBackoffMs = 20000;

    while (this.state === WorkerState.ACTIVE) {
      if (this.isPaused) {
        await delay(1000);
        continue;
      }

      // For unit tests - decrement iterations in producer loop
      if (this.queueIterations > 0) this.queueIterations--;
      if (this.queueIterations === 0) break;

      try {
        await this.processQueueMessages();

        failedIterations = 0;
        errorBackoffMs = 1000;

        await delay(this.queuePullInterval);
      } catch (err) {
        this.logger.error(`[${this.workerId}]: Error in queue producer loop: ${err}. Retrying in ${errorBackoffMs}ms`);
        failedIterations++;

        if (failedIterations > 10) {
          this.logger.error(`[${this.workerId}]: Too many queue failures (${failedIterations}). Pausing worker.`);
          this.pause();
          failedIterations = 0;
        }

        await delay(errorBackoffMs);
        errorBackoffMs = Math.min(errorBackoffMs * 2, maxErrorBackoffMs);
      }
    }

    this.logger.info(`[${this.workerId}]: Queue producer loop stopped`);
  }

  private async runDBConsumerLoop(): Promise<void> {
    this.logger.info(`[${this.workerId}]: DB consumer loop started - interval: ${this.dbProcessInterval}ms`);

    let failedIterations = 0;
    let errorBackoffMs = 1000;
    const maxErrorBackoffMs = 20000;
    let lastStatsLog = 0;

    while (this.state === WorkerState.ACTIVE) {
      if (this.isPaused) {
        // Check if pause duration has elapsed for auto-resume
        if (this.pausedAt > 0 && Date.now() - this.pausedAt >= this.pauseDuration) {
          this.logger.info(`[${this.workerId}]: Auto-resuming worker after ${this.pauseDuration}ms pause`);
          this.resume();
        } else {
          await delay(1000);
          continue;
        }
      }

      // For unit tests - decrement iterations in consumer loop
      if (this.iterations > 0) this.iterations--;
      if (this.iterations === 0) this.state = WorkerState.INACTIVE;

      try {
        await this.processInternalQueue();

        // Log queue stats periodically (every 30 seconds)
        const now = Date.now();
        if (now - lastStatsLog >= 30000) {
          const stats = this.internalQueue.getStats();
          const capacity = this.internalQueue.getAvailableCapacity();
          if (stats.size > 0) {
            this.logger.info(`[${this.workerId}]: Internal queue stats - Size: ${stats.size}/${this.internalQueue.maxQueueSize}, Available capacity: ${capacity}, Oldest message: ${stats.oldestMessage}ms`);
          }
          if (capacity === 0) {
            this.logger.warn(`[${this.workerId}]: Internal queue is at capacity! Queue consumption is paused.`);
          }
          lastStatsLog = now;
        }

        failedIterations = 0;
        errorBackoffMs = 1000;

        await delay(this.dbProcessInterval);
      } catch (err) {
        this.logger.error(`[${this.workerId}]: Error in DB consumer loop: ${err}. Retrying in ${errorBackoffMs}ms`);
        failedIterations++;

        if (failedIterations > 10) {
          this.logger.error(`[${this.workerId}]: Too many DB failures (${failedIterations}). Pausing worker.`);
          this.pause();
          failedIterations = 0;
        }

        await delay(errorBackoffMs);
        errorBackoffMs = Math.min(errorBackoffMs * 2, maxErrorBackoffMs);
      }
    }

    this.logger.info(`[${this.workerId}]: DB consumer loop stopped`);
  }

  async startAsync() {
    this.state = WorkerState.ACTIVE;

    // Apply random startup jitter to spread out worker starts and avoid thundering herd
    if (this.startupJitterMs > 0 && this.iterations === -1) {
      const jitter = Math.floor(Math.random() * this.startupJitterMs);
      this.logger.info(`[${this.workerId}]: Applying startup jitter of ${jitter}ms`);
      await delay(jitter);
    }

    this.logger.info(`[${this.workerId}]: Worker starting with concurrent producer/consumer loops - Queue interval: ${this.queuePullInterval}ms, DB interval: ${this.dbProcessInterval}ms`);

    // Run both loops concurrently - they operate independently
    await Promise.all([
      this.runQueueProducerLoop(),
      this.runDBConsumerLoop()
    ]);

    this.logger.info(`[${this.workerId}]: Worker stopped. Final internal queue size: ${this.internalQueue.getQueueSize()}`);
  }
}
