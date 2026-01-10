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
  sqsIterations: number;
  maxAge: number;
  sqsPullInterval: number;
  dbProcessInterval: number;
  isPaused: boolean;
  pauseDuration: number;
  startupJitterMs: number;
  private pausedAt: number;

  constructor(opts: IWorkerOptions) {
    this.logger = opts.logger;
    this.iterations = -1;
    this.sqsIterations = -1;
    this.tablePrefix = TABLE_PREFIX;
    this.workerId = uuidv4();
    this.state = WorkerState.IDLE;
    this.queue = new Queue(opts.logger, this.workerId);
    this.db = new EventDB(opts.logger, this.workerId);
    this.internalQueue = new InternalQueue(opts.logger, this.workerId);
    this.maxAge = process.env.MAX_AGE ? parseInt(process.env.MAX_AGE) : 60000;
    this.sqsPullInterval = process.env.SQS_PULL_INTERVAL ? parseInt(process.env.SQS_PULL_INTERVAL) : 1000;
    this.dbProcessInterval = process.env.DB_PROCESS_INTERVAL ? parseInt(process.env.DB_PROCESS_INTERVAL) : 2000;
    this.isPaused = false;
    this.pausedAt = 0;
    this.pauseDuration = process.env.PAUSE_DURATION ? parseInt(process.env.PAUSE_DURATION) : 300000; // 5 minutes default
    this.startupJitterMs = process.env.STARTUP_JITTER_MS ? parseInt(process.env.STARTUP_JITTER_MS) : 5000; // 5 seconds default
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
    this.sqsIterations = iterations;
  }

  // For unit tests only - set faster intervals for testing
  setTestIntervals(sqsInterval: number = 100, dbInterval: number = 200) {
    this.sqsPullInterval = sqsInterval;
    this.dbProcessInterval = dbInterval;
    this.logger.warn(`[${this.workerId}]: Test mode - SQS interval: ${sqsInterval}ms, DB interval: ${dbInterval}ms`);
  }

  private async removeFromQueue(message: any) {
    try {
      await this.queue.remove(message);
      this.logger.debug(`[${this.workerId}]: Removed message from Queue!`);
    } catch (err) {
      this.logger.error(`[${this.workerId}]: Error Removing item from Queue!`, err);
    }
  }

  private async processSQSMessages() {
    try {
      // Check if internal queue has capacity before consuming from SQS
      if (!this.internalQueue.hasCapacity(1)) {
        this.logger.debug(`[${this.workerId}]: Internal queue is full (${this.internalQueue.getQueueSize()}/${this.internalQueue.maxQueueSize}). Skipping SQS consumption.`);
        return;
      }

      const collectedMessages: any[] = await this.queue.receive();

      if (!Array.isArray(collectedMessages) || collectedMessages.length === 0) {
        this.logger.debug(`[${this.workerId}]: No messages received from SQS`);
        return;
      }

      this.logger.debug(`[${this.workerId}]: Retrieved ${collectedMessages.length} messages from SQS`);

      const allEvents: any[] = this.queue.getEventJSONsFromMessages(collectedMessages);
      const pendingMessages: { message: any, event: any, tableName: string, index: number }[] = [];

      // First pass: validate all messages and check for available tables
      for (let i = 0; i < allEvents.length; i++) {
        const eventJson = allEvents[i];
        const shardId = eventJson.shardId ? eventJson.shardId : (eventJson.host ? eventJson.host : 'default');
        const tableName: string = this.tablePrefix + shardId;

        const tableExists: boolean = await this.db.TableExists(tableName);
        if (!tableExists) {
          this.logger.warn(`[${this.workerId}]: No Table named:'${tableName}' was found`);
          if (Date.now() - eventJson.timestamp > this.maxAge) {
            this.logger.warn(`[${this.workerId}]: Event has expired. Removing event from queue`);
            await this.removeFromQueue(collectedMessages[i]);
          }
          continue;
        }

        pendingMessages.push({
          message: collectedMessages[i],
          event: eventJson,
          tableName,
          index: i
        });
      }

      // Second pass: add messages to internal queue and immediately remove from SQS
      let addedCount = 0;
      let skippedCount = 0;

      for (const pending of pendingMessages) {
        if (!this.internalQueue.hasCapacity(1)) {
          skippedCount++;
          this.logger.warn(`[${this.workerId}]: Internal queue capacity exceeded. Skipping remaining ${pendingMessages.length - addedCount - skippedCount + 1} messages.`);
          break;
        }

        const added = this.internalQueue.add(pending.message, pending.event, pending.tableName, pending.index);
        if (added) {
          // Remove from SQS immediately after adding to internal queue
          await this.removeFromQueue(pending.message);
          addedCount++;
        } else {
          skippedCount++;
          this.logger.error(`[${this.workerId}]: Failed to add message to internal queue despite capacity check`);
        }
      }

      if (skippedCount > 0) {
        this.logger.warn(`[${this.workerId}]: Could not process ${skippedCount} messages due to internal queue capacity. Messages remain in SQS for retry.`);
      }

      this.logger.debug(`[${this.workerId}]: Added ${addedCount} messages to internal queue and removed from SQS. Queue size: ${this.internalQueue.getQueueSize()}`);
    } catch (err) {
      this.logger.error(`[${this.workerId}]: Error processing SQS messages: ${err}`);
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

      for (const [tableName, queuedMessages] of Object.entries(groupedByTable)) {
        eventsByTable[tableName] = queuedMessages.map(qm => qm.event);
      }

      const writeResults = await this.db.batchWriteByTable(eventsByTable);

      let successCount = 0;
      const failedMessages: any[] = [];

      for (const result of writeResults) {
        const tableMessages = groupedByTable[result.tableName];
        if (result.success) {
          successCount += tableMessages.length;
        } else {
          this.logger.error(`[${this.workerId}]: Failed to write to table ${result.tableName}:`, result.error);
          failedMessages.push(...tableMessages);
        }
      }

      if (failedMessages.length > 0) {
        this.internalQueue.requeue(failedMessages);
      }

      this.logger.debug(`[${this.workerId}]: Processed batch: ${successCount} successful, ${failedMessages.length} failed`);

    } catch (err) {
      this.logger.error(`[${this.workerId}]: Error processing internal queue batch: ${err}`);
    }
  }

  private async runSQSProducerLoop(): Promise<void> {
    this.logger.info(`[${this.workerId}]: SQS producer loop started - interval: ${this.sqsPullInterval}ms`);

    let failedIterations = 0;
    let errorBackoffMs = 1000;
    const maxErrorBackoffMs = 20000;

    while (this.state === WorkerState.ACTIVE) {
      if (this.isPaused) {
        await delay(1000);
        continue;
      }

      // For unit tests - decrement iterations in producer loop
      if (this.sqsIterations > 0) this.sqsIterations--;
      if (this.sqsIterations === 0) break;

      try {
        await this.processSQSMessages();

        failedIterations = 0;
        errorBackoffMs = 1000;

        await delay(this.sqsPullInterval);
      } catch (err) {
        this.logger.error(`[${this.workerId}]: Error in SQS producer loop: ${err}. Retrying in ${errorBackoffMs}ms`);
        failedIterations++;

        if (failedIterations > 10) {
          this.logger.error(`[${this.workerId}]: Too many SQS failures (${failedIterations}). Pausing worker.`);
          this.pause();
          failedIterations = 0;
        }

        await delay(errorBackoffMs);
        errorBackoffMs = Math.min(errorBackoffMs * 2, maxErrorBackoffMs);
      }
    }

    this.logger.info(`[${this.workerId}]: SQS producer loop stopped`);
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
            this.logger.warn(`[${this.workerId}]: Internal queue is at capacity! SQS consumption is paused.`);
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

    this.logger.info(`[${this.workerId}]: Worker starting with concurrent producer/consumer loops - SQS interval: ${this.sqsPullInterval}ms, DB interval: ${this.dbProcessInterval}ms`);

    // Run both loops concurrently - they operate independently
    await Promise.all([
      this.runSQSProducerLoop(),
      this.runDBConsumerLoop()
    ]);

    this.logger.info(`[${this.workerId}]: Worker stopped. Final internal queue size: ${this.internalQueue.getQueueSize()}`);
  }
}
