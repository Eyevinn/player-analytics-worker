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
  maxAge: number;
  sqsPullInterval: number;
  dbProcessInterval: number;

  constructor(opts: IWorkerOptions) {
    this.logger = opts.logger;
    this.iterations = -1;
    this.tablePrefix = TABLE_PREFIX;
    this.workerId = uuidv4();
    this.state = WorkerState.IDLE;
    this.queue = new Queue(opts.logger, this.workerId);
    this.db = new EventDB(opts.logger, this.workerId);
    this.internalQueue = new InternalQueue(opts.logger, this.workerId);
    this.maxAge = process.env.MAX_AGE ? parseInt(process.env.MAX_AGE) : 60000;
    this.sqsPullInterval = process.env.SQS_PULL_INTERVAL ? parseInt(process.env.SQS_PULL_INTERVAL) : 1000;
    this.dbProcessInterval = process.env.DB_PROCESS_INTERVAL ? parseInt(process.env.DB_PROCESS_INTERVAL) : 2000;
  }

  // For unit tests only
  setLoopIterations(iterations: number) {
    this.logger.warn(`[${this.workerId}]: Setting worker iterations to: ${iterations}`);
    this.iterations = iterations;
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

      // Second pass: add messages to internal queue with capacity check
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
          addedCount++;
        } else {
          skippedCount++;
          this.logger.error(`[${this.workerId}]: Failed to add message to internal queue despite capacity check`);
        }
      }

      if (skippedCount > 0) {
        this.logger.warn(`[${this.workerId}]: Could not process ${skippedCount} messages due to internal queue capacity. Messages remain in SQS for retry.`);
      }

      this.logger.debug(`[${this.workerId}]: Added ${addedCount} messages to internal queue. Queue size: ${this.internalQueue.getQueueSize()}`);
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
      
      const successfulMessages: any[] = [];
      const failedMessages: any[] = [];
      
      for (const result of writeResults) {
        const tableMessages = groupedByTable[result.tableName];
        if (result.success) {
          successfulMessages.push(...tableMessages.map(qm => qm.message));
        } else {
          this.logger.error(`[${this.workerId}]: Failed to write to table ${result.tableName}:`, result.error);
          failedMessages.push(...tableMessages);
        }
      }

      for (const message of successfulMessages) {
        await this.removeFromQueue(message);
      }

      if (failedMessages.length > 0) {
        this.internalQueue.requeue(failedMessages);
      }

      this.logger.debug(`[${this.workerId}]: Processed batch: ${successfulMessages.length} successful, ${failedMessages.length} failed`);
      
    } catch (err) {
      this.logger.error(`[${this.workerId}]: Error processing internal queue batch: ${err}`);
    }
  }

  async startAsync() {
    this.state = WorkerState.ACTIVE;
    this.logger.info(`[${this.workerId}]: Worker starting with internal queue - SQS pull interval: ${this.sqsPullInterval}ms, DB process interval: ${this.dbProcessInterval}ms`);

    let sqsLastRun = 0;
    let dbLastRun = 0;
    let failedIterations = 0;
    let isPaused = false;

    while (this.state === WorkerState.ACTIVE) {
      if (isPaused) {
        await delay(30000);
        continue;
      }

      // For Unit tests
      if (this.iterations > 0) this.iterations--;
      if (this.iterations === 0) this.state = WorkerState.INACTIVE;

      const now = Date.now();

      try {
        // Process SQS messages at configured interval
        if (now - sqsLastRun >= this.sqsPullInterval) {
          await this.processSQSMessages();
          sqsLastRun = now;
        }

        // Process internal queue at configured interval
        if (now - dbLastRun >= this.dbProcessInterval) {
          await this.processInternalQueue();
          dbLastRun = now;
        }

        // Brief delay to prevent tight loops
        await delay(100);

        // Log queue stats periodically
        if (now % 30000 < 1000) {
          const stats = this.internalQueue.getStats();
          const capacity = this.internalQueue.getAvailableCapacity();
          if (stats.size > 0) {
            this.logger.info(`[${this.workerId}]: Internal queue stats - Size: ${stats.size}/${this.internalQueue.maxQueueSize}, Available capacity: ${capacity}, Oldest message: ${stats.oldestMessage}ms`);
          }
          if (capacity === 0) {
            this.logger.warn(`[${this.workerId}]: Internal queue is at capacity! SQS consumption is paused.`);
          }
        }

        failedIterations = 0;

      } catch (err) {
        this.logger.error(`[${this.workerId}]: Error in main loop: ${err}`);
        failedIterations++;
        
        if (failedIterations > 10) {
          this.logger.error(`[${this.workerId}]: Too many failed iterations (${failedIterations}). Pausing worker.`);
          isPaused = true;
          failedIterations = 0;
        }
        
        await delay(5000);
      }
    }

    this.logger.info(`[${this.workerId}]: Worker stopped. Final internal queue size: ${this.internalQueue.getQueueSize()}`);
  }
}
