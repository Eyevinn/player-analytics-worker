import winston from 'winston';
import EventDB from './EventDB';
import Queue from './Queue';
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

interface ThrottleConfig {
  baseDelay: number;
  maxDelay: number;
  responseTimeThreshold: number;
  backoffMultiplier: number;
}

export class Worker {
  logger: winston.Logger;
  queue: any;
  db: any;
  state: string;
  workerId: string;
  tablePrefix: string;
  iterations: number;
  maxAge: number;
  private throttleConfig: ThrottleConfig;
  private currentDelay: number;

  constructor(opts: IWorkerOptions) {
    this.logger = opts.logger;
    this.iterations = -1;
    this.tablePrefix = TABLE_PREFIX;
    this.workerId = uuidv4();
    this.state = WorkerState.IDLE;
    this.queue = new Queue(opts.logger, this.workerId);
    this.db = new EventDB(opts.logger, this.workerId);
    this.maxAge = process.env.MAX_AGE ? parseInt(process.env.MAX_AGE) : 60000;
    
    this.throttleConfig = {
      baseDelay: parseInt(process.env.BASE_QUEUE_DELAY || '3000'),
      maxDelay: parseInt(process.env.MAX_QUEUE_DELAY || '30000'),
      responseTimeThreshold: parseInt(process.env.RESPONSE_TIME_THRESHOLD || '5000'),
      backoffMultiplier: parseFloat(process.env.BACKOFF_MULTIPLIER || '1.5')
    };
    this.currentDelay = this.throttleConfig.baseDelay;
  }

  // For unit tests only
  setLoopIterations(iterations: number) {
    this.logger.warn(`[${this.workerId}]: Setting worker iterations to: ${iterations}`);
    this.iterations = iterations;
  }

  private async removeFromQueue(message: any) {
    try {
      await this.queue.remove(message);
      this.logger.debug(`[${this.workerId}]: Removed message from Queue!`);
    } catch (err) {
      this.logger.error(`[${this.workerId}]: Error Removing item from Queue!`, err);
    }
  }

  async startAsync() {
    this.state = WorkerState.ACTIVE;
    this.logger.debug(`[${this.workerId}]: Worker is Active...`);

    let failedIterations = 0;
    let isPaused = false;
    while (this.state === WorkerState.ACTIVE) {
      if (isPaused) {
        // This will pause the worker but not terminate the process.
        // Issue for failed writes needs to be fixed first and then the worker can be
        // restarted.
        await delay(30000);
        continue;
      }
      // For Unit tests
      if (this.iterations > 0) this.iterations--;
      if (this.iterations === 0) this.state = WorkerState.INACTIVE;

      this.logger.debug(`[${this.workerId}]: Worker is fetching from Queue...`);
      try {
        const collectedMessages: any[] = await this.queue.receive();
        if (!Array.isArray(collectedMessages)) {
          this.logger.warn(`[${this.workerId}]: Error collecting messages from queue`);
          continue;
        }
        if (!collectedMessages || collectedMessages.length === 0) {
          this.logger.debug(`[${this.workerId}]: Received No Messages from Queue. Going to Try Again`);
          const throttleDelay = this.calculateThrottleDelay();
          await delay(throttleDelay);
          continue;
        }
        const allEvents: any[] = this.queue.getEventJSONsFromMessages(collectedMessages);
        const validMessages: any[] = [];
        const eventsByTable: { [tableName: string]: { events: any[], messageIndices: number[] } } = {};
        
        for (let i = 0; i < allEvents.length; i++) {
          const eventJson = allEvents[i];
          const shardId = eventJson.shardId ? eventJson.shardId : (eventJson.host ? eventJson.host : 'default');
          const tableName: string = this.tablePrefix + shardId;
          const result: boolean = await this.db.TableExists(tableName);
          if (!result) {
            this.logger.warn(`[${this.workerId}]: No Table named:'${tableName}' was found`);
            if (Date.now() - eventJson.timestamp > this.maxAge) {
              this.logger.warn(`[${this.workerId}]: Event has expired. Removing event from queue`);
              await this.removeFromQueue(collectedMessages[i]);
            }
            continue;
          }
          validMessages.push(collectedMessages[i]);
          
          if (!eventsByTable[tableName]) {
            eventsByTable[tableName] = { events: [], messageIndices: [] };
          }
          eventsByTable[tableName].events.push(eventJson);
          eventsByTable[tableName].messageIndices.push(validMessages.length - 1);
        }

        const writePromises: Promise<any>[] = [];
        const tableNames: string[] = [];
        for (const [tableName, tableData] of Object.entries(eventsByTable)) {
          writePromises.push(this.db.writeMultiple(tableData.events, tableName));
          tableNames.push(tableName);
        }
        
        const writeResults = await Promise.allSettled(writePromises);
        let pushedMessages: any[] = [];
        let hasFailures = false;
        
        for (let i = 0; i < writeResults.length; i++) {
          const result = writeResults[i];
          const tableName = tableNames[i];
          const tableData = eventsByTable[tableName];
          
          if (result.status === 'fulfilled') {
            // Add all messages for this table to pushedMessages
            for (const msgIndex of tableData.messageIndices) {
              pushedMessages.push(validMessages[msgIndex]);
            }
          } else {
            hasFailures = true;
            this.logger.error(`[${this.workerId}]: Failed to write to table ${tableName}:`, result.reason);
          }
        }
        if (pushedMessages.length == 0 && validMessages.length > 0) {
          this.logger.warn(`[${this.workerId}]: No messages were pushed to the database but we have ${validMessages.length} valid messages.`);
          failedIterations++;
          if (failedIterations > 5) {
            this.logger.error(`[${this.workerId}]: Too many failed iterations (${failedIterations}). Pausing worker.`);
            isPaused = true;
          }
        } else {
          this.logger.debug(`[${this.workerId}]: Successfully pushed ${pushedMessages.length} messages to the database.`);
          failedIterations = 0;
          for (let i = 0; i < pushedMessages.length; i++) {
            await this.removeFromQueue(pushedMessages[i]);
          }
          
          // Check for abort conditions
          for (const result of writeResults) {
            if (result.status === 'rejected' && result.reason === 'abort') {
              this.state = WorkerState.INACTIVE;
              break;
            }
          }
        }
      } catch (err) {
        this.logger.error(`[${this.workerId}]: Error: ${err}`);
        await delay(20000);
      }
    }
  }

  private calculateThrottleDelay(): number {
    const avgReceiveTime = this.queue.getAverageResponseTime('receive');
    const avgRemoveTime = this.queue.getAverageResponseTime('remove');
    const avgResponseTime = (avgReceiveTime + avgRemoveTime) / 2;

    if (avgResponseTime > this.throttleConfig.responseTimeThreshold) {
      this.currentDelay = Math.min(
        this.currentDelay * this.throttleConfig.backoffMultiplier,
        this.throttleConfig.maxDelay
      );
      
      this.logger.warn(
        `[${this.workerId}]: High response times detected (avg: ${avgResponseTime}ms). ` +
        `Increasing throttle delay to ${this.currentDelay}ms`
      );
    } else if (avgResponseTime < this.throttleConfig.responseTimeThreshold / 2) {
      this.currentDelay = Math.max(
        this.currentDelay / this.throttleConfig.backoffMultiplier,
        this.throttleConfig.baseDelay
      );
      
      if (process.env.DEBUG) {
        this.logger.debug(
          `[${this.workerId}]: Response times normalized (avg: ${avgResponseTime}ms). ` +
          `Reducing throttle delay to ${this.currentDelay}ms`
        );
      }
    }

    return this.currentDelay;
  }

  public getThrottleStats(): { 
    currentDelay: number; 
    avgReceiveTime: number; 
    avgRemoveTime: number; 
    config: ThrottleConfig 
  } {
    return {
      currentDelay: this.currentDelay,
      avgReceiveTime: this.queue.getAverageResponseTime('receive'),
      avgRemoveTime: this.queue.getAverageResponseTime('remove'),
      config: { ...this.throttleConfig }
    };
  }
}
