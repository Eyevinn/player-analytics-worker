import winston from 'winston';
import { AbstractDBAdapter } from '../types/interfaces';

export default class EventDB {
  logger: winston.Logger;
  DBAdapter: AbstractDBAdapter;
  tableNamesCache: string[];
  instanceId: string;

  constructor(logger: winston.Logger, id: string) {
    this.logger = logger;
    this.tableNamesCache = [];
    this.instanceId = id;
  }

  public async TableExists(tableName: string): Promise<boolean> {
    await this.getDBAdapter();
    try {
      // - If cache does not have the requested table name. Update cache, it might be there.
      if (!this.tableNamesCache.includes(tableName)) {
        this.logger.debug(`[${this.instanceId}]: Updating tableNames cache`);
        await this.createTable(tableName);
        this.tableNamesCache.push(tableName);
      }
    } catch (err) {
      if (JSON.stringify(err).indexOf('ResourceInUseException')) {
        this.tableNamesCache.push(tableName);
      } else {
        this.logger.error(`[${this.instanceId}]: Failed to update tableNames cache!`);
        throw new Error(err);
      }
    }
    return this.tableNamesCache.includes(tableName);
  }

  public async createTable(name: string): Promise<void> {
    try {
      await this.DBAdapter.createTable(name);
    } catch (err) {
      this.logger.error(`[${this.instanceId}]: Failed to create table '${name}'!`);
      throw new Error(err);
    }
  }

  private async getDBAdapter(): Promise<void> {
    if (this.DBAdapter === undefined) {
      let dbAdapter: any;
      switch (process.env.DB_TYPE) {
        case 'DYNAMODB':
          dbAdapter = (await import('@eyevinn/player-analytics-shared')).DynamoDBAdapter;
          break;
        case 'MONGODB':
          dbAdapter = (await import('@eyevinn/player-analytics-shared')).MongoDBAdapter;
          break;
        default:
          this.logger.warn(`[${this.instanceId}]: No database type specified`);
          throw new Error('No database type specified');
      }
      this.DBAdapter = new dbAdapter(this.logger);
    }
  }

  public write(event: any, table: string): Promise<any> {
    const promise = new Promise((resolve, reject) => {
      this.DBAdapter.putItem({
        tableName: table,
        data: event,
      })
        .then(() => {
          resolve('Wrote to Table');
        })
        .catch((err) => {
          reject(err);
        });
    });
    promise.catch((exc) =>
      this.logger.error(`[${this.instanceId}]: Failed Writing to Database! '${exc.error}'`)
    );
    return promise;
  }
}
