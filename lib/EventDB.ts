import winston from 'winston';
import { AbstractDBAdapter } from '@eyevinn/player-analytics-shared/types/interfaces';
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
      if (!this.tableNamesCache.includes(tableName)) {
        const doesExist = await this.DBAdapter.tableExists(tableName);
        if (!doesExist) {
          return false;
        }
        this.logger.debug(`[${this.instanceId}]: Updating tableNames cache`);
        this.tableNamesCache.push(tableName);
      }
      return true;
    } catch (err) {
      this.logger.error(`[${this.instanceId}]: Failed to update tableNames cache!`);
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
        case 'CLICKHOUSE':
          dbAdapter = (await import('@eyevinn/player-analytics-shared')).ClickHouseDBAdapter;
          break;
        default:
          this.logger.warn(`[${this.instanceId}]: No database type specified`);
          throw new Error('No database type specified');
      }
      this.DBAdapter = new dbAdapter(this.logger);
    }
  }

  public async write(event: any, table: string): Promise<any> {
    try {
      await this.DBAdapter.putItem({
        tableName: table,
        data: event,
      });
      this.logger.debug(`[${this.instanceId}]: Wrote to Table`);
    } catch (err) {
      this.logger.error(`[${this.instanceId}]: Failed Writing to Database!`);
      this.logger.error(err.message);
      throw err;
    }
  }

  public async writeMultiple(events: any[], table: string): Promise<any> {
    try {
      await this.DBAdapter.putItems({
        tableName: table,
        data: events,
      });
      this.logger.debug(`[${this.instanceId}]: Wrote ${events.length} items to Table`);
    } catch (err) {
      this.logger.error(`[${this.instanceId}]: Failed Writing multiple items to Database!`);
      this.logger.error(err.message);
      throw err;
    }
  }
}
