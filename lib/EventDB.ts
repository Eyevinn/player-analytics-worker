import winston from 'winston';
import { AbstractDBAdapter } from '@eyevinn/player-analytics-shared/types/interfaces';
//import { ClickHouseDBAdapter } from '@eyevinn/player-analytics-shared';
import ClickHouseDBAdapter from './ClickHouseDBAdapter';
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
        this.tableNamesCache.push(tableName);
      }
      return true;
    } catch (err) {
      this.logger.error(`Failed to update tableNames cache.!'${err}'`);
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
        case 'TERMINAL':
          this.logger.info("Terminal DB specified.");
          dbAdapter = (await import('./TerminalDB')).default;
          break;
        case 'CLICKHOUSEDB':         
          dbAdapter = ClickHouseDBAdapter;
          break;
        default:
          this.logger.warn(` No database type specified`);
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
          resolve('Write to Table');
        })
        .catch((err) => {
          reject(err);
        });
    });
    promise.catch((exc) =>
      this.logger.error(
        `Failed Writing to Database! '${exc.error}'`
      )
    );
    return promise;
  }
}
