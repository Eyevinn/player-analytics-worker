import winston from 'winston';

export default class EventDB {
  logger: winston.Logger;
  DBAdapter: any;
  tableNamesCache: string[];

  constructor(logger: winston.Logger) {
    this.logger = logger;
    this.tableNamesCache = [];
  }

  public async TableExists(tableName: string) {
    await this._getDBAdapter();
    // - If cache does not have the requested table name. Update cache, it might be there.
    if (!this.tableNamesCache.includes(tableName)) {
      this.logger.info('Updating tableNames cache');
      this.tableNamesCache = await this.DBAdapter.getTableNames();
    }
    return this.tableNamesCache.includes(tableName);
  }

  public async createTable(name: string) {
    try {
      await this.DBAdapter.createTable(name);
    } catch (err) {
      this.logger.warn('Problem when creating table');
      throw new Error(err);
    }
  }

  private async _getDBAdapter(): Promise<void> {
    if (this.DBAdapter === undefined) {
      let dbAdapter: any;
      switch (process.env.DB_TYPE) {
        case 'dynamodb':
          dbAdapter = (await import('../adapters/DynamoDBAdapter'))
            .DynamoDBAdapter;
          break;
        default:
          this.logger.warn('No database type specified');
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
    promise.catch(() => this.logger.error('Failed Writing to Database')); // suppress unhandled rejection
    return promise;
  }
}
