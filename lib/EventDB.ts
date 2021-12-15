import winston from "winston";

export default class EventDB {
  logger: winston.Logger;
  DBAdapter: any;
  table: string;

  constructor(logger: winston.Logger) {
    this.logger = logger;
  }

  public async createTable(name: string) {
    try {
      await this._getDBAdapter();
      if (this.table === undefined) {
        await this.DBAdapter.createTable(name);
        this.table = name;
      }
    } catch (err) {
      this.logger.warn("Problem when creating table");
      throw new Error(err);
    }
  }

  private async _getDBAdapter(): Promise<void> {
    if (this.DBAdapter === undefined) {
      let dbAdapter: any;
      switch (process.env.DB_TYPE) {
        case "dynamodb":
          dbAdapter = (await import("../adapters/DynamoDBAdapter"))
            .DynamoDBAdapter;
          break;
        default:
          this.logger.warn("No database type specified");
          throw new Error("No database type specified");
      }
      this.DBAdapter = new dbAdapter(this.logger);
    }
  }

  public async write(event: any): Promise<void> {
    try {
      await this.DBAdapter.putItem({
        tableName: this.table,
        data: event,
      });
    } catch (err) {
      throw new Error(err);
    }
  }
}
