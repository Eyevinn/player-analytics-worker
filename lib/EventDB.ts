import winston from "winston";

export default class EventDB {
  logger: winston.Logger;
  DBAdapter: any;

  constructor(logger: winston.Logger) {
    this.logger = logger;
  }

  private async _getDBAdapter(): Promise<void> {
    if (this.DBAdapter === undefined) {
      let dbAdapter: any;
      switch (process.env.QUEUE_TYPE) {
        case "SQS":
          dbAdapter = await import("../adapters/DynamoDBAdapter");
          break;
        default:
          this.logger.warn("No queue type specified");
          throw new Error("No queue type specified");
      }
      console.log("I made something");
      this.DBAdapter = new dbAdapter(this.logger);
    }
  }

  // TODO: Implement DB adapter methods
}
