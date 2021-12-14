import winston from "winston";

export interface IDDBCommandInput {
  tableName: string;
  data: Object;
}
export abstract class AbstractDBAdapter {
  logger: winston.Logger;
  dbClient: any;

  abstract putItem(params: Object): Promise<void>;
  abstract getItem(params: Object): Promise<any>;
  abstract deleteItem(params: Object): Promise<void>;
  abstract getSessionItems(params: Object): Promise<any>;
}
