import winston from 'winston';

export interface IDDBPutItemInput {
  tableName: string;
  data: Object;
}
export interface IDDBGetItemInput {
  tableName: string;
  eventId: string;
}

export interface IHandleErrorOutput {
  errorType: string;
  message: Object;
}
export abstract class AbstractDBAdapter {
  logger: winston.Logger;
  dbClient: any;

  abstract getTableNames(): Promise<string[]>;
  abstract createTable(name: string): Promise<void>;
  abstract putItem(params: Object): Promise<void>;
  abstract getItem(params: Object): Promise<any>;
  abstract deleteItem(params: Object): Promise<void>;
  abstract getItemsBySession(params: Object): Promise<any>;
  abstract handleError(error: any): IHandleErrorOutput;
}
