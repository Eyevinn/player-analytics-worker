import {
  CreateTableCommand,
  ListTablesCommand,
  PutItemCommand,
  DynamoDBClient,
  GetItemCommand,
  DeleteItemCommand,
} from '@aws-sdk/client-dynamodb';
import winston from 'winston';
import {
  AbstractDBAdapter,
  IDDBGetItemInput,
  IDDBPutItemInput,
} from '../types/interfaces';
import { v4 as uuidv4 } from 'uuid';

export class DynamoDBAdapter implements AbstractDBAdapter {
  logger: winston.Logger;
  dbClient: DynamoDBClient;

  constructor(logger: winston.Logger) {
    this.dbClient = new DynamoDBClient({
      region: process.env.AWS_REGION,
      maxAttempts: 5,
    });
    this.logger = logger;
  }

  async getTableNames(): Promise<string[]> {
    const tablesData = await this.dbClient.send(
      new ListTablesCommand({ Limit: 100 })
    );
    if (tablesData.TableNames) {
      return tablesData.TableNames;
    }
    return [];
  }

  async createTable(tableName: string): Promise<void> {
    try {
      const params = {
        AttributeDefinitions: [
          {
            AttributeName: 'eventId',
            AttributeType: 'S',
          },
        ],
        KeySchema: [
          {
            AttributeName: 'eventId',
            KeyType: 'HASH',
          },
        ],
        ProvisionedThroughput: {
          ReadCapacityUnits: 3,
          WriteCapacityUnits: 3,
        },
        TableName: tableName,
        StreamSpecification: {
          StreamEnabled: false,
        },
      };
      await this.dbClient.send(new CreateTableCommand(params));
      this.logger.info(`Created Table '${tableName}'`);
    } catch (err) {
      this.logger.error('Table creation Error!');
      throw new Error(err);
    }
  }

  async putItem(params: IDDBPutItemInput): Promise<void> {
    const eventItem = {
      eventId: { S: uuidv4() },
    };
    Object.keys(params.data).forEach((key) => {
      eventItem[key] = { S: JSON.stringify(params.data[key]) };
    });

    try {
      await this.dbClient.send(
        new PutItemCommand({
          TableName: params.tableName,
          Item: eventItem,
        })
      );
      this.logger.debug(
        `Put event with event ID:${eventItem.eventId['S']} in Table:${params.tableName}`
      );
    } catch (err) {
      this.logger.error(err);
      throw new Error(err);
    }
  }

  async getItem(params: IDDBGetItemInput): Promise<any> {
    try {
      const rawData = await this.dbClient.send(
        new GetItemCommand({
          TableName: params.tableName,
          Key: { eventId: { S: params.eventId } },
        })
      );
      this.logger.debug('Read Item from Table');
      return rawData;
    } catch (err) {
      this.logger.error(err);
    }
  }

  async deleteItem(params: IDDBGetItemInput): Promise<any> {
    try {
      const data = await this.dbClient.send(
        new DeleteItemCommand({
          TableName: params.tableName,
          Key: { eventId: { S: params.eventId } },
        })
      );
      this.logger.debug('Deleted Item from Table', data);
      return data;
    } catch (err) {
      this.logger.error(err);
    }
  }

  async getItemsBySession(params: any): Promise<any> {
    //TODO
  }
}
