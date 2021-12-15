require("dotenv").config();
import {
  CreateTableCommand,
  ListTablesCommand,
  PutItemCommand,
  DynamoDBClient,
  GetItemCommand,
  DeleteItemCommand,
} from "@aws-sdk/client-dynamodb";
import winston from "winston";
import {
  AbstractDBAdapter,
  IDDBGetItemInput,
  IDDBPutItemInput,
} from "../types/interfaces";
import { v4 as uuidv4 } from "uuid";
import { DynamoDB } from "aws-sdk";

export class DynamoDBAdapter implements AbstractDBAdapter {
  logger: winston.Logger;
  dbClient: DynamoDBClient;
  docClient: any;

  constructor(logger: winston.Logger) {
    this.dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
    this.docClient = new DynamoDB.DocumentClient();
    this.logger = logger;
  }

  async createTable(tableName: string): Promise<void> {
    try {
      const tablesData = await this.dbClient.send(
        new ListTablesCommand({ Limit: 100 })
      );
      // Create new Table if none exists
      if (
        !tablesData.TableNames ||
        !tablesData.TableNames.includes(tableName)
      ) {
        const params = {
          AttributeDefinitions: [
            {
              AttributeName: "eventId",
              AttributeType: "S",
            },
          ],
          KeySchema: [
            {
              AttributeName: "eventId",
              KeyType: "HASH",
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
        // Create a new table
        const data = await this.dbClient.send(new CreateTableCommand(params));
        this.logger.info("Table Created", data);
      }
    } catch (err) {
      this.logger.error("Error", err);
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
      this.logger.info("Put to Table");
    } catch (err) {
      this.logger.error("Error", err);
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
      this.logger.info("Read from Table");
      return rawData;
    } catch (err) {
      this.logger.error("Error", err);
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
      this.logger.info("Deleted from Table", data);
      return data;
    } catch (err) {
      this.logger.error("Error", err);
    }
  }

  async getItemsBySession(params: any): Promise<any> {
    //TODO
  }
}
