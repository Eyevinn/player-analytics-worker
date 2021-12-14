require("dotenv").config();
import {
  DynamoDB,
  CreateTableCommand,
  ListTablesCommand,
  PutItemCommand,
  DynamoDBClient,
  GetItemCommand,
  DeleteItemCommand,
  QueryCommand,
} from "@aws-sdk/client-dynamodb";
import { valid_events } from "../spec/events/test_events";
import winston from "winston";
import { AbstractDBAdapter, IDDBCommandInput } from "../types/interfaces";

export class DynamoDBAdapter implements AbstractDBAdapter {
  logger: winston.Logger;
  dbClient: DynamoDBClient;

  constructor(logger: winston.Logger) {
    this.dbClient = new DynamoDBClient({ region: process.env.AWS_REGION });
    this.logger = logger;
  }

  async createTable(tableName: string): Promise<void> {
    try {
      const tablesData = await this.dbClient.send(
        new ListTablesCommand({ Limit: 10 })
      );
      // Create new Table if none exists
      if (
        !tablesData.TableNames ||
        !tablesData.TableNames.includes(tableName)
      ) {
        const params = {
          AttributeDefinitions: [
            {
              AttributeName: "message_id",
              AttributeType: "S",
            },
          ],
          KeySchema: [
            {
              AttributeName: "message_id",
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

  async putItem(params: IDDBCommandInput): Promise<void> {
    const eventItem = {};
    Object.keys(params.data).forEach((field) => {
      eventItem[field] = { S: JSON.stringify(params.data[field]) };
    });

    try {
      const data = await this.dbClient.send(
        new PutItemCommand({
          TableName: params.tableName,
          Item: eventItem,
        })
      );
      this.logger.info("Put to Table", data);
    } catch (err) {
      this.logger.error("Error", err);
    }
  }

  async getItem(params: IDDBCommandInput): Promise<any> {
    try {
      const rawData = await this.dbClient.send(
        new GetItemCommand({
          TableName: params.tableName,
          Key: params.data["key"],
        })
      );
      this.logger.info("Read from Table", rawData);
      return rawData;
    } catch (err) {
      this.logger.error("Error", err);
    }
  }

  async deleteItem(params: IDDBCommandInput): Promise<any> {
    try {
      const data = await this.dbClient.send(
        new DeleteItemCommand({
          TableName: params.tableName,
          Key: params.data["key"],
        })
      );
      this.logger.info("Deleted from Table", data);
      return data;
    } catch (err) {
      this.logger.error("Error", err);
    }
  }

  async getSessionItems(params: IDDBCommandInput): Promise<any> {
    const queryParams = {
      KeyConditionExpression: "sessionId = :sid",
      ExpressionAttributeValues: {
        ":sid": { S: params.data["sessionId"] },
      },
      TableName: params.tableName,
    };
    try {
      const data = await this.dbClient.send(new QueryCommand(queryParams));
      this.logger.info("Got Items for Session");
      return data;
    } catch (err) {
      this.logger.error("Error", err);
    }
  }
}
