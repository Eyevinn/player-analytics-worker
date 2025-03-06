import { NodeClickHouseClient } from '@clickhouse/client/dist/client';
import { createClient } from '@clickhouse/client';
import winston from 'winston';
// import {
//     AbstractDBAdapter,
//     IHandleErrorOutput,
//     ErrorType,
//     IPutItemInput,
//     IGetItemInput,
//     IGetItems,
// } from '../../types/interfaces';


export enum ErrorType {
  'ABORT' = 0,
  'CONTINUE' = 1,
}

export interface IHandleErrorOutput {
  errorType: ErrorType;
  error: Object;
}

export interface IPutItemInput {
  tableName: string;
  data: Object;
}

export interface IGetItems {
  sessionId: string;
  tableName: string;
}

export interface IGetItemInput {
  tableName: string;
  sessionId: string;
  timestamp: number;
}

export abstract class AbstractDBAdapter {
  logger: winston.Logger;
  dbClient: any;

  abstract tableExists(name: string): Promise<boolean>;
  abstract putItem(params: IPutItemInput): Promise<boolean>;
  abstract getItem(params: IGetItemInput): Promise<any>;
  abstract deleteItem(params: IGetItemInput): Promise<boolean>;
  abstract getItemsBySession(params: IGetItems): Promise<any[]>;
  abstract handleError(error: any): IHandleErrorOutput;
}

const DB_NAME = "EPAS";

export default class ClickHouseDBAdapter implements AbstractDBAdapter {
    logger: winston.Logger;
    dbClient: NodeClickHouseClient;

    constructor(logger: winston.Logger) {

        this.dbClient = createClient({
            url: process.env.CLICKHOUSE_HOST || "http://localhost:8123",
            username: process.env.CLICKHOUSE_USER || "default",
            password: process.env.CLICKHOUSE_PASSWORD || "",
            database: process.env.CLICKHOUSE_DB || "default",
        })
        this.logger = logger;
    }

    async tableExists(name: string): Promise<boolean> {
        const client = this.dbClient;
        const resultSet = await client.query({
            query: `
            SELECT table FROM system.tables
                where database like 'default' and table like {var_tableName: String}
        `,
            format: 'JSON',
            query_params: {
                var_tableName: name
            }
        })
        const result = await resultSet.json();
        // this.logger.info(`Result is '${result}' - '${result.data.length}'`);
        await client.close();
        return result.data.length>0;
    }

    async putItem(params: IPutItemInput): Promise<boolean> {
        
        this.logger.info("Put Item to Table:", params.tableName);
        try { 
           await this.dbClient.command({
                query: `
                   CREATE TABLE IF NOT EXISTS ${params.tableName} 
                   (sessionId  UInt64, event String, timestamp UInt64, playhead UInt64, duration UInt64, host String) 
                     ORDER BY (sessionId) 
                `,
            })
            await this.dbClient.close();
            return true;
        } catch (err) {
            throw this.handleError(err);
        }
    }

    async getItem({ sessionId, tableName, timestamp }: IGetItemInput): Promise<any> {
        try {
            this.logger.info("Get Item from Table:", sessionId, tableName, timestamp);
            const client = this.dbClient;
            const resultSet = await client.query({
                query: `
                    SELECT * FROM {var_tableName: String} WHERE sessionId = {var_sessionId: String} AND timestamp = {var_timestamp: UInt64}
                `,
                format: 'JSON',
                query_params: {
                    var_tableName: tableName,
                    var_sessionId: sessionId,
                    var_timestamp: timestamp
                }
            })
            const result = await resultSet.json();
            this.logger.info("Read Item from Table:", result);
            await client.close()
            return result.data;
        } catch (error) {
            throw this.handleError(error);
        }
    }

    async deleteItem(params: IGetItemInput): Promise<boolean> {
        const client = this.dbClient;
        this.logger.info("Delete Item from Table:", params);
        //      void (async () => {
        try {
            await client.command({
                query: `
                        DELETE FROM {var_tableName: String} WHERE sessionId = {var_sessionId: String} AND timestamp = {var_timestamp: UInt64}
                        `,
                query_params: {
                    var_tableName: params.tableName,
                    var_sessionId: params.sessionId,
                    var_timestamp: params.timestamp
                },
                // Recommended, by ClickHouse, for cluster usage to avoid situations
                // where a query processing error occurred after the response code
                // and HTTP headers were sent to the client.
                clickhouse_settings: {
                    wait_end_of_query: 1,
                },
            })
            await client.close();
            return true;
        } catch (error) {
            throw this.handleError(error);

        }
        //       })()
    }

    async getItemsBySession(params: IGetItems): Promise<any[]> {

        try {
            this.logger.info("Get Items by Session:", params);
            const client = this.dbClient;
            const resultSet = await client.query({
                query: `
            SELECT * FROM {var_tableName: String} WHERE sessionId = {var_sessionId: String}
            `,
                format: 'JSON',
                query_params: {
                    var_tableName: params.tableName,
                    var_sessionId: params.sessionId
                }
            })
            const result = await resultSet.json();
            await client.close();
            return result.data;
        } catch (error) {
            throw this.handleError(error);
        }
    }

    public handleError(error: any): IHandleErrorOutput {
        this.logger.error(error);
        return {
            errorType: ErrorType.ABORT,
            error: error,
        };
    }
}