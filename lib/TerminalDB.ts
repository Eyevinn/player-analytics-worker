// Purpose: A simple terminal-based database adapter for debugging purposes.
// This adapter is used when the environment variable DB_TYPE is set to 'TERMINAL'.
import winston from 'winston';
import { AbstractDBAdapter } from '@eyevinn/player-analytics-shared/types/interfaces';

export default class TerminalDB {
    logger: winston.Logger;
    tableNamesCache: string[];

    constructor(logger: winston.Logger) {
        this.logger = logger;
        this.logger.info('Using terminal db');
        this.tableNamesCache = [];
    }

    tableExists(tableName: string): Promise<boolean> {
        return new Promise((resolve, reject) => {
            this.logger.info(`Checking if table exists ${tableName}`);
            if (!this.tableNamesCache.includes(tableName)) {
                resolve(false);
                this.tableNamesCache.push(tableName);
            }
            resolve(true);
        });
    }

    stringifyParams(params: any): string {
        try {
            // Convert the params object to a formatted JSON string with 2-space indentation
            return JSON.stringify(params, null, 2);
        } catch (err) {
            // Handle circular references or other JSON stringification errors
            return `(object not fully stringifiable): ${params}`;
        }
    }

    putItem(params: any): Promise<boolean> {
        return new Promise((resolve, reject) => {  
            this.logger.info(`Writing to db:\n${this.stringifyParams(params)}`);
            resolve(true);
        });
    }

    getItem(params: any): Promise<any> {
        return new Promise((resolve, reject) => {
            this.logger.info(`Reading from db:\n${this.stringifyParams(params)}`);
            resolve({});
        });
    }

    deleteItem(params: any): Promise<boolean> {
        return new Promise((resolve, reject) => {
            this.logger.info(`Deleting from db:\n${this.stringifyParams(params)}`);
            resolve(true);
        });
    }

    getItemsBySession(params: any): Promise<any[]> {
        return new Promise((resolve, reject) => {
            this.logger.info(`Reading from db:\n${this.stringifyParams(params)}`);
            resolve([]);
        });
    }
    handleError(error: any): any {  
        this.logger.error(`Error: ${error}`);
        return error;   
    }
}