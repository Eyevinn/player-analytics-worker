import winston from "winston";

export abstract class AbstractDBAdapter {
  logger: winston.Logger;
  client: any;
  /**
   * Attempts to connect to the database.
   * This should throw an Error when the connection couldn't be established.
   */
  abstract connect(): Promise<void>;

  /**
   * Creates the database (schema).
   */
  abstract create(): Promise<void>;

  /**
   * Should validate that the database's schema is intact.
   */
  abstract validate(): Promise<void>;

  /**
   * This method defines what to do when connections to the database fail.
   */
  abstract handleUnreachable(): Promise<void>;

  /**
   * Returns a connected state.
   * The adapter defines, when it's "connected".
   */
  abstract get connected(): boolean;

  abstract setItem(): Promise<void>;

  abstract getItem(): Promise<any>;
}
