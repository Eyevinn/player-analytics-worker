import { Worker } from './index';
import Logger from './logging/logger';

//For Demo...
const myWorker = new Worker({ logger: Logger });
myWorker.start();
