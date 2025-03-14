import {
  CreateTableCommand,
  ListTablesCommand,
  PutItemCommand,
  DynamoDBClient,
  GetItemCommand,
  DeleteItemCommand,
  DescribeTableCommand,
  DescribeTableCommandOutput,
  AttributeValue,
  CreateTableCommandOutput,
  PutItemCommandOutput,
  GetItemCommandOutput,
  DeleteItemCommandOutput,
  ListTablesCommandOutput,
} from '@aws-sdk/client-dynamodb';
import { AwsError, mockClient } from 'aws-sdk-client-mock';
import Logger from '../logging/logger';
import { Worker, WorkerState } from '../index';
import {
  DeleteMessageCommand,
  DeleteMessageCommandOutput,
  ReceiveMessageCommand,
  ReceiveMessageCommandOutput,
  SQSClient,
} from '@aws-sdk/client-sqs';
import Queue from '../lib/Queue';
import EventDB from '../lib/EventDB';
import { SqsQueueAdapter } from '@eyevinn/player-analytics-shared';

const ddbMock = mockClient(DynamoDBClient);
const sqsMock = mockClient(SQSClient);

let receiveMsgReply: ReceiveMessageCommandOutput[];
let deleteMsgReply: DeleteMessageCommandOutput;
let listTableReply: ListTablesCommandOutput;
let createTableReply: CreateTableCommandOutput;
let putItemReply: PutItemCommandOutput;
let describeTableReply: DescribeTableCommandOutput;

describe('A Worker', () => {
  beforeEach(() => {
    process.env.QUEUE_TYPE = 'SQS';
    process.env.AWS_REGION = 'us-north-1';
    process.env.DB_TYPE = 'DYNAMODB';

    const queueAdapter = spyOn(SqsQueueAdapter.prototype as any, 'checkQueueExists').and.returnValue(true);
    sqsMock.reset();
    ddbMock.reset();

    receiveMsgReply = [
      {
        $metadata: {},
      },
      {
        $metadata: {},
        Messages: [
          {
            MessageId: '62686810-05ba-4b43-62730ff3156g7jd3',
            ReceiptHandle:
              'MbZj6wDWli+JvwwJaBV+3dcjk2YW2vA3' +
              '+STFFljTM8tJJg6HRG6PYSasuWXPJB+C' +
              'wLj1FjgXUv1uSj1gUPAWV66FU/WeR4mq' +
              '2OKpEGYWbnLmpRCJVAyeMjeU5ZBdtcQ+' +
              'QEauMZc8ZRv37sIW2iJKq3M9MFx1YvV11A2x/KSbkJ0=',
            MD5OfBody: 'fafb00f5732ab283681e124bf8747ed1',
            Body: JSON.stringify({
              event: 'loading',
              timestamp: 0,
              playhead: 0,
              duration: 0,
              host: 'mock.tenant.one',
            }),
          },
        ],
      },
    ];
    deleteMsgReply = {
      $metadata: {
        requestId: '123-123-123-123',
      },
    };
    listTableReply = {
      $metadata: {},
      TableNames: ['epas_mock.tenant.old'],
    };
    createTableReply = {
      $metadata: {},
      TableDescription: {
        TableName: 'epas_mock.tenant.one',
      },
    };
    putItemReply = {
      $metadata: {
        requestId: '123-123-abc-abc',
      },
    };
    describeTableReply = {
      $metadata: {
        requestId: '123-123-abc-abc',
        httpStatusCode: 200,
      },
      Table: {
        TableStatus: 'ACTIVE',
      },
    };
  });

  afterEach(() => {
    delete process.env.QUEUE_TYPE;
    delete process.env.AWS_REGION;
    delete process.env.DB_TYPE;
  });

  it('should not have the same workerId as an other Worker', async () => {
    const workerA = new Worker({ logger: Logger });
    const workerB = new Worker({ logger: Logger });
    expect(workerA.workerId).not.toBe(workerB.workerId);
  });

  it('should receive Queue messages, push to database, remove messages from Queue', async () => {
    const spyTableExists = spyOn(EventDB.prototype, 'TableExists').and.callThrough();
    const spyWrite = spyOn(EventDB.prototype, 'write').and.callThrough();
    const spyRemove = spyOn(Queue.prototype, 'remove').and.callThrough();
    const spyGetEvent = spyOn(
      Queue.prototype,
      'getEventJSONsFromMessages'
    ).and.callThrough();

    const testWorker = new Worker({ logger: Logger });

    sqsMock.on(ReceiveMessageCommand).callsFake(() => receiveMsgReply[1]);
    sqsMock.on(DeleteMessageCommand).resolves(deleteMsgReply);
    ddbMock.on(PutItemCommand).resolves(putItemReply);
    ddbMock.on(DescribeTableCommand).resolves(describeTableReply);
    // Test the Worker
    testWorker.setLoopIterations(1);
    await testWorker.startAsync();

    expect(spyTableExists).toHaveBeenCalled();
    expect(spyWrite).toHaveBeenCalled();
    expect(spyGetEvent).toHaveBeenCalled();
    expect(spyRemove).toHaveBeenCalled();
  });

  // Try Again if messages = 0
  it('should receive Queue messages, push to database, remove messages from Queue', async () => {
    const spyTableExists = spyOn(EventDB.prototype, 'TableExists').and.callThrough();
    const spyWrite = spyOn(EventDB.prototype, 'write').and.callThrough();
    const spyRemove = spyOn(Queue.prototype, 'remove').and.callThrough();
    const spyGetEvent = spyOn(
      Queue.prototype,
      'getEventJSONsFromMessages'
    ).and.callThrough();

    const testWorker = new Worker({ logger: Logger });

    sqsMock.on(ReceiveMessageCommand).callsFake(() => receiveMsgReply.shift());
    sqsMock.on(DeleteMessageCommand).resolves(deleteMsgReply);
    ddbMock.on(PutItemCommand).resolves(putItemReply);
    ddbMock.on(DescribeTableCommand).resolves(describeTableReply);
    // Test the Worker
    testWorker.setLoopIterations(2);
    await testWorker.startAsync();

    expect(spyTableExists).toHaveBeenCalled();
    expect(spyWrite).toHaveBeenCalled();
    expect(spyGetEvent).toHaveBeenCalled();
    expect(spyRemove).toHaveBeenCalled();
  });

  it('should not push item to DB if target table does not exist', async () => {
    const itemReply: AwsError = {
      Type: 'Sender',
      Code: 'ResourceNotFoundException',
      name: 'ResourceNotFoundException',
      message: 'Requested resource not found',
      $fault: 'client',
      $metadata: {
        httpStatusCode: 400,
        requestId: 'df840ab9-e68b-5c0e-b4a0-5094f2dfaee8',
        attempts: 1,
        totalRetryDelay: 0,
      },
    };
    const recieveMessageReply = {
      $metadata: {},
      Messages: [
        {
          MessageId: '62686810-05ba-4b43-62730ff3156g7jd3',
          ReceiptHandle:
            'MbZj6wDWli+JvwwJaBV+3dcjk2YW2vA3' +
            '+STFFljTM8tJJg6HRG6PYSasuWXPJB+C' +
            'wLj1FjgXUv1uSj1gUPAWV66FU/WeR4mq' +
            '2OKpEGYWbnLmpRCJVAyeMjeU5ZBdtcQ+' +
            'QEauMZc8ZRv37sIW2iJKq3M9MFx1YvV11A2x/KSbkJ0=',
          MD5OfBody: 'fafb00f5732ab283681e124bf8747ed1',
          Body: JSON.stringify({
            event: 'loading',
            timestamp: Date.now(),
            playhead: 0,
            duration: 0,
            host: 'mock.tenant.one',
          }),
        },
      ],
    };
    const spyTableExists = spyOn(EventDB.prototype, 'TableExists').and.callThrough();
    const spyWrite = spyOn(EventDB.prototype, 'write').and.callThrough();
    const spyRemove = spyOn(Queue.prototype, 'remove').and.callThrough();
    const spyGetEvent = spyOn(
      Queue.prototype,
      'getEventJSONsFromMessages'
    ).and.callThrough();

    const testWorker = new Worker({ logger: Logger });
    sqsMock.on(ReceiveMessageCommand).callsFake(() => recieveMessageReply);
    sqsMock.on(DeleteMessageCommand).resolves(deleteMsgReply);
    ddbMock.on(DescribeTableCommand).rejects(itemReply);
    // Test the Worker
    testWorker.setLoopIterations(1);
    await testWorker.startAsync();

    expect(spyTableExists).toHaveBeenCalled();
    expect(spyWrite).not.toHaveBeenCalled();
    expect(spyGetEvent).toHaveBeenCalled();
    expect(spyRemove).not.toHaveBeenCalled();
  });

  it('should not push item to DB if target table status is not ACTIVE', async () => {
    const describeTableReply: DescribeTableCommandOutput = {
      $metadata: {
        requestId: '123-123-abc-abc',
        httpStatusCode: 200,
      },
      Table: {
        TableStatus: 'CREATING',
      },
    };
    const recieveMessageReply = {
      $metadata: {},
      Messages: [
        {
          MessageId: '62686810-05ba-4b43-62730ff3156g7jd3',
          ReceiptHandle:
            'MbZj6wDWli+JvwwJaBV+3dcjk2YW2vA3' +
            '+STFFljTM8tJJg6HRG6PYSasuWXPJB+C' +
            'wLj1FjgXUv1uSj1gUPAWV66FU/WeR4mq' +
            '2OKpEGYWbnLmpRCJVAyeMjeU5ZBdtcQ+' +
            'QEauMZc8ZRv37sIW2iJKq3M9MFx1YvV11A2x/KSbkJ0=',
          MD5OfBody: 'fafb00f5732ab283681e124bf8747ed1',
          Body: JSON.stringify({
            event: 'loading',
            timestamp: Date.now(),
            playhead: 0,
            duration: 0,
            host: 'mock.tenant.one',
          }),
        },
      ],
    };
    const spyTableExists = spyOn(EventDB.prototype, 'TableExists').and.callThrough();
    const spyWrite = spyOn(EventDB.prototype, 'write').and.callThrough();
    const spyRemove = spyOn(Queue.prototype, 'remove').and.callThrough();
    const spyGetEvent = spyOn(
      Queue.prototype,
      'getEventJSONsFromMessages'
    ).and.callThrough();

    const testWorker = new Worker({ logger: Logger });
    sqsMock.on(ReceiveMessageCommand).callsFake(() => recieveMessageReply);
    sqsMock.on(DeleteMessageCommand).resolves(deleteMsgReply);
    ddbMock.on(DescribeTableCommand).resolves(describeTableReply);
    // Test the Worker
    testWorker.setLoopIterations(1);
    await testWorker.startAsync();

    expect(spyTableExists).toHaveBeenCalled();
    expect(spyWrite).not.toHaveBeenCalled();
    expect(spyGetEvent).toHaveBeenCalled();
    expect(spyRemove).not.toHaveBeenCalled();
  });

  it('should remove item from queue if it has expired and the target table does not exist', async () => {
    const itemReply: AwsError = {
      Type: 'Sender',
      Code: 'ResourceNotFoundException',
      name: 'ResourceNotFoundException',
      message: 'Requested resource not found',
      $fault: 'client',
      $metadata: {
        httpStatusCode: 400,
        requestId: 'df840ab9-e68b-5c0e-b4a0-5094f2dfaee8',
        attempts: 1,
        totalRetryDelay: 0,
      },
    };
    const spyTableExists = spyOn(EventDB.prototype, 'TableExists').and.callThrough();
    const spyWrite = spyOn(EventDB.prototype, 'write').and.callThrough();
    const spyRemove = spyOn(Queue.prototype, 'remove').and.callThrough();
    const spyGetEvent = spyOn(
      Queue.prototype,
      'getEventJSONsFromMessages'
    ).and.callThrough();

    const testWorker = new Worker({ logger: Logger });
    sqsMock.on(ReceiveMessageCommand).callsFake(() => receiveMsgReply[1]);
    sqsMock.on(DeleteMessageCommand).resolves(deleteMsgReply);
    ddbMock.on(DescribeTableCommand).rejects(itemReply);
    // Test the Worker
    testWorker.setLoopIterations(1);
    await testWorker.startAsync();

    expect(spyTableExists).toHaveBeenCalled();
    expect(spyWrite).not.toHaveBeenCalled();
    expect(spyGetEvent).toHaveBeenCalled();
    expect(spyRemove).toHaveBeenCalled();
  });

  it('should only remove messages from queue if they have been successfully added to database', async () => {
    const spyTableExists = spyOn(EventDB.prototype, 'TableExists').and.callThrough();
    const spyWrite = spyOn(EventDB.prototype, 'write').and.callThrough();
    const spyRemove = spyOn(Queue.prototype, 'remove').and.callThrough();
    const spyGetEvent = spyOn(
      Queue.prototype,
      'getEventJSONsFromMessages'
    ).and.callThrough();

    const testWorker = new Worker({ logger: Logger });
    const itemReply: AwsError = {
      Type: 'Sender',
      Code: 'ResourceNotFoundException',
      name: 'ResourceNotFoundException',
      message: 'Requested resource not found',
      $fault: 'client',
      $metadata: {
        httpStatusCode: 400,
        requestId: 'df840ab9-e68b-5c0e-b4a0-5094f2dfaee8',
        attempts: 1,
        totalRetryDelay: 0,
      },
    };
    sqsMock.on(ReceiveMessageCommand).callsFake(() => receiveMsgReply[1]);
    ddbMock.on(PutItemCommand).rejects(itemReply);
    ddbMock.on(DescribeTableCommand).resolves(describeTableReply);
    // Test the Worker
    testWorker.setLoopIterations(1);
    await testWorker.startAsync();

    expect(spyTableExists).toHaveBeenCalled();
    expect(spyWrite).toHaveBeenCalled();
    expect(spyGetEvent).toHaveBeenCalled();
    expect(spyRemove).not.toHaveBeenCalled();
  });

  it('should stop if an unwanted DB error occurs', async () => {
    const spyTableExists = spyOn(EventDB.prototype, 'TableExists').and.callThrough();
    const spyWrite = spyOn(EventDB.prototype, 'write').and.callThrough();
    const spyRemove = spyOn(Queue.prototype, 'remove').and.callThrough();
    const spyGetEvent = spyOn(
      Queue.prototype,
      'getEventJSONsFromMessages'
    ).and.callThrough();

    const testWorker = new Worker({ logger: Logger });
    const itemReply: AwsError = {
      Type: 'Sender',
      Code: 'RequestLimitExceeded',
      name: 'RequestLimitExceeded',
      message: 'Throughput exceeds the current throughput limit for your account.',
      $fault: 'client',
      $metadata: {
        httpStatusCode: 400,
        requestId: 'df840ab9-e68b-5c0e-b4a0-5094f2dfaee8',
        attempts: 1,
        totalRetryDelay: 0,
      },
    };
    sqsMock.on(ReceiveMessageCommand).callsFake(() => receiveMsgReply[1]);
    ddbMock.on(PutItemCommand).rejects(itemReply);
    ddbMock.on(DescribeTableCommand).resolves(describeTableReply);
    // Test the Worker
    testWorker.setLoopIterations(1);
    await testWorker.startAsync();

    expect(spyTableExists).toHaveBeenCalled();
    expect(spyWrite).toHaveBeenCalled();
    expect(spyGetEvent).toHaveBeenCalled();
    expect(spyRemove).not.toHaveBeenCalled();
    expect(testWorker.state).toEqual(WorkerState.INACTIVE);
  });
});
