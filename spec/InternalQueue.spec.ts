import InternalQueue from '../lib/InternalQueue';
import Logger from '../logging/logger';

describe('InternalQueue', () => {
  let queue: InternalQueue;

  beforeEach(() => {
    // Create a fresh queue instance for each test
    queue = new InternalQueue(Logger, 'test-instance-id');
  });

  it('should store messages when add() is called', () => {
    const mockMessage = { MessageId: 'msg1', ReceiptHandle: 'handle1' };
    const mockEvent = { event: 'play', timestamp: Date.now(), playhead: 0, duration: 0 };
    const tableName = 'epas_test_table';

    const result = queue.add(mockMessage, mockEvent, tableName, 0);

    expect(result).toBe(true);
    expect(queue.getQueueSize()).toBe(1);
  });

  it('should increase count when multiple messages are added', () => {
    const mockMessage1 = { MessageId: 'msg1', ReceiptHandle: 'handle1' };
    const mockMessage2 = { MessageId: 'msg2', ReceiptHandle: 'handle2' };
    const mockEvent = { event: 'play', timestamp: Date.now(), playhead: 0, duration: 0 };
    const tableName = 'epas_test_table';

    queue.add(mockMessage1, mockEvent, tableName, 0);
    queue.add(mockMessage2, mockEvent, tableName, 1);

    expect(queue.getQueueSize()).toBe(2);
  });

  it('should return correct batch size when getBatch() is called', () => {
    const mockEvent = { event: 'play', timestamp: Date.now(), playhead: 0, duration: 0 };
    const tableName = 'epas_test_table';

    // Add 10 messages
    for (let i = 0; i < 10; i++) {
      queue.add({ MessageId: `msg${i}` }, mockEvent, tableName, i);
    }

    // Set batch size to 5 via environment variable
    process.env.INTERNAL_QUEUE_BATCH_SIZE = '5';
    const queueWithBatchSize = new InternalQueue(Logger, 'test-instance-id');
    for (let i = 0; i < 10; i++) {
      queueWithBatchSize.add({ MessageId: `msg${i}` }, mockEvent, tableName, i);
    }

    const batch = queueWithBatchSize.getBatch();

    expect(batch.length).toBe(5);
    expect(queueWithBatchSize.getQueueSize()).toBe(5);

    delete process.env.INTERNAL_QUEUE_BATCH_SIZE;
  });

  it('should empty queue when getBatch() retrieves all items', () => {
    const mockEvent = { event: 'play', timestamp: Date.now(), playhead: 0, duration: 0 };
    const tableName = 'epas_test_table';

    // Add 3 messages
    for (let i = 0; i < 3; i++) {
      queue.add({ MessageId: `msg${i}` }, mockEvent, tableName, i);
    }

    expect(queue.getQueueSize()).toBe(3);

    const batch = queue.getBatch();

    expect(batch.length).toBe(3);
    expect(queue.getQueueSize()).toBe(0);
    expect(queue.isEmpty()).toBe(true);
  });

  it('should requeue messages back to the queue', () => {
    const mockMessage = { MessageId: 'msg1', ReceiptHandle: 'handle1' };
    const mockEvent = { event: 'play', timestamp: Date.now(), playhead: 0, duration: 0 };
    const tableName = 'epas_test_table';

    queue.add(mockMessage, mockEvent, tableName, 0);
    const batch = queue.getBatch();

    expect(queue.isEmpty()).toBe(true);

    queue.requeue(batch);

    expect(queue.getQueueSize()).toBe(1);
    expect(queue.isEmpty()).toBe(false);

    // Verify the requeued message comes back in next batch
    const nextBatch = queue.getBatch();
    expect(nextBatch.length).toBe(1);
    expect(nextBatch[0].message.MessageId).toBe('msg1');
    expect(nextBatch[0].retryCount).toBe(1); // Retry count should be incremented
  });

  it('should drop messages that exceed max retries when requeuing', () => {
    const mockMessage = { MessageId: 'msg1', ReceiptHandle: 'handle1' };
    const mockEvent = { event: 'play', timestamp: Date.now(), playhead: 0, duration: 0 };
    const tableName = 'epas_test_table';

    // Set max retries to 2 for this test
    process.env.INTERNAL_QUEUE_MAX_RETRIES = '2';
    const queueWithMaxRetries = new InternalQueue(Logger, 'test-instance-id');

    queueWithMaxRetries.add(mockMessage, mockEvent, tableName, 0);
    let batch = queueWithMaxRetries.getBatch();

    // First requeue - retryCount becomes 1
    queueWithMaxRetries.requeue(batch);
    expect(queueWithMaxRetries.getQueueSize()).toBe(1);

    batch = queueWithMaxRetries.getBatch();

    // Second requeue - retryCount becomes 2
    queueWithMaxRetries.requeue(batch);
    expect(queueWithMaxRetries.getQueueSize()).toBe(1);

    batch = queueWithMaxRetries.getBatch();

    // Third requeue - retryCount would be 3, should be dropped
    queueWithMaxRetries.requeue(batch);
    expect(queueWithMaxRetries.getQueueSize()).toBe(0);
    expect(queueWithMaxRetries.isEmpty()).toBe(true);

    delete process.env.INTERNAL_QUEUE_MAX_RETRIES;
  });

  it('should return true when queue is at capacity with isFull check via hasCapacity', () => {
    const mockEvent = { event: 'play', timestamp: Date.now(), playhead: 0, duration: 0 };
    const tableName = 'epas_test_table';

    // Set max queue size to 5 for this test
    process.env.INTERNAL_QUEUE_MAX_SIZE = '5';
    const queueWithMaxSize = new InternalQueue(Logger, 'test-instance-id');

    // Add 5 messages (at capacity)
    for (let i = 0; i < 5; i++) {
      const result = queueWithMaxSize.add({ MessageId: `msg${i}` }, mockEvent, tableName, i);
      expect(result).toBe(true);
    }

    expect(queueWithMaxSize.hasCapacity(1)).toBe(false);
    expect(queueWithMaxSize.getAvailableCapacity()).toBe(0);

    // Try to add one more - should fail
    const result = queueWithMaxSize.add({ MessageId: 'msg6' }, mockEvent, tableName, 6);
    expect(result).toBe(false);
    expect(queueWithMaxSize.getQueueSize()).toBe(5);

    delete process.env.INTERNAL_QUEUE_MAX_SIZE;
  });

  it('should group messages by table name correctly', () => {
    const mockEvent1 = { event: 'play', timestamp: Date.now(), playhead: 0, duration: 0, host: 'tenant.one' };
    const mockEvent2 = { event: 'pause', timestamp: Date.now(), playhead: 30, duration: 0, host: 'tenant.one' };
    const mockEvent3 = { event: 'seek', timestamp: Date.now(), playhead: 60, duration: 0, host: 'tenant.two' };

    queue.add({ MessageId: 'msg1' }, mockEvent1, 'epas_tenant.one', 0);
    queue.add({ MessageId: 'msg2' }, mockEvent2, 'epas_tenant.one', 1);
    queue.add({ MessageId: 'msg3' }, mockEvent3, 'epas_tenant.two', 2);

    const batch = queue.getBatch();
    const grouped = queue.groupByTable(batch);

    expect(Object.keys(grouped).length).toBe(2);
    expect(grouped['epas_tenant.one'].length).toBe(2);
    expect(grouped['epas_tenant.two'].length).toBe(1);

    // Verify correct events are grouped
    expect(grouped['epas_tenant.one'][0].event.event).toBe('play');
    expect(grouped['epas_tenant.one'][1].event.event).toBe('pause');
    expect(grouped['epas_tenant.two'][0].event.event).toBe('seek');
  });

  it('should return correct stats with size and oldest message age', () => {
    const mockEvent = { event: 'play', timestamp: Date.now(), playhead: 0, duration: 0 };
    const tableName = 'epas_test_table';

    // Empty queue should return null for oldestMessage
    let stats = queue.getStats();
    expect(stats.size).toBe(0);
    expect(stats.oldestMessage).toBe(null);

    // Add messages with slight delay to test oldest message calculation
    queue.add({ MessageId: 'msg1' }, mockEvent, tableName, 0);
    queue.add({ MessageId: 'msg2' }, mockEvent, tableName, 1);

    stats = queue.getStats();
    expect(stats.size).toBe(2);
    expect(stats.oldestMessage).toBeGreaterThanOrEqual(0);
    expect(stats.oldestMessage).not.toBe(null);
  });

  it('should return empty batch when queue is empty', () => {
    const batch = queue.getBatch();
    expect(batch.length).toBe(0);
    expect(queue.isEmpty()).toBe(true);
  });

  it('should clear all messages when clear() is called', () => {
    const mockEvent = { event: 'play', timestamp: Date.now(), playhead: 0, duration: 0 };
    const tableName = 'epas_test_table';

    for (let i = 0; i < 5; i++) {
      queue.add({ MessageId: `msg${i}` }, mockEvent, tableName, i);
    }

    expect(queue.getQueueSize()).toBe(5);

    queue.clear();

    expect(queue.getQueueSize()).toBe(0);
    expect(queue.isEmpty()).toBe(true);
  });

  it('should handle hasCapacity correctly for multiple items', () => {
    const mockEvent = { event: 'play', timestamp: Date.now(), playhead: 0, duration: 0 };
    const tableName = 'epas_test_table';

    process.env.INTERNAL_QUEUE_MAX_SIZE = '10';
    const queueWithMaxSize = new InternalQueue(Logger, 'test-instance-id');

    // Add 7 messages
    for (let i = 0; i < 7; i++) {
      queueWithMaxSize.add({ MessageId: `msg${i}` }, mockEvent, tableName, i);
    }

    expect(queueWithMaxSize.hasCapacity(3)).toBe(true); // 7 + 3 = 10, exactly at capacity
    expect(queueWithMaxSize.hasCapacity(4)).toBe(false); // 7 + 4 = 11, exceeds capacity
    expect(queueWithMaxSize.getAvailableCapacity()).toBe(3);

    delete process.env.INTERNAL_QUEUE_MAX_SIZE;
  });

  it('should maintain FIFO order when retrieving batches', () => {
    const tableName = 'epas_test_table';

    for (let i = 0; i < 5; i++) {
      const mockEvent = { event: 'play', timestamp: Date.now(), playhead: i * 10, duration: 0 };
      queue.add({ MessageId: `msg${i}` }, mockEvent, tableName, i);
    }

    const batch = queue.getBatch();

    // Verify messages are returned in the order they were added
    for (let i = 0; i < 5; i++) {
      expect(batch[i].message.MessageId).toBe(`msg${i}`);
      expect(batch[i].event.playhead).toBe(i * 10);
      expect(batch[i].messageIndex).toBe(i);
    }
  });

  it('should preserve message data structure when requeuing', () => {
    const mockMessage = { MessageId: 'msg1', ReceiptHandle: 'handle1', Body: 'test-body' };
    const mockEvent = { event: 'play', timestamp: 12345, playhead: 100, duration: 600 };
    const tableName = 'epas_test_table';

    queue.add(mockMessage, mockEvent, tableName, 42);
    const batch = queue.getBatch();

    queue.requeue(batch);
    const nextBatch = queue.getBatch();

    expect(nextBatch[0].message.MessageId).toBe('msg1');
    expect(nextBatch[0].message.ReceiptHandle).toBe('handle1');
    expect(nextBatch[0].message.Body).toBe('test-body');
    expect(nextBatch[0].event.event).toBe('play');
    expect(nextBatch[0].event.timestamp).toBe(12345);
    expect(nextBatch[0].event.playhead).toBe(100);
    expect(nextBatch[0].tableName).toBe('epas_test_table');
    expect(nextBatch[0].messageIndex).toBe(42);
  });
});
