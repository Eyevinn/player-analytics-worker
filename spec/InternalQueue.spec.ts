import Logger from '../logging/logger';
import InternalQueue from '../lib/InternalQueue';

Logger.silent = true;

function makeMessage(event: string, table: string, index: number = 0) {
  return {
    message: { MessageId: `msg-${index}` },
    event: { event, timestamp: Date.now() },
    tableName: table,
    messageIndex: index,
  };
}

describe('InternalQueue', () => {
  let queue: InternalQueue;

  beforeEach(() => {
    delete process.env.INTERNAL_QUEUE_BATCH_SIZE;
    delete process.env.INTERNAL_QUEUE_MAX_SIZE;
    delete process.env.INTERNAL_QUEUE_MAX_RETRIES;
    queue = new InternalQueue(Logger, 'test-queue');
  });

  describe('add()', () => {
    it('should add a message and report correct size', () => {
      const added = queue.add({ id: '1' }, { event: 'playing' }, 'epas_default', 0);
      expect(added).toBe(true);
      expect(queue.getQueueSize()).toBe(1);
    });

    it('should reject when queue is full', () => {
      queue.maxQueueSize = 2;
      queue.add({}, {}, 'table', 0);
      queue.add({}, {}, 'table', 1);
      const added = queue.add({}, {}, 'table', 2);
      expect(added).toBe(false);
      expect(queue.getQueueSize()).toBe(2);
    });
  });

  describe('getBatch()', () => {
    it('should return empty array when queue is empty', () => {
      expect(queue.getBatch()).toEqual([]);
    });

    it('should return a batch of correct size', () => {
      process.env.INTERNAL_QUEUE_BATCH_SIZE = '3';
      queue = new InternalQueue(Logger, 'test-batch');

      for (let i = 0; i < 5; i++) {
        queue.add({}, { event: `e${i}` }, 'table', i);
      }

      const batch = queue.getBatch();
      expect(batch.length).toBe(3);
      expect(queue.getQueueSize()).toBe(2);

      const batch2 = queue.getBatch();
      expect(batch2.length).toBe(2);
      expect(queue.getQueueSize()).toBe(0);
    });

    it('should preserve FIFO order', () => {
      for (let i = 0; i < 5; i++) {
        queue.add({}, { event: `event-${i}` }, 'table', i);
      }

      process.env.INTERNAL_QUEUE_BATCH_SIZE = '2';
      queue = new InternalQueue(Logger, 'test-order');
      for (let i = 0; i < 5; i++) {
        queue.add({}, { event: `event-${i}` }, 'table', i);
      }

      const batch1 = queue.getBatch();
      expect(batch1[0].event.event).toBe('event-0');
      expect(batch1[1].event.event).toBe('event-1');

      const batch2 = queue.getBatch();
      expect(batch2[0].event.event).toBe('event-2');
      expect(batch2[1].event.event).toBe('event-3');
    });

    it('should compact after consuming more than half the array', () => {
      process.env.INTERNAL_QUEUE_BATCH_SIZE = '3';
      queue = new InternalQueue(Logger, 'test-compact');

      for (let i = 0; i < 6; i++) {
        queue.add({}, { event: `e${i}` }, 'table', i);
      }

      // Consume first batch (3 items) — headIndex = 3, array length = 6 → triggers compact
      queue.getBatch();
      expect(queue.getQueueSize()).toBe(3);

      // Add more — should work normally after compaction
      queue.add({}, { event: 'e6' }, 'table', 6);
      expect(queue.getQueueSize()).toBe(4);
    });
  });

  describe('requeue()', () => {
    it('should prepend requeued messages', () => {
      process.env.INTERNAL_QUEUE_BATCH_SIZE = '2';
      queue = new InternalQueue(Logger, 'test-requeue');

      for (let i = 0; i < 4; i++) {
        queue.add({}, { event: `e${i}` }, 'table', i);
      }

      const batch = queue.getBatch(); // gets e0, e1
      // Requeue e0, e1
      queue.requeue(batch);

      // Next batch should get requeued items first
      const batch2 = queue.getBatch();
      expect(batch2[0].event.event).toBe('e0');
      expect(batch2[1].event.event).toBe('e1');
    });

    it('should drop messages that exceed max retries', () => {
      process.env.INTERNAL_QUEUE_MAX_RETRIES = '1';
      queue = new InternalQueue(Logger, 'test-retry-limit');

      queue.add({}, { event: 'e0' }, 'table', 0);
      const batch = queue.getBatch();

      // First requeue — retryCount goes to 1 (at max)
      queue.requeue(batch);
      expect(queue.getQueueSize()).toBe(1);

      const batch2 = queue.getBatch();
      // Second requeue — retryCount would be 2, exceeds max (1)
      queue.requeue(batch2);
      expect(queue.getQueueSize()).toBe(0); // dropped
    });
  });

  describe('groupByTable()', () => {
    it('should group messages by table name', () => {
      const messages = [
        { ...makeMessage('play', 'epas_site_a', 0), retryCount: 0, addedAt: Date.now() },
        { ...makeMessage('pause', 'epas_site_b', 1), retryCount: 0, addedAt: Date.now() },
        { ...makeMessage('seek', 'epas_site_a', 2), retryCount: 0, addedAt: Date.now() },
      ];

      const grouped = queue.groupByTable(messages);
      expect(Object.keys(grouped).length).toBe(2);
      expect(grouped['epas_site_a'].length).toBe(2);
      expect(grouped['epas_site_b'].length).toBe(1);
    });
  });

  describe('utility methods', () => {
    it('isEmpty should reflect queue state', () => {
      expect(queue.isEmpty()).toBe(true);
      queue.add({}, {}, 'table', 0);
      expect(queue.isEmpty()).toBe(false);
    });

    it('clear should empty the queue', () => {
      queue.add({}, {}, 'table', 0);
      queue.add({}, {}, 'table', 1);
      queue.clear();
      expect(queue.getQueueSize()).toBe(0);
      expect(queue.isEmpty()).toBe(true);
    });

    it('hasCapacity should check against maxQueueSize', () => {
      queue.maxQueueSize = 2;
      expect(queue.hasCapacity(1)).toBe(true);
      expect(queue.hasCapacity(2)).toBe(true);
      expect(queue.hasCapacity(3)).toBe(false);
      queue.add({}, {}, 'table', 0);
      expect(queue.hasCapacity(2)).toBe(false);
    });

    it('getStats should return size and oldest message age', () => {
      const stats = queue.getStats();
      expect(stats.size).toBe(0);
      expect(stats.oldestMessage).toBeNull();

      queue.add({}, {}, 'table', 0);
      const stats2 = queue.getStats();
      expect(stats2.size).toBe(1);
      expect(stats2.oldestMessage).toBeGreaterThanOrEqual(0);
    });
  });
});
