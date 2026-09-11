import Logger from '../logging/logger';
import { Worker } from '../index';

// These tests exercise the adapter-agnostic batch-remove entry resolution in
// Worker#mapBatchEntriesToMessages directly. The three shared queue adapters
// report batch-remove result entries in two different shapes:
//   - SQS: uppercase `Id` = stringified index into the messages array.
//   - Redis/Beanstalkd: lowercase `id` = the message's own job id.
// The resolver must map either shape back to the original message object so that
// success/failure accounting and individual retries keep working on all
// backends (worker#47).
describe('Worker batch-remove entry resolution', () => {
  afterEach(() => {
    delete process.env.QUEUE_TYPE;
  });

  // Private method accessed via cast, matching the repo's existing test style.
  const mapEntries = (worker: Worker, entries: any[], messages: any[]): any[] =>
    (worker as any).mapBatchEntriesToMessages(entries, messages);

  it('resolves Redis/Beanstalkd-shaped entries (lowercase id) back to the original messages', () => {
    const worker = new Worker({ logger: Logger });

    const messages = [
      { id: 'job-1', event: 'playing' },
      { id: 'job-2', event: 'paused' },
      { id: 'job-3', event: 'seeking' },
    ];

    // Redis/Beanstalkd report entries keyed by the message's own `id`, out of
    // order relative to the input array to prove we match by identity, not index.
    const successful = [{ id: 'job-3' }, { id: 'job-1' }];
    const failed = [{ id: 'job-2', reason: 'not completed' }];

    const resolvedSuccessful = mapEntries(worker, successful, messages);
    const resolvedFailed = mapEntries(worker, failed, messages);

    expect(resolvedSuccessful.length).toBe(2);
    expect(resolvedSuccessful).toContain(messages[2]);
    expect(resolvedSuccessful).toContain(messages[0]);

    expect(resolvedFailed.length).toBe(1);
    // Must resolve to the ORIGINAL message object (carrying `id`), not the entry.
    expect(resolvedFailed[0]).toBe(messages[1]);
    expect(resolvedFailed[0].id).toBe('job-2');
  });

  it('still resolves SQS-shaped entries (uppercase index-based Id) back to the original messages', () => {
    const worker = new Worker({ logger: Logger });

    const messages = [
      { MessageId: 'msg-0', ReceiptHandle: 'receipt-0' },
      { MessageId: 'msg-1', ReceiptHandle: 'receipt-1' },
    ];

    // SQS DeleteMessageBatch entries: `Id` is the stringified batch index and a
    // failure entry carries no ReceiptHandle.
    const successful = [{ Id: '0', ReceiptHandle: 'receipt-0' }];
    const failed = [{ Id: '1', Code: 'InternalError', Message: 'transient' }];

    const resolvedSuccessful = mapEntries(worker, successful, messages);
    const resolvedFailed = mapEntries(worker, failed, messages);

    expect(resolvedSuccessful.length).toBe(1);
    expect(resolvedSuccessful[0]).toBe(messages[0]);

    expect(resolvedFailed.length).toBe(1);
    // The failed entry (index '1') must map back to the original message so the
    // individual retry has the ReceiptHandle it needs.
    expect(resolvedFailed[0]).toBe(messages[1]);
    expect(resolvedFailed[0].ReceiptHandle).toBe('receipt-1');
  });

  it('skips entries that cannot be resolved to any message', () => {
    const worker = new Worker({ logger: Logger });
    const messages = [{ id: 'job-1' }];

    // Unknown lowercase id and out-of-range uppercase index both drop out.
    const resolved = mapEntries(
      worker,
      [{ id: 'does-not-exist' }, { Id: '99' }],
      messages
    );

    expect(resolved.length).toBe(0);
  });

  it('returns an empty array for undefined or empty entry lists', () => {
    const worker = new Worker({ logger: Logger });
    const messages = [{ id: 'job-1' }];

    expect(mapEntries(worker, undefined as any, messages).length).toBe(0);
    expect(mapEntries(worker, [], messages).length).toBe(0);
  });
});
