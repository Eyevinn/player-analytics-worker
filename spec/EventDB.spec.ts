import Logger from '../logging/logger';
import EventDB from '../lib/EventDB';

Logger.silent = true;

describe('EventDB', () => {
  let db: EventDB;

  beforeEach(() => {
    db = new EventDB(Logger, 'test-db');
  });

  afterEach(() => {
    delete process.env.DB_TYPE;
    delete process.env.CLICKHOUSE_URL;
  });

  describe('getDBAdapter()', () => {
    it('should throw when DB_TYPE is not set', async () => {
      delete process.env.DB_TYPE;
      await expectAsync(db.TableExists('test_table'))
        .toBeRejectedWithError('No database type specified');
    });

    it('should throw for unsupported DB_TYPE', async () => {
      process.env.DB_TYPE = 'INVALID';
      await expectAsync(db.TableExists('test_table'))
        .toBeRejectedWithError('No database type specified');
    });
  });

  describe('TableExists()', () => {
    it('should cache table names after first successful check', async () => {
      process.env.DB_TYPE = 'DYNAMODB';
      process.env.AWS_REGION = 'us-east-1';

      const mockAdapter = {
        tableExists: jasmine.createSpy('tableExists').and.returnValue(Promise.resolve(true)),
        putItem: jasmine.createSpy('putItem'),
        putItems: jasmine.createSpy('putItems'),
        getItem: jasmine.createSpy('getItem'),
        deleteItem: jasmine.createSpy('deleteItem'),
        getItemsBySession: jasmine.createSpy('getItemsBySession'),
        handleError: jasmine.createSpy('handleError'),
      };

      // Inject mock adapter
      db.DBAdapter = mockAdapter as any;

      const exists1 = await db.TableExists('epas_test');
      expect(exists1).toBe(true);
      expect(mockAdapter.tableExists).toHaveBeenCalledTimes(1);

      // Second call should use cache — no additional adapter call
      const exists2 = await db.TableExists('epas_test');
      expect(exists2).toBe(true);
      expect(mockAdapter.tableExists).toHaveBeenCalledTimes(1);
    });

    it('should return false when table does not exist', async () => {
      const mockAdapter = {
        tableExists: jasmine.createSpy('tableExists').and.returnValue(Promise.resolve(false)),
        putItem: jasmine.createSpy('putItem'),
        putItems: jasmine.createSpy('putItems'),
        getItem: jasmine.createSpy('getItem'),
        deleteItem: jasmine.createSpy('deleteItem'),
        getItemsBySession: jasmine.createSpy('getItemsBySession'),
        handleError: jasmine.createSpy('handleError'),
      };

      db.DBAdapter = mockAdapter as any;

      const exists = await db.TableExists('nonexistent_table');
      expect(exists).toBe(false);
    });

    it('should throw when adapter throws', async () => {
      const mockAdapter = {
        tableExists: jasmine.createSpy('tableExists').and.returnValue(
          Promise.reject(new Error('Connection refused'))
        ),
        putItem: jasmine.createSpy('putItem'),
        putItems: jasmine.createSpy('putItems'),
        getItem: jasmine.createSpy('getItem'),
        deleteItem: jasmine.createSpy('deleteItem'),
        getItemsBySession: jasmine.createSpy('getItemsBySession'),
        handleError: jasmine.createSpy('handleError'),
      };

      db.DBAdapter = mockAdapter as any;

      await expectAsync(db.TableExists('epas_test')).toBeRejected();
    });
  });

  describe('write()', () => {
    it('should call putItem with correct params', async () => {
      const mockAdapter = {
        tableExists: jasmine.createSpy('tableExists'),
        putItem: jasmine.createSpy('putItem').and.returnValue(Promise.resolve(true)),
        putItems: jasmine.createSpy('putItems'),
        getItem: jasmine.createSpy('getItem'),
        deleteItem: jasmine.createSpy('deleteItem'),
        getItemsBySession: jasmine.createSpy('getItemsBySession'),
        handleError: jasmine.createSpy('handleError'),
      };

      db.DBAdapter = mockAdapter as any;

      const event = { event: 'playing', sessionId: 'sess-1', timestamp: 1000 };
      await db.write(event, 'epas_test');

      expect(mockAdapter.putItem).toHaveBeenCalledWith({
        tableName: 'epas_test',
        data: event,
      });
    });

    it('should throw when putItem fails', async () => {
      const mockAdapter = {
        tableExists: jasmine.createSpy('tableExists'),
        putItem: jasmine.createSpy('putItem').and.returnValue(
          Promise.reject(new Error('Write failed'))
        ),
        putItems: jasmine.createSpy('putItems'),
        getItem: jasmine.createSpy('getItem'),
        deleteItem: jasmine.createSpy('deleteItem'),
        getItemsBySession: jasmine.createSpy('getItemsBySession'),
        handleError: jasmine.createSpy('handleError'),
      };

      db.DBAdapter = mockAdapter as any;

      await expectAsync(db.write({}, 'epas_test')).toBeRejectedWithError('Write failed');
    });
  });

  describe('writeMultiple()', () => {
    it('should call putItems with correct params', async () => {
      const mockAdapter = {
        tableExists: jasmine.createSpy('tableExists'),
        putItem: jasmine.createSpy('putItem'),
        putItems: jasmine.createSpy('putItems').and.returnValue(Promise.resolve(true)),
        getItem: jasmine.createSpy('getItem'),
        deleteItem: jasmine.createSpy('deleteItem'),
        getItemsBySession: jasmine.createSpy('getItemsBySession'),
        handleError: jasmine.createSpy('handleError'),
      };

      db.DBAdapter = mockAdapter as any;

      const events = [
        { event: 'playing', sessionId: 'sess-1' },
        { event: 'heartbeat', sessionId: 'sess-1' },
      ];
      await db.writeMultiple(events, 'epas_test');

      expect(mockAdapter.putItems).toHaveBeenCalledWith({
        tableName: 'epas_test',
        data: events,
      });
    });
  });

  describe('batchWriteByTable()', () => {
    it('should write to multiple tables in parallel', async () => {
      const mockAdapter = {
        tableExists: jasmine.createSpy('tableExists').and.returnValue(Promise.resolve(true)),
        putItem: jasmine.createSpy('putItem'),
        putItems: jasmine.createSpy('putItems').and.returnValue(Promise.resolve(true)),
        getItem: jasmine.createSpy('getItem'),
        deleteItem: jasmine.createSpy('deleteItem'),
        getItemsBySession: jasmine.createSpy('getItemsBySession'),
        handleError: jasmine.createSpy('handleError'),
      };

      db.DBAdapter = mockAdapter as any;

      const eventsByTable = {
        'epas_site_a': [{ event: 'play' }, { event: 'pause' }],
        'epas_site_b': [{ event: 'seek' }],
      };

      const results = await db.batchWriteByTable(eventsByTable);

      expect(results.length).toBe(2);
      expect(results.every(r => r.success)).toBe(true);
      expect(mockAdapter.putItems).toHaveBeenCalledTimes(2);
    });

    it('should report failures per table without blocking others', async () => {
      const mockAdapter = {
        tableExists: jasmine.createSpy('tableExists').and.returnValue(Promise.resolve(true)),
        putItem: jasmine.createSpy('putItem'),
        putItems: jasmine.createSpy('putItems').and.callFake((params: { tableName: string }) => {
          if (params.tableName === 'epas_failing') {
            return Promise.reject(new Error('Write timeout'));
          }
          return Promise.resolve(true);
        }),
        getItem: jasmine.createSpy('getItem'),
        deleteItem: jasmine.createSpy('deleteItem'),
        getItemsBySession: jasmine.createSpy('getItemsBySession'),
        handleError: jasmine.createSpy('handleError'),
      };

      db.DBAdapter = mockAdapter as any;

      const eventsByTable = {
        'epas_good': [{ event: 'play' }],
        'epas_failing': [{ event: 'pause' }],
      };

      const results = await db.batchWriteByTable(eventsByTable);

      expect(results.length).toBe(2);
      const good = results.find(r => r.tableName === 'epas_good');
      const bad = results.find(r => r.tableName === 'epas_failing');
      expect(good!.success).toBe(true);
      expect(bad!.success).toBe(false);
    });

    it('should skip empty event arrays', async () => {
      const mockAdapter = {
        tableExists: jasmine.createSpy('tableExists').and.returnValue(Promise.resolve(true)),
        putItem: jasmine.createSpy('putItem'),
        putItems: jasmine.createSpy('putItems').and.returnValue(Promise.resolve(true)),
        getItem: jasmine.createSpy('getItem'),
        deleteItem: jasmine.createSpy('deleteItem'),
        getItemsBySession: jasmine.createSpy('getItemsBySession'),
        handleError: jasmine.createSpy('handleError'),
      };

      db.DBAdapter = mockAdapter as any;

      const eventsByTable = {
        'epas_active': [{ event: 'play' }],
        'epas_empty': [],
      };

      const results = await db.batchWriteByTable(eventsByTable);

      expect(results.length).toBe(1);
      expect(mockAdapter.putItems).toHaveBeenCalledTimes(1);
    });
  });
});
