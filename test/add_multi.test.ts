import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { strict as assert } from 'node:assert';
import { inspect } from 'node:util';
import { config } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';
import { queryRowCount } from './helpers/athena_helper';
import { createPartitionedParquetFile } from './helpers/parquet_helper';

import { getMetadata, addPartitionSpec, addDataFiles } from '../src';

void test('multi-file multi-partition test', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_multi',
    'test_table_multi',
    [
      { name: 'app_name', type: 'string', required: true },
      { name: 'event_datetime', type: 'timestamp', required: true },
      { name: 'detail', type: 'string', required: false },
    ]
  );

  await t.test('add partition spec', async () => {
    const result = await addPartitionSpec({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      specId: 1,
      fields: [
        {
          'field-id': 1000,
          name: 'app_name',
          'source-id': 1,
          transform: 'identity',
        },
        {
          'field-id': 1001,
          name: 'event_datetime_day',
          'source-id': 2,
          transform: 'day',
        },
      ],
    });
    log('Partition spec added:', result);
  });

  await t.test('add multiple files to multiple partitions', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    assert(tableBucket, 'Could not extract table bucket');

    // Create files for different partitions
    const files = await Promise.all([
      createPartitionedParquetFile(
        tableBucket,
        'app1',
        new Date('2024-01-01'),
        1
      ),
      createPartitionedParquetFile(
        tableBucket,
        'app1',
        new Date('2024-01-02'),
        2
      ),
      createPartitionedParquetFile(
        tableBucket,
        'app2',
        new Date('2024-01-01'),
        3
      ),
      createPartitionedParquetFile(
        tableBucket,
        'app2',
        new Date('2024-01-02'),
        4
      ),
    ]);

    const result = await addDataFiles({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      lists: [
        {
          specId: 1,
          schemaId: 0,
          files: [
            {
              file: `s3://${tableBucket}/${files[0].key}`,
              partitions: {
                app_name: 'app1',
                event_datetime_day: '2024-01-01',
              },
              recordCount: 10n,
              fileSize: BigInt(files[0].size),
            },
            {
              file: `s3://${tableBucket}/${files[1].key}`,
              partitions: {
                app_name: 'app1',
                event_datetime_day: '2024-01-02',
              },
              recordCount: 10n,
              fileSize: BigInt(files[1].size),
            },
            {
              file: `s3://${tableBucket}/${files[2].key}`,
              partitions: {
                app_name: 'app2',
                event_datetime_day: '2024-01-01',
              },
              recordCount: 10n,
              fileSize: BigInt(files[2].size),
            },
            {
              file: `s3://${tableBucket}/${files[3].key}`,
              partitions: {
                app_name: 'app2',
                event_datetime_day: '2024-01-02',
              },
              recordCount: 10n,
              fileSize: BigInt(files[3].size),
            },
          ],
        },
      ],
    });
    log('addDataFiles result for all partitions:', result);
  });

  await t.test('verify total row count', async () => {
    const rowCount = await queryRowCount(namespace, name);
    log('Total row count:', rowCount);
    assert.strictEqual(rowCount, 40, `Expected 40 total rows, got ${rowCount}`);
  });

  await t.test('verify app1 partition', async () => {
    const rowCount = await queryRowCount(namespace, name, "app_name = 'app1'");
    log('App1 row count:', rowCount);
    assert.strictEqual(
      rowCount,
      20,
      `Expected 20 rows for app1, got ${rowCount}`
    );
  });

  await t.test('verify app2 partition', async () => {
    const rowCount = await queryRowCount(namespace, name, "app_name = 'app2'");
    log('App2 row count:', rowCount);
    assert.strictEqual(
      rowCount,
      20,
      `Expected 20 rows for app2, got ${rowCount}`
    );
  });

  await t.test('verify 2024-01-01 partition', async () => {
    const rowCount = await queryRowCount(
      namespace,
      name,
      "date(event_datetime) = date('2024-01-01')"
    );
    log('2024-01-01 row count:', rowCount);
    assert.strictEqual(
      rowCount,
      20,
      `Expected 20 rows for 2024-01-01, got ${rowCount}`
    );
  });

  await t.test('verify 2024-01-02 partition', async () => {
    const rowCount = await queryRowCount(
      namespace,
      name,
      "date(event_datetime) = date('2024-01-02')"
    );
    log('2024-01-02 row count:', rowCount);
    assert.strictEqual(
      rowCount,
      20,
      `Expected 20 rows for 2024-01-02, got ${rowCount}`
    );
  });

  await t.test('verify specific partition combinations', async () => {
    const combinations = [
      {
        where:
          "app_name = 'app1' AND date(event_datetime) = date('2024-01-01')",
        expected: 10,
      },
      {
        where:
          "app_name = 'app1' AND date(event_datetime) = date('2024-01-02')",
        expected: 10,
      },
      {
        where:
          "app_name = 'app2' AND date(event_datetime) = date('2024-01-01')",
        expected: 10,
      },
      {
        where:
          "app_name = 'app2' AND date(event_datetime) = date('2024-01-02')",
        expected: 10,
      },
    ];

    for (const { where, expected } of combinations) {
      const rowCount = await queryRowCount(namespace, name, where);
      log(`Row count for ${where}:`, rowCount);
      assert.strictEqual(
        rowCount,
        expected,
        `Expected ${expected} rows for ${where}, got ${rowCount}`
      );
    }
  });

  await t.test('add multiple files in multiple lists', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    assert(tableBucket, 'Could not extract table bucket');

    // Create 4 more files for different partitions
    const files = await Promise.all([
      createPartitionedParquetFile(
        tableBucket,
        'app3',
        new Date('2024-01-01'),
        5
      ),
      createPartitionedParquetFile(
        tableBucket,
        'app3',
        new Date('2024-01-02'),
        6
      ),
      createPartitionedParquetFile(
        tableBucket,
        'app4',
        new Date('2024-01-01'),
        7
      ),
      createPartitionedParquetFile(
        tableBucket,
        'app4',
        new Date('2024-01-02'),
        8
      ),
    ]);

    const result = await addDataFiles({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      lists: [
        {
          specId: 1,
          schemaId: 0,
          files: [
            {
              file: `s3://${tableBucket}/${files[0].key}`,
              partitions: {
                app_name: 'app3',
                event_datetime_day: '2024-01-01',
              },
              recordCount: 10n,
              fileSize: BigInt(files[0].size),
            },
            {
              file: `s3://${tableBucket}/${files[1].key}`,
              partitions: {
                app_name: 'app3',
                event_datetime_day: '2024-01-02',
              },
              recordCount: 10n,
              fileSize: BigInt(files[1].size),
            },
          ],
        },
        {
          specId: 1,
          schemaId: 0,
          files: [
            {
              file: `s3://${tableBucket}/${files[2].key}`,
              partitions: {
                app_name: 'app4',
                event_datetime_day: '2024-01-01',
              },
              recordCount: 10n,
              fileSize: BigInt(files[2].size),
            },
            {
              file: `s3://${tableBucket}/${files[3].key}`,
              partitions: {
                app_name: 'app4',
                event_datetime_day: '2024-01-02',
              },
              recordCount: 10n,
              fileSize: BigInt(files[3].size),
            },
          ],
        },
      ],
    });
    log('addDataFiles result for multiple lists:', result);
  });

  await t.test('verify total row count after second batch', async () => {
    const rowCount = await queryRowCount(namespace, name);
    log('Total row count after second batch:', rowCount);
    assert.strictEqual(
      rowCount,
      80,
      `Expected 80 total rows after second batch, got ${rowCount}`
    );
  });

  await t.test('verify new app partitions', async () => {
    const app3Count = await queryRowCount(namespace, name, "app_name = 'app3'");
    const app4Count = await queryRowCount(namespace, name, "app_name = 'app4'");
    log('App3 row count:', app3Count);
    log('App4 row count:', app4Count);
    assert.strictEqual(
      app3Count,
      20,
      `Expected 20 rows for app3, got ${app3Count}`
    );
    assert.strictEqual(
      app4Count,
      20,
      `Expected 20 rows for app4, got ${app4Count}`
    );
  });

  await t.test('final metadata check', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    log('Final metadata:', inspect(metadata, { depth: 99 }));
  });
});
