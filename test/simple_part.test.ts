import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { strict as assert } from 'node:assert';
import { inspect } from 'node:util';
import { config } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';
import { queryRowCount } from './helpers/athena_helper';
import { createPartitionedParquetFile } from './helpers/parquet_helper';

import { getMetadata, addPartitionSpec, addDataFiles } from '../src';

void test('multi-level partitioning test', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_part',
    'test_table_part',
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

  await t.test('add files to app1/2024-01-01 partition', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    assert(tableBucket, 'Could not extract table bucket');

    const date = new Date('2024-01-01');
    const { key, size } = await createPartitionedParquetFile(
      tableBucket,
      'app1',
      date,
      1
    );

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
              file: `s3://${tableBucket}/${key}`,
              partitions: {
                app_name: 'app1',
                event_datetime_day: '2024-01-01',
              },
              recordCount: 10n,
              fileSize: BigInt(size),
            },
          ],
        },
      ],
    });
    log('addDataFiles result app1/2024-01-01:', result);
  });

  await t.test('add files to app1/2024-01-02 partition', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    assert(tableBucket, 'tableBucket is undefined');

    const date = new Date('2024-01-02');
    const { key, size } = await createPartitionedParquetFile(
      tableBucket,
      'app1',
      date,
      2
    );

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
              file: `s3://${tableBucket}/${key}`,
              partitions: {
                app_name: 'app1',
                event_datetime_day: '2024-01-02',
              },
              recordCount: 10n,
              fileSize: BigInt(size),
            },
          ],
        },
      ],
    });
    log('addDataFiles result app1/2024-01-02:', result);
  });

  await t.test('add files to app2/2024-01-01 partition', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    assert(tableBucket, 'tableBucket is undefined');

    const date = new Date('2024-01-01');
    const { key, size } = await createPartitionedParquetFile(
      tableBucket,
      'app2',
      date,
      3
    );

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
              file: `s3://${tableBucket}/${key}`,
              partitions: {
                app_name: 'app2',
                event_datetime_day: '2024-01-01',
              },
              recordCount: 10n,
              fileSize: BigInt(size),
            },
          ],
        },
      ],
    });
    log('addDataFiles result app2/2024-01-01:', result);
  });

  await t.test('add files to app2/2024-01-02 partition', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    assert(tableBucket, 'tableBucket is undefined');

    const date = new Date('2024-01-02');
    const { key, size } = await createPartitionedParquetFile(
      tableBucket,
      'app2',
      date,
      4
    );

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
              file: `s3://${tableBucket}/${key}`,
              partitions: {
                app_name: 'app2',
                event_datetime_day: '2024-01-02',
              },
              recordCount: 10n,
              fileSize: BigInt(size),
            },
          ],
        },
      ],
    });
    log('addDataFiles result app2/2024-01-02:', result);
  });

  await t.test('query total row count', async () => {
    const rowCount = await queryRowCount(namespace, name);
    log('Total row count:', rowCount);
    assert.strictEqual(rowCount, 40, `Expected 40 total rows, got ${rowCount}`);
  });

  await t.test('query app1 partition', async () => {
    const rowCount = await queryRowCount(namespace, name, "app_name = 'app1'");
    log('App1 row count:', rowCount);
    assert.strictEqual(
      rowCount,
      20,
      `Expected 20 rows for app1, got ${rowCount}`
    );
  });

  await t.test('query 2024-01-01 partition', async () => {
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

  await t.test('query specific partition', async () => {
    const rowCount = await queryRowCount(
      namespace,
      name,
      "app_name = 'app1' AND date(event_datetime) = date('2024-01-01')"
    );
    log('App1 2024-01-01 row count:', rowCount);
    assert.strictEqual(
      rowCount,
      10,
      `Expected 10 rows for app1/2024-01-01, got ${rowCount}`
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
