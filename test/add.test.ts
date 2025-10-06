import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { strict as assert } from 'node:assert';
import { inspect } from 'node:util';
import { config } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';
import { queryRowCount } from './helpers/athena_helper';
import { createSimpleParquetFile } from './helpers/parquet_helper';

import { getMetadata, addDataFiles } from '../src';

void test('add multiple parquet files test', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_add',
    'test_table_add',
    [
      { name: 'app', type: 'string', required: true },
      { name: 'event_datetime', type: 'timestamp', required: true },
    ]
  );

  await t.test('add first parquet file (10 rows)', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    assert(tableBucket, 'Could not extract table bucket');

    const { key, size } = await createSimpleParquetFile(tableBucket, 1);

    const result = await addDataFiles({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      lists: [
        {
          specId: 0,
          schemaId: 0,
          files: [
            {
              file: `s3://${tableBucket}/${key}`,
              partitions: { app: 'test-app' },
              recordCount: 10n,
              fileSize: BigInt(size),
            },
          ],
        },
      ],
    });
    log('addDataFiles result 1:', result);

    const rowCount = await queryRowCount(namespace, name);
    log('Row count after first file:', rowCount);
    assert.strictEqual(
      rowCount,
      10,
      `Expected 10 rows after first file, got ${rowCount}`
    );
  });

  await t.test('add second parquet file (10 rows)', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    assert(tableBucket, 'Could not extract table bucket');

    const { key, size } = await createSimpleParquetFile(tableBucket, 2);

    const result = await addDataFiles({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      lists: [
        {
          specId: 0,
          schemaId: 0,
          files: [
            {
              file: `s3://${tableBucket}/${key}`,
              partitions: { app: 'test-app' },
              recordCount: 10n,
              fileSize: BigInt(size),
            },
          ],
        },
      ],
    });
    log('addDataFiles result 2:', result);

    const rowCount = await queryRowCount(namespace, name);
    log('Row count after second file:', rowCount);
    assert.strictEqual(
      rowCount,
      20,
      `Expected 20 rows after second file, got ${rowCount}`
    );
  });

  await t.test('add third parquet file (10 rows)', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    assert(tableBucket, 'Could not extract table bucket');

    const { key, size } = await createSimpleParquetFile(tableBucket, 3);

    const result = await addDataFiles({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      lists: [
        {
          specId: 0,
          schemaId: 0,
          files: [
            {
              file: `s3://${tableBucket}/${key}`,
              partitions: { app: 'test-app' },
              recordCount: 10n,
              fileSize: BigInt(size),
            },
          ],
        },
      ],
    });
    log('addDataFiles result 3:', result);

    const rowCount = await queryRowCount(namespace, name);
    log('Row count after third file:', rowCount);
    assert.strictEqual(
      rowCount,
      30,
      `Expected 30 rows after third file, got ${rowCount}`
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
