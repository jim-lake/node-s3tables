import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { strict as assert } from 'node:assert';
import { config } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';
import { queryRows } from './helpers/athena_helper';
import { createPartitionedParquetFile } from './helpers/parquet_helper';

import { getMetadata, addPartitionSpec, addDataFiles } from '../src';
import { manifestCompact } from '../src/manifest_compact';

void test('manifest compact test', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_compact',
    'test_table_compact',
    [
      { name: 'app_name', type: 'string', required: true },
      { name: 'event_datetime', type: 'timestamp', required: true },
      { name: 'detail', type: 'string', required: false },
    ]
  );

  await t.test('add partition spec', async () => {
    await addPartitionSpec({
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
  });

  await t.test('add file 1 to partition', async () => {
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

    await addDataFiles({
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
  });

  await t.test('add file 2 to partition', async () => {
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
      2
    );

    await addDataFiles({
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
  });

  await t.test('add file 3 to partition', async () => {
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
      3
    );

    await addDataFiles({
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
  });

  await t.test('verify table contents before compact', async () => {
    const rows = await queryRows(namespace, name);
    log('Row count before compact:', rows.length);
    assert.strictEqual(rows.length, 30, `Expected 30 rows, got ${rows.length}`);
  });

  await t.test('run compact', async () => {
    const result = await manifestCompact({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    log('Compact result:', result);
    assert.strictEqual(
      result.outputManifestCount,
      1,
      `Expected outputManifestCount = 1, got ${result.outputManifestCount}`
    );
  });

  await t.test('verify queries work after compact', async () => {
    const rows = await queryRows(namespace, name);
    log('Row count after compact:', rows.length);
    assert.strictEqual(
      rows.length,
      30,
      `Expected 30 rows after compact, got ${rows.length}`
    );
  });
});
