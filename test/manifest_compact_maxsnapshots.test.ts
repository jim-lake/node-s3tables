import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { strict as assert } from 'node:assert';
import { config } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';
import { createPartitionedParquetFile } from './helpers/parquet_helper';

import { getMetadata, addPartitionSpec, addDataFiles } from '../src';
import { manifestCompact } from '../src/manifest_compact';

void test('manifest compact with maxSnapshots', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_compact_maxsnap',
    'test_table_compact_maxsnap',
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
      ],
    });
  });

  await t.test('add file 1', async () => {
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
              partitions: { app_name: 'app1' },
              recordCount: 10n,
              fileSize: BigInt(size),
            },
          ],
        },
      ],
    });
  });

  await t.test('add file 2', async () => {
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
              partitions: { app_name: 'app1' },
              recordCount: 10n,
              fileSize: BigInt(size),
            },
          ],
        },
      ],
    });
  });

  await t.test('compact with maxSnapshots removes oldest', async () => {
    const metadataBefore = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const snapshotCountBefore = metadataBefore.snapshots.length;
    log('Snapshots before compact:', snapshotCountBefore);

    const result = await manifestCompact({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      maxSnapshots: 2,
    });
    log('Compact result:', result);
    assert.strictEqual(result.changed, true);

    const metadataAfter = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    log('Snapshots after compact:', metadataAfter.snapshots.length);
    assert(metadataAfter.snapshots.length <= 2);
  });
});
