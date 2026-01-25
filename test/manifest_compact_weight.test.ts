import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { strict as assert } from 'node:assert';
import { config } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';
import { createPartitionedParquetFile } from './helpers/parquet_helper';

import { getMetadata, addPartitionSpec, addDataFiles } from '../src';
import { manifestCompact } from '../src';
import type { ManifestListRecord } from '../src';

void test('manifest compact with targetCount and calculateWeight', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_compact_weight',
    'test_table_compact_weight',
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

  await t.test('add files to 4 different partitions', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    assert(tableBucket, 'Could not extract table bucket');

    for (let day = 1; day <= 4; day++) {
      const date = new Date(`2024-01-0${day}`);
      const files = await Promise.all([
        createPartitionedParquetFile(tableBucket, 'app1', date, day * 2 - 1),
        createPartitionedParquetFile(tableBucket, 'app1', date, day * 2),
      ]);

      await addDataFiles({
        tableBucketARN: config.tableBucketARN,
        namespace,
        name,
        lists: [
          {
            specId: 1,
            schemaId: 0,
            files: files.map(({ key, size }) => ({
              file: `s3://${tableBucket}/${key}`,
              partitions: {
                app_name: 'app1',
                event_datetime_day: `2024-01-0${day}`,
              },
              recordCount: 10n,
              fileSize: BigInt(size),
            })),
          },
        ],
      });
    }
  });

  await t.test('compact with targetCount limits output manifests', async () => {
    function calculateWeight(group: ManifestListRecord[]) {
      return group.reduce((sum, r) => sum + Number(r.added_rows_count), 0);
    }

    const result = await manifestCompact({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      targetCount: 2,
      calculateWeight,
    });
    log('Compact result:', result);
    assert.strictEqual(result.changed, true);
    assert.strictEqual(result.outputManifestCount, 2);
  });
});
