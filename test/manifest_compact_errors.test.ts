import { test } from './helpers/test_helper';
import { strict as assert } from 'node:assert';
import { config } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';
import { createPartitionedParquetFile } from './helpers/parquet_helper';

import { getMetadata, addPartitionSpec, addDataFiles } from '../src';
import { manifestCompact } from '../src/manifest_compact';

void test('manifest compact error cases', async (t) => {
  await t.test('bad tableBucketARN throws error', async () => {
    await assert.rejects(
      async () => {
        await manifestCompact({
          tableBucketARN: 'invalid-arn',
          namespace: 'test',
          name: 'test',
        });
      },
      { message: 'bad tableBucketARN' }
    );
  });

  await t.test('non-existent table throws error', async () => {
    await assert.rejects(async () => {
      await manifestCompact({
        tableBucketARN: config.tableBucketARN,
        namespace: 'nonexistent_namespace',
        name: 'nonexistent_table',
      });
    });
  });

  await t.test(
    'impossible compaction with incompatible partition specs',
    async () => {
      const { namespace, name } = await setupTable(
        t,
        'test_ns_compact_err',
        'test_table_compact_err',
        [
          { name: 'app', type: 'string', required: true },
          { name: 'date', type: 'date', required: true },
          { name: 'value', type: 'string', required: false },
        ]
      );

      await addPartitionSpec({
        tableBucketARN: config.tableBucketARN,
        namespace,
        name,
        specId: 1,
        fields: [
          {
            'field-id': 1000,
            name: 'app',
            'source-id': 1,
            transform: 'identity',
          },
        ],
      });

      const metadata = await getMetadata({
        tableBucketARN: config.tableBucketARN,
        namespace,
        name,
      });
      const tableBucket = metadata.location.split('/').slice(-1)[0];
      assert(tableBucket);

      const date = new Date('2024-01-01');
      const { key: key1, size: size1 } = await createPartitionedParquetFile(
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
                file: `s3://${tableBucket}/${key1}`,
                partitions: { app: 'app1' },
                recordCount: 10n,
                fileSize: BigInt(size1),
              },
            ],
          },
        ],
      });

      await addPartitionSpec({
        tableBucketARN: config.tableBucketARN,
        namespace,
        name,
        specId: 2,
        fields: [
          {
            'field-id': 1001,
            name: 'date_day',
            'source-id': 2,
            transform: 'day',
          },
        ],
      });

      const { key: key2, size: size2 } = await createPartitionedParquetFile(
        tableBucket,
        'app2',
        date,
        2
      );

      await addDataFiles({
        tableBucketARN: config.tableBucketARN,
        namespace,
        name,
        lists: [
          {
            specId: 2,
            schemaId: 0,
            files: [
              {
                file: `s3://${tableBucket}/${key2}`,
                partitions: { date_day: '2024-01-01' },
                recordCount: 10n,
                fileSize: BigInt(size2),
              },
            ],
          },
        ],
      });

      const result = await manifestCompact({
        tableBucketARN: config.tableBucketARN,
        namespace,
        name,
        targetCount: 1,
        calculateWeight: () => 1,
      });

      assert.strictEqual(result.changed, false);
      assert.strictEqual(result.outputManifestCount, 0);
    }
  );
});
