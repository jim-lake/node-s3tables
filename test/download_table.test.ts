import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { strict as assert } from 'node:assert';
import { config } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';
import { createPartitionedParquetFile } from './helpers/parquet_helper';
import { access, readFile } from 'node:fs/promises';
import { join } from 'node:path';

import { getMetadata, addPartitionSpec, addDataFiles } from '../src';
import { downloadTable } from './download_table';

void test('download table test', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_download',
    'test_table_download',
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

  await t.test('add data files', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    assert(tableBucket, 'Could not extract table bucket');

    const files = await Promise.all([
      createPartitionedParquetFile(
        tableBucket,
        'app1',
        new Date('2024-01-01'),
        1
      ),
      createPartitionedParquetFile(
        tableBucket,
        'app2',
        new Date('2024-01-02'),
        2
      ),
    ]);

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
                app_name: 'app2',
                event_datetime_day: '2024-01-02',
              },
              recordCount: 10n,
              fileSize: BigInt(files[1].size),
            },
          ],
        },
      ],
    });
  });

  await t.test('add second commit', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    assert(tableBucket, 'Could not extract table bucket');

    const files = await Promise.all([
      createPartitionedParquetFile(
        tableBucket,
        'app3',
        new Date('2024-01-03'),
        3
      ),
    ]);

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
              file: `s3://${tableBucket}/${files[0].key}`,
              partitions: {
                app_name: 'app3',
                event_datetime_day: '2024-01-03',
              },
              recordCount: 10n,
              fileSize: BigInt(files[0].size),
            },
          ],
        },
      ],
    });
  });

  await t.test('download table', async () => {
    const outputDir = '/tmp/download-test';
    await downloadTable({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      outputDir,
    });

    /* Verify metadata.json exists */
    const metadataPath = join(outputDir, 'metadata.json');
    await access(metadataPath);
    const metadataContent = await readFile(metadataPath, 'utf-8');
    const metadata = JSON.parse(metadataContent) as {
      snapshots: { 'manifest-list': string }[];
    };
    log('Downloaded metadata snapshots:', metadata.snapshots.length);
    assert(metadata.snapshots, 'metadata should have snapshots');
    assert.strictEqual(metadata.snapshots.length, 2, 'should have 2 snapshots');

    /* Verify manifest list files exist */
    let manifestListCount = 0;
    for (const snapshot of metadata.snapshots) {
      const manifestListUrl = snapshot['manifest-list'];
      const manifestListKey = manifestListUrl.split('/').slice(3).join('/');
      const manifestListPath = join(outputDir, manifestListKey);
      await access(manifestListPath);
      manifestListCount++;
      log('Verified manifest list:', manifestListPath);
    }

    log(
      `All files downloaded successfully: ${manifestListCount} manifest lists`
    );
    assert.strictEqual(
      manifestListCount,
      2,
      'should have downloaded 2 manifest lists'
    );
  });
});
