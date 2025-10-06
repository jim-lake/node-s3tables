import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { strict as assert } from 'node:assert';
import { config } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';
import { queryRowCount } from './helpers/athena_helper';
import { createPartitionedParquetFile } from './helpers/parquet_helper';

import { getMetadata, addPartitionSpec, addDataFiles } from '../src';

void test('bad partition labeling test', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_bad_part',
    'test_table_bad_part',
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

  await t.test('add files with WRONG partition labels', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    assert(tableBucket, 'Could not extract table bucket');

    // Create file with app1/2024-01-01 data but label it as app2/2024-01-02
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
                app_name: 'app2', // WRONG: file contains app1 data
                event_datetime_day: '2024-01-02', // WRONG: file contains 2024-01-01 data
              },
              recordCount: 10n,
              fileSize: BigInt(size),
            },
          ],
        },
      ],
    });
    log('addDataFiles result with wrong partitions:', result);
  });

  await t.test('query for app1 - should find 0 rows', async () => {
    const rowCount = await queryRowCount(namespace, name, "app_name = 'app1'");
    log('App1 row count (should be 0):', rowCount);
    assert.strictEqual(
      rowCount,
      0,
      `Expected 0 rows for app1 due to wrong labeling, got ${rowCount}`
    );
  });

  await t.test(
    'query for app2 - should find 10 rows but wrong data',
    async () => {
      const rowCount = await queryRowCount(
        namespace,
        name,
        "app_name = 'app2'"
      );
      log('App2 row count (should be 10 but wrong data):', rowCount);
      assert.strictEqual(
        rowCount,
        10,
        `Expected 10 rows for app2 partition, got ${rowCount}`
      );
    }
  );

  await t.test('query for 2024-01-01 - should find 0 rows', async () => {
    const rowCount = await queryRowCount(
      namespace,
      name,
      "date(event_datetime) = date('2024-01-01')"
    );
    log('2024-01-01 row count (should be 0):', rowCount);
    assert.strictEqual(
      rowCount,
      0,
      `Expected 0 rows for 2024-01-01 due to wrong labeling, got ${rowCount}`
    );
  });

  await t.test(
    'query for 2024-01-02 - should find 10 rows but wrong data',
    async () => {
      const rowCount = await queryRowCount(
        namespace,
        name,
        "date(event_datetime) = date('2024-01-02')"
      );
      log('2024-01-02 row count (should be 10 but wrong data):', rowCount);
      assert.strictEqual(
        rowCount,
        10,
        `Expected 10 rows for 2024-01-02 partition, got ${rowCount}`
      );
    }
  );
});
