import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { strict as assert } from 'node:assert';
import { config, clients } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';
import { queryRows } from './helpers/athena_helper';
import { getMetadata, addSchema, addPartitionSpec, addDataFiles } from '../src';
import { PutObjectCommand } from '@aws-sdk/client-s3';
import { readFileSync, statSync } from 'node:fs';

void test('redshift export compatibility test', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_redshift',
    'test_table_redshift',
    [{ name: 'placeholder', type: 'string', required: false }]
  );

  await t.test('add schema with all redshift export columns', async () => {
    await addSchema({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      schemaId: 1,
      fields: [
        { id: 1, name: 'app', type: 'string', required: false },
        { id: 2, name: 'app_ver', type: 'string', required: false },
        { id: 3, name: 'server_ver', type: 'string', required: false },
        { id: 4, name: 'config_ver', type: 'string', required: false },
        { id: 5, name: 'ingest_datetime', type: 'timestamp', required: false },
        { id: 6, name: 'event_datetime', type: 'timestamp', required: false },
        { id: 7, name: 'device_tag', type: 'string', required: false },
        { id: 8, name: 'device_type', type: 'string', required: false },
        { id: 9, name: 'device_family', type: 'string', required: false },
        { id: 10, name: 'user_map_tag', type: 'string', required: false },
        { id: 11, name: 'os', type: 'string', required: false },
        { id: 12, name: 'os_ver', type: 'string', required: false },
        { id: 13, name: 'browser', type: 'string', required: false },
        { id: 14, name: 'browser_ver', type: 'string', required: false },
        { id: 15, name: 'marketplace', type: 'string', required: false },
        { id: 16, name: 'country', type: 'string', required: false },
        { id: 17, name: 'language', type: 'string', required: false },
        { id: 18, name: 'online_status', type: 'string', required: false },
        { id: 19, name: 'group_tag', type: 'string', required: false },
        { id: 20, name: 'kingdom', type: 'string', required: false },
        { id: 21, name: 'phylum', type: 'string', required: false },
        { id: 22, name: 'class', type: 'string', required: false },
        { id: 23, name: 'order', type: 'string', required: false },
        { id: 24, name: 'family', type: 'string', required: false },
        { id: 25, name: 'genus', type: 'string', required: false },
        { id: 26, name: 'species', type: 'string', required: false },
        { id: 27, name: 'float1', type: 'double', required: false },
        { id: 28, name: 'float2', type: 'double', required: false },
        { id: 29, name: 'float3', type: 'double', required: false },
        { id: 30, name: 'float4', type: 'double', required: false },
        { id: 31, name: 'event_index', type: 'int', required: false },
      ],
    });
  });

  await t.test('add partition spec', async () => {
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
        {
          'field-id': 1001,
          name: 'event_datetime_day',
          'source-id': 6,
          transform: 'day',
        },
      ],
    });
  });

  await t.test('upload and add both redshift export files', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    assert(tableBucket, 'Could not extract table bucket');

    const files = [
      'test/files/redshift_export_0001.parquet',
      'test/files/redshift_export_0002.parquet',
    ];

    const uploadedFiles = [];
    for (const localPath of files) {
      const fileBuffer = readFileSync(localPath);
      const fileSize = statSync(localPath).size;
      const fileName = localPath.split('/').pop();
      assert(fileName, 'Could not extract file name');
      const s3Key = `data/app=myapp/event_datetime_day=2026-01-01/${fileName}`;

      await clients.s3.send(
        new PutObjectCommand({
          Bucket: tableBucket,
          Key: s3Key,
          Body: fileBuffer,
        })
      );

      uploadedFiles.push({
        file: `s3://${tableBucket}/${s3Key}`,
        partitions: { app: 'myapp', event_datetime_day: '2026-01-01' },
        recordCount: 2627n,
        fileSize: BigInt(fileSize),
      });
    }

    await addDataFiles({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      lists: [{ specId: 1, schemaId: 1, files: uploadedFiles }],
    });

    const rows = await queryRows(namespace, name);
    log('Total row count:', rows.length);
    assert.strictEqual(
      rows.length,
      5254,
      `Expected 5254 rows, got ${rows.length}`
    );

    const firstRow = rows[0];
    assert(firstRow, 'No rows returned');
    log('First row:', firstRow);
    assert.strictEqual(firstRow['device_tag'], '550506bf60998d44014753ab');
    assert.strictEqual(firstRow['server_ver'], '1.2');
    assert.strictEqual(firstRow['country'], 'US');
    assert.strictEqual(firstRow['kingdom'], 'single_trophy_league');
    assert.strictEqual(firstRow['phylum'], 'defender_loss');
    assert.strictEqual(
      firstRow['event_datetime'],
      '2026-01-01 00:05:28.658000'
    );
    assert.strictEqual(
      firstRow['ingest_datetime'],
      '2026-01-01 00:05:29.202000'
    );

    const rowsAfter2026 = await queryRows(
      namespace,
      name,
      "event_datetime > TIMESTAMP '2026-01-01 00:00:00'"
    );
    log('Rows after 2026-01-01:', rowsAfter2026.length);
    assert.strictEqual(
      rowsAfter2026.length,
      5254,
      `Expected 5254 rows after 2026-01-01, got ${rowsAfter2026.length}`
    );

    const rowsBefore2026 = await queryRows(
      namespace,
      name,
      "event_datetime < TIMESTAMP '2026-01-01 00:00:00'"
    );
    log('Rows before 2026-01-01:', rowsBefore2026.length);
    assert.strictEqual(
      rowsBefore2026.length,
      0,
      `Expected 0 rows before 2026-01-01, got ${rowsBefore2026.length}`
    );
  });
});
