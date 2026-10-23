import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { strict as assert } from 'node:assert';
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { PutObjectCommand } from '@aws-sdk/client-s3';
import { config, clients } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';
import { queryRows } from './helpers/athena_helper';

import { getMetadata, addDataFiles } from '../src';

/*
 * Format test for rds_export1.gz.parquet, a 4-row fixture exported from RDS (a
 * gzip-page compressed parquet file) with 14 columns. The Iceberg schema below
 * mirrors those columns one-to-one. Every column is optional (required: false):
 * the export can contain nulls (the `example_disabled` row has null last_*
 * timestamps) and Iceberg forbids adding required columns after create anyway.
 *
 * This is the ONLY test that uses this fixture, and it does one thing: prove the
 * exported parquet format imports as-is and is queryable from Athena. The
 * original bytes are uploaded and registered with addDataFiles -- no rewrite.
 */
const RDS_EXPORT_PATH = join(
  import.meta.dirname,
  'files',
  'rds_export1.gz.parquet'
);
const RDS_EXPORT_ROWS = 4;

const RDS_SCHEMA = [
  { name: 'job_name', type: 'string', required: false },
  { name: 'create_time', type: 'timestamp', required: false },
  { name: 'is_disabled', type: 'int', required: false },
  { name: 'frequency_secs', type: 'int', required: false },
  { name: 'retry_secs', type: 'int', required: false },
  { name: 'max_run_secs', type: 'int', required: false },
  { name: 'status', type: 'string', required: false },
  { name: 'run_count', type: 'int', required: false },
  { name: 'last_interval_time', type: 'timestamp', required: false },
  { name: 'last_start_time', type: 'timestamp', required: false },
  { name: 'last_result_time', type: 'timestamp', required: false },
  { name: 'last_success_time', type: 'timestamp', required: false },
  { name: 'last_start_worker_id', type: 'string', required: false },
  { name: 'last_result', type: 'string', required: false },
];

void test('rds parquet format import test', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_rds_import',
    'test_table_rds_import',
    RDS_SCHEMA
  );

  await t.test('import rds parquet and query via athena', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    assert(
      tableBucket,
      'Could not extract table bucket from metadata location'
    );

    /* Upload the original fixture bytes as-is (no rewrite). */
    const buffer = await readFile(RDS_EXPORT_PATH);
    const key = `data/rds-import-${Date.now()}.parquet`;
    await clients.s3.send(
      new PutObjectCommand({ Bucket: tableBucket, Key: key, Body: buffer })
    );

    await addDataFiles({
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
              partitions: {},
              recordCount: BigInt(RDS_EXPORT_ROWS),
              fileSize: BigInt(buffer.length),
            },
          ],
        },
      ],
    });

    const rows = await queryRows(namespace, name);
    log('rows after rds import:', rows.length);
    assert.strictEqual(
      rows.length,
      RDS_EXPORT_ROWS,
      `expected ${RDS_EXPORT_ROWS} rows from the imported parquet, got ${rows.length}`
    );
    /*
     * Spot-check real column values came through (not just a row count): the
     * fixture contains this job_name with these values.
     */
    const byJob = new Map(rows.map((r) => [r['job_name'], r]));
    log('imported job_name values:', [...byJob.keys()]);
    assert(
      byJob.has('example_simple1'),
      'expected job_name example_simple1 from the fixture'
    );
    assert.strictEqual(
      byJob.get('example_simple1')?.['status'],
      'RUNNING',
      'example_simple1 should have status RUNNING'
    );
    assert.strictEqual(
      byJob.get('example_simple1')?.['run_count'],
      '35',
      'example_simple1 should have run_count 35'
    );
  });
});
