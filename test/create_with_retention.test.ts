import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { strict as assert } from 'node:assert';
import { config } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';

import { getMetadata } from '../src';
import type { IcebergMetadata } from '../src/iceberg';

/*
 * IcebergMetadata (src/iceberg.ts) does not model table properties, but the S3
 * Tables REST endpoint returns them. This narrows the read-back of the
 * `properties` map without widening the shared type.
 */
interface MetadataWithProperties extends IcebergMetadata {
  properties?: Record<string, string>;
}

const ONE_HOUR_MS = String(60 * 60 * 1000);

/*
 * Verify snapshot-retention properties can be set at table-create time through
 * the API. setupTable passes these into the S3 Tables CreateTable metadata
 * (metadata.iceberg.properties), so the table is created with them in one call
 * -- no separate set-properties update. We require that a 1 hour
 * max-snapshot-age and a 1-snapshot minimum round-trip back from getMetadata.
 */
void test('create table with retention properties test', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_retention',
    'test_table_retention',
    [{ name: 'app', type: 'string', required: true }],
    {
      'history.expire.max-snapshot-age-ms': ONE_HOUR_MS,
      'history.expire.min-snapshots-to-keep': '1',
    }
  );

  await t.test('retention properties round-trip from metadata', async () => {
    const metadata = (await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    })) as MetadataWithProperties;
    log('table properties:', metadata.properties);

    assert(metadata.properties, 'metadata should carry properties');
    assert.strictEqual(
      metadata.properties['history.expire.max-snapshot-age-ms'],
      ONE_HOUR_MS,
      'max-snapshot-age-ms should be 1 hour'
    );
    assert.strictEqual(
      metadata.properties['history.expire.min-snapshots-to-keep'],
      '1',
      'min-snapshots-to-keep should be 1'
    );
  });
});
