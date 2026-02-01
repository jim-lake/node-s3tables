import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { strict as assert } from 'node:assert';
import { config, clients } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';
import { queryRows } from './helpers/athena_helper';
import {
  getMetadata,
  addSchema,
  addPartitionSpec,
  importRedshiftManifest,
  downloadAvro,
  parseS3Url,
} from '../src';
import { ManifestListSchema, makeManifestSchema } from '../src/avro_schema';
import { PutObjectCommand } from '@aws-sdk/client-s3';
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';
import type { ManifestListRecord, ManifestFileRecord } from '../src/avro_types';

interface ManifestEntry {
  url: string;
  meta: { content_length: number; record_count: number };
}

interface RedshiftManifest {
  entries: ManifestEntry[];
  schema: { elements: { name: string }[] };
  author: { name: string; version: string };
}

void test('redshift import manifest test', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_rs_import',
    'test_table_rs_import',
    [{ name: 'placeholder', type: 'string', required: false }]
  );

  const testFilesDir = 'test/files/redshift_unload';
  const manifestPath = join(testFilesDir, 'manifest');
  const manifestContent: RedshiftManifest = JSON.parse(
    readFileSync(manifestPath, 'utf-8')
  ) as RedshiftManifest;

  let schemaId: number;
  let specId: number;

  await t.test('add schema matching redshift manifest', async () => {
    const result = await addSchema({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      schemaId: 1,
      fields: [
        { id: 1, name: 'app', type: 'string', required: false },
        { id: 2, name: 'app_ver', type: 'string', required: false },
        { id: 3, name: 'ingest_datetime', type: 'timestamp', required: false },
        { id: 4, name: 'event_datetime', type: 'timestamp', required: false },
        { id: 5, name: 'hostname', type: 'string', required: false },
        { id: 6, name: 'filename', type: 'string', required: false },
        { id: 7, name: 'log_level', type: 'string', required: false },
        { id: 8, name: 'device_tag', type: 'string', required: false },
        { id: 9, name: 'user_tag', type: 'string', required: false },
        { id: 10, name: 'remote_address', type: 'string', required: false },
        { id: 11, name: 'response_bytes', type: 'int', required: false },
        { id: 12, name: 'response_ms', type: 'double', required: false },
        { id: 13, name: 'device_type', type: 'string', required: false },
        { id: 14, name: 'os', type: 'string', required: false },
        { id: 15, name: 'os_ver', type: 'string', required: false },
        { id: 16, name: 'browser', type: 'string', required: false },
        { id: 17, name: 'browser_ver', type: 'string', required: false },
        { id: 18, name: 'country', type: 'string', required: false },
        { id: 19, name: 'language', type: 'string', required: false },
        { id: 20, name: 'log_line', type: 'string', required: false },
      ],
    });
    schemaId = result.metadata['current-schema-id'];
    log('Schema ID:', schemaId);
  });

  await t.test('add partition spec', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const currentSchema = metadata.schemas.find(
      (s) => s['schema-id'] === schemaId
    );
    assert(currentSchema, 'Current schema not found');

    const appField = currentSchema.fields.find((f) => f.name === 'app');
    const ingestDatetimeField = currentSchema.fields.find(
      (f) => f.name === 'ingest_datetime'
    );
    assert(appField, 'app field not found');
    assert(ingestDatetimeField, 'ingest_datetime field not found');

    log('app field id:', appField.id);
    log('ingest_datetime field id:', ingestDatetimeField.id);

    const result = await addPartitionSpec({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      specId: 1,
      fields: [
        {
          'field-id': 1000,
          name: 'app',
          'source-id': appField.id,
          transform: 'identity',
        },
        {
          'field-id': 1001,
          name: 'ingest_date',
          'source-id': ingestDatetimeField.id,
          transform: 'day',
        },
      ],
    });
    specId = result.metadata['default-spec-id'];
    log('Spec ID:', specId);
  });

  await t.test('upload files and import redshift manifest', async () => {
    const s3Prefix = `test-unload-${Date.now()}`;

    async function uploadFile(localPath: string, s3Key: string) {
      const fileBuffer = readFileSync(localPath);
      await clients.s3.send(
        new PutObjectCommand({
          Bucket: config.outputBucket,
          Key: s3Key,
          Body: fileBuffer,
        })
      );
    }

    // Upload all json.zst files maintaining partition structure
    for (const appDir of ['app=test-app', 'app=test-app2']) {
      const appPath = join(testFilesDir, appDir);
      const stat = statSync(appPath);
      if (!stat.isDirectory()) {
        break;
      }

      const partitionDirs = readdirSync(appPath);
      for (const partDir of partitionDirs) {
        const partPath = join(appPath, partDir);
        if (statSync(partPath).isDirectory()) {
          const files = readdirSync(partPath).filter((f) =>
            f.endsWith('.json.zst')
          );
          for (const file of files) {
            const localPath = join(partPath, file);
            const s3Key = `${s3Prefix}/${appDir}/${partDir}/${file}`;
            await uploadFile(localPath, s3Key);
          }
        }
      }
    }

    // Create updated manifest with correct S3 URLs
    const updatedManifest: RedshiftManifest = {
      ...manifestContent,
      entries: manifestContent.entries.map((entry) => {
        const urlParts = entry.url.split('/');
        const partitions = urlParts.slice(-3).join('/');
        return {
          ...entry,
          url: `s3://${config.outputBucket}/${s3Prefix}/${partitions}`,
        };
      }),
    };

    // Upload manifest
    const manifestKey = `${s3Prefix}/manifest`;
    await clients.s3.send(
      new PutObjectCommand({
        Bucket: config.outputBucket,
        Key: manifestKey,
        Body: JSON.stringify(updatedManifest),
      })
    );

    const manifestUrl = `s3://${config.outputBucket}/${manifestKey}`;
    log('Manifest URL:', manifestUrl);

    // Import using the API with explicit schema and spec IDs
    const result = await importRedshiftManifest({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      redshiftManifestUrl: manifestUrl,
      schemaId,
      specId,
    });

    log('Import result:', result);
    assert(result.snapshotId, 'Expected snapshotId in result');
  });

  await t.test('validate stats in manifest', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });

    const snapshot = metadata.snapshots.find(
      (s) => s['snapshot-id'] === metadata['current-snapshot-id']
    );
    assert(snapshot, 'Current snapshot not found');

    const manifestListPath = snapshot['manifest-list'];
    log('Manifest list:', manifestListPath);

    const { bucket: manifestListBucket, key: manifestListKey } =
      parseS3Url(manifestListPath);

    const region = config.tableBucketARN.split(':')[3];
    assert(region, 'Region not found in tableBucketARN');

    // Read manifest list
    const manifestList = await downloadAvro<ManifestListRecord>({
      region,
      bucket: manifestListBucket,
      key: manifestListKey,
      avroSchema: ManifestListSchema,
    });

    assert(manifestList.length > 0, 'No manifest records found');
    const manifestRecord = manifestList[0];
    assert(manifestRecord, 'First manifest record is undefined');

    // Read first manifest file
    const manifestFilePath = manifestRecord.manifest_path;
    const { bucket: manifestBucket, key: manifestKey } =
      parseS3Url(manifestFilePath);

    const spec = metadata['partition-specs'].find(
      (s) => s['spec-id'] === manifestRecord.partition_spec_id
    );
    assert(spec, 'Partition spec not found');

    const manifestFileSchema = makeManifestSchema(spec, metadata.schemas);

    const manifestFiles = await downloadAvro<ManifestFileRecord>({
      region,
      bucket: manifestBucket,
      key: manifestKey,
      avroSchema: manifestFileSchema,
    });

    assert(manifestFiles.length > 0, 'No data files found in manifest');
    const manifestEntry = manifestFiles[0];
    assert(manifestEntry, 'First manifest entry is undefined');

    const dataFile = manifestEntry.data_file;
    assert(dataFile, 'data_file missing from manifest entry');

    // Validate stats exist
    log('Data file stats:', {
      file_size_in_bytes: dataFile.file_size_in_bytes,
      record_count: dataFile.record_count,
      column_sizes: dataFile.column_sizes,
      value_counts: dataFile.value_counts,
      null_value_counts: dataFile.null_value_counts,
      lower_bounds: dataFile.lower_bounds,
      upper_bounds: dataFile.upper_bounds,
    });

    assert(dataFile.file_size_in_bytes, 'file_size_in_bytes missing');
    assert(dataFile.record_count, 'record_count missing');
    assert(dataFile.column_sizes, 'column_sizes missing');
    assert(dataFile.value_counts, 'value_counts missing');
    assert(dataFile.null_value_counts, 'null_value_counts missing');
    assert(dataFile.lower_bounds, 'lower_bounds missing');
    assert(dataFile.upper_bounds, 'upper_bounds missing');

    // Validate column_sizes has entries
    const columnSizes = dataFile.column_sizes;
    const columnSizeKeys = Object.keys(columnSizes);
    assert(
      columnSizeKeys.length > 0,
      'column_sizes should have at least one entry'
    );
    log('Column sizes keys:', columnSizeKeys);
  });

  await t.test('validate data with athena', async () => {
    const expectedTotal = manifestContent.entries.reduce(
      (sum, e) => sum + e.meta.record_count,
      0
    );

    const rows = await queryRows(namespace, name);
    log('Total row count:', rows.length);
    assert.strictEqual(
      rows.length,
      expectedTotal,
      `Expected ${expectedTotal} rows`
    );

    // Validate partition filtering using source column
    const sept25Rows = await queryRows(
      namespace,
      name,
      "date(ingest_datetime) = date('2024-09-25')"
    );
    log('Sept 25 rows:', sept25Rows.length);
    assert.strictEqual(
      sept25Rows.length,
      135,
      'Expected 135 rows for 2024-09-25'
    );

    const dec20Rows = await queryRows(
      namespace,
      name,
      "date(ingest_datetime) = date('2024-12-20')"
    );
    log('Dec 20 rows:', dec20Rows.length);
    assert.strictEqual(dec20Rows.length, 26, 'Expected 26 rows for 2024-12-20');
  });
});
