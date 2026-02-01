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
import { PutObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { ParquetReader } from 'parquetjs';
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

void test('redshift import with rewriteParquet test', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_rs_rewrite',
    'test_table_rs_rewrite',
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
        { id: 1, name: 'app', type: 'string', required: true },
        { id: 2, name: 'app_ver', type: 'string', required: false },
        { id: 3, name: 'ingest_datetime', type: 'timestamp', required: true },
        { id: 4, name: 'event_datetime', type: 'timestamp', required: true },
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
        { id: 20, name: 'log_line', type: 'string', required: true },
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

  await t.test('upload files and import with rewriteParquet=true', async () => {
    const s3Prefix = `test-rewrite-${Date.now()}`;

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
    const { bucket: manifestListBucket, key: manifestListKey } =
      parseS3Url(manifestListPath);

    const region = config.tableBucketARN.split(':')[3];
    assert(region, 'Region not found in tableBucketARN');

    const manifestList = await downloadAvro<ManifestListRecord>({
      region,
      bucket: manifestListBucket,
      key: manifestListKey,
      avroSchema: ManifestListSchema,
    });

    assert(manifestList.length > 0, 'No manifest records found');
    const manifestRecord = manifestList[0];
    assert(manifestRecord, 'First manifest record is undefined');

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

    assert(dataFile.file_size_in_bytes, 'file_size_in_bytes missing');
    assert(dataFile.record_count, 'record_count missing');
    assert(dataFile.column_sizes, 'column_sizes missing');
    assert(dataFile.value_counts, 'value_counts missing');
    assert(dataFile.null_value_counts, 'null_value_counts missing');
    assert(dataFile.lower_bounds, 'lower_bounds missing');
    assert(dataFile.upper_bounds, 'upper_bounds missing');

    const columnSizes = dataFile.column_sizes;
    assert(
      Object.keys(columnSizes).length > 0,
      'column_sizes should have at least one entry'
    );
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

  await t.test('validate ZSTD compression', async () => {
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
    const { bucket: manifestListBucket, key: manifestListKey } =
      parseS3Url(manifestListPath);

    const region = config.tableBucketARN.split(':')[3];
    assert(region, 'Region not found in tableBucketARN');

    const manifestList = await downloadAvro<ManifestListRecord>({
      region,
      bucket: manifestListBucket,
      key: manifestListKey,
      avroSchema: ManifestListSchema,
    });

    const manifestRecord = manifestList[0];
    assert(manifestRecord, 'First manifest record is undefined');

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

    const manifestEntry = manifestFiles[0];
    assert(manifestEntry, 'First manifest entry is undefined');

    const dataFilePath = manifestEntry.data_file.file_path;
    const { bucket: dataBucket, key: dataKey } = parseS3Url(dataFilePath);

    const getCmd = new GetObjectCommand({ Bucket: dataBucket, Key: dataKey });
    const { Body } = await clients.s3.send(getCmd);
    assert(Body, 'Data file body missing');

    const buffer = await Body.transformToByteArray();
    const reader = await ParquetReader.openBuffer(Buffer.from(buffer));

    interface ParquetMetadata {
      row_groups?: {
        columns: {
          meta_data?: { codec?: number; path_in_schema?: string[] };
        }[];
      }[];
    }

    const parquetMetadata = reader.metadata as unknown as ParquetMetadata;
    const rowGroups = parquetMetadata.row_groups ?? [];
    assert(rowGroups.length > 0, 'No row groups found');

    const ZSTD_CODEC = 6;
    for (const rg of rowGroups) {
      for (const column of rg.columns) {
        const codec = column.meta_data?.codec;
        const fieldName = column.meta_data?.path_in_schema?.[0] ?? 'unknown';
        assert.strictEqual(
          codec,
          ZSTD_CODEC,
          `Expected ZSTD compression (codec=6) for field ${fieldName}, got codec=${codec ?? 'undefined'}`
        );
      }
    }

    await reader.close();
    log('ZSTD compression validation passed');
  });
});
