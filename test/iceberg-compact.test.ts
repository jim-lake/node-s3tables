import { PassThrough } from 'node:stream';
import { PutObjectCommand } from '@aws-sdk/client-s3';
import { ParquetWriter, ParquetSchema } from 'parquetjs';
import { strict as assert } from 'node:assert';
import { randomUUID } from 'node:crypto';

import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { config, clients } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';

import { getMetadata, addPartitionSpec, addDataFiles } from '../src';

import { BATCHES } from './files/iceberg_error_batches';
const BATCH_LIMIT = 3;

void test('Iceberg Compact test', async (t) => {
  const { namespace, name } = await setupTable(t, 'iceberg', 'compact', [
    { id: 1, name: 'app', required: true, type: 'string' },
    { id: 2, name: 'app_ver', required: false, type: 'string' },
    { id: 3, name: 'ingest_datetime', required: true, type: 'timestamptz' },
    { id: 4, name: 'event_datetime', required: true, type: 'timestamptz' },
    { id: 5, name: 'hostname', required: false, type: 'string' },
    { id: 6, name: 'filename', required: false, type: 'string' },
    { id: 7, name: 'log_level', required: false, type: 'string' },
    { id: 8, name: 'device_tag', required: false, type: 'string' },
    { id: 9, name: 'user_tag', required: false, type: 'string' },
    { id: 10, name: 'remote_address', required: false, type: 'string' },
    { id: 11, name: 'response_bytes', required: false, type: 'int' },
    { id: 12, name: 'response_ms', required: false, type: 'double' },
    { id: 13, name: 'device_type', required: false, type: 'string' },
    { id: 14, name: 'os', required: false, type: 'string' },
    { id: 15, name: 'os_ver', required: false, type: 'string' },
    { id: 16, name: 'browser', required: false, type: 'string' },
    { id: 17, name: 'browser_ver', required: false, type: 'string' },
    { id: 18, name: 'country', required: false, type: 'string' },
    { id: 19, name: 'language', required: false, type: 'string' },
    { id: 20, name: 'log_line', required: true, type: 'string' },
  ]);
  log('namespace:', namespace, 'name:', name);
  await t.test('add partition spec', async () => {
    const result = await addPartitionSpec({
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
          name: 'ingest_date',
          'source-id': 3,
          transform: 'day',
        },
      ],
    });
    log('Partition spec added:', result);
  });
  let tableBucket = '';
  await t.test('get bucket', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    tableBucket = metadata.location.split('/').slice(-1)[0] ?? '';
    assert(tableBucket.length > 0, 'bad tableBucket');
  });

  let batch_num = 1;
  for (const batch of BATCHES.slice(0, BATCH_LIMIT)) {
    const batchNum = batch_num++;
    const bucket = tableBucket;
    await t.test(`batch: ${batchNum}`, async () => {
      const files = [];
      for (const file of batch) {
        const { key, size, partitions } = await _writeParquetFile(bucket, file);
        files.push({
          file: `s3://${bucket}/${key}`,
          partitions,
          recordCount: BigInt(file.length),
          fileSize: BigInt(size),
        });
      }

      log('files:', files);
      const result = await addDataFiles({
        tableBucketARN: config.tableBucketARN,
        namespace,
        name,
        lists: [{ specId: 1, schemaId: 0, files }],
      });
      log('addDataFiles result:', result);
    });
  }
});

interface LogRecord {
  app: string;
  ingest_datetime: number;
  [key: string]: string | number | null;
}

async function _writeParquetFile(
  tableBucket: string,
  list: LogRecord[]
): Promise<{ key: string; size: number; partitions: Record<string, string> }> {
  const firstRow = list[0];
  if (!firstRow) {
    throw new Error('Empty list');
  }
  const app = firstRow.app;
  const ingestDatetime = firstRow.ingest_datetime;
  const date = new Date(ingestDatetime / 1000);
  const dateParts = date.toISOString().split('T');
  const ingest_date = dateParts[0];
  if (!ingest_date) {
    throw new Error('Could not extract date string from ISO string');
  }
  const s3Key = `data/app=${app}/ingest_date=${ingest_date}/${randomUUID()}.parquet`;

  const schema = new ParquetSchema({
    app: { type: 'UTF8', compression: 'ZSTD', optional: false },
    app_ver: { type: 'UTF8', compression: 'ZSTD', optional: true },
    ingest_datetime: {
      type: 'TIMESTAMP_MICROS',
      compression: 'ZSTD',
      optional: false,
    },
    event_datetime: {
      type: 'TIMESTAMP_MICROS',
      compression: 'ZSTD',
      optional: false,
    },
    hostname: { type: 'UTF8', compression: 'ZSTD', optional: true },
    filename: { type: 'UTF8', compression: 'ZSTD', optional: true },
    log_level: { type: 'UTF8', compression: 'ZSTD', optional: true },
    device_tag: { type: 'UTF8', compression: 'ZSTD', optional: true },
    user_tag: { type: 'UTF8', compression: 'ZSTD', optional: true },
    remote_address: { type: 'UTF8', compression: 'ZSTD', optional: true },
    response_bytes: { type: 'INT32', compression: 'ZSTD', optional: true },
    response_ms: { type: 'DOUBLE', compression: 'ZSTD', optional: true },
    device_type: { type: 'UTF8', compression: 'ZSTD', optional: true },
    os: { type: 'UTF8', compression: 'ZSTD', optional: true },
    os_ver: { type: 'UTF8', compression: 'ZSTD', optional: true },
    browser: { type: 'UTF8', compression: 'ZSTD', optional: true },
    browser_ver: { type: 'UTF8', compression: 'ZSTD', optional: true },
    country: { type: 'UTF8', compression: 'ZSTD', optional: true },
    language: { type: 'UTF8', compression: 'ZSTD', optional: true },
    log_line: { type: 'UTF8', compression: 'ZSTD', optional: false },
  });

  const stream = new PassThrough();
  const chunks: Buffer[] = [];
  stream.on('data', (chunk: Buffer) => chunks.push(chunk));
  const writer = await ParquetWriter.openStream(schema, stream);
  for (const row of list) {
    await writer.appendRow(row);
  }
  await writer.close();
  const fileBuffer = Buffer.concat(chunks);
  await clients.s3.send(
    new PutObjectCommand({ Bucket: tableBucket, Key: s3Key, Body: fileBuffer })
  );
  return {
    key: s3Key,
    size: fileBuffer.length,
    partitions: { app, ingest_date },
  };
}
