import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { strict as assert } from 'node:assert';
import { inspect } from 'node:util';
import { PassThrough } from 'node:stream';
import { config } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';
import { executeQuery } from './helpers/athena_helper';
import { PutObjectCommand } from '@aws-sdk/client-s3';
import { ParquetWriter, ParquetSchema } from 'parquetjs';
import { clients } from './helpers/aws_clients';

import { getMetadata, addSchema, addDataFiles } from '../src';

const schema = new ParquetSchema({
  app: { type: 'UTF8' },
  event_datetime: { type: 'TIMESTAMP_MILLIS' },
});

void test('create s3tables test', async (t) => {
  const { namespace, name, tableArn } = await setupTable(
    t,
    'test_ns_create',
    'test_table1',
    [{ name: 'app', type: 'string', required: true }]
  );

  await t.test('add schema', async () => {
    const add_result = await addSchema({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      schemaId: 1,
      fields: [
        { id: 1, name: 'app', type: 'string' as const, required: true },
        {
          id: 2,
          name: 'event_datetime',
          type: 'timestamp' as const,
          required: true,
        },
      ],
    });
    log('add_result:', add_result);
  });

  await t.test('get metadata by tableARN', async () => {
    if (!tableArn) {
      throw new Error('tableArn is undefined');
    }
    const metadata_by_arn = await getMetadata({ tableArn });
    log('metadata_by_arn:', inspect(metadata_by_arn, { depth: 99 }));
  });

  await t.test('create parquet file and add to table', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const bucketParts = metadata.location.split('/');
    const tableBucket = bucketParts[bucketParts.length - 1];
    assert(
      tableBucket,
      'Could not extract table bucket from metadata location'
    );
    const s3Key = `data/app=test-app/data-${Date.now()}.parquet`;

    const stream = new PassThrough();
    const chunks: Buffer[] = [];
    stream.on('data', (chunk: Buffer) => chunks.push(chunk));

    const writer = await ParquetWriter.openStream(schema, stream);
    await writer.appendRow({ app: 'test-app', event_datetime: new Date() });
    await writer.close();

    const fileBuffer = Buffer.concat(chunks);

    await clients.s3.send(
      new PutObjectCommand({
        Bucket: tableBucket,
        Key: s3Key,
        Body: fileBuffer,
      })
    );
    const result = await addDataFiles({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      lists: [
        {
          specId: 0,
          schemaId: 1,
          files: [
            {
              file: `s3://${tableBucket}/${s3Key}`,
              partitions: { app: 'test-app' },
              recordCount: 1n,
              fileSize: BigInt(fileBuffer.length),
            },
          ],
        },
      ],
    });
    log('addDataFiles result:', result);
  });
  await t.test('create second parquet file and add to table', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const bucketParts = metadata.location.split('/');
    const tableBucket = bucketParts[bucketParts.length - 1];
    assert(
      tableBucket,
      'Could not extract table bucket from metadata location'
    );
    const s3Key = `data/app=test-app2/data-${Date.now()}.parquet`;

    const stream = new PassThrough();
    const chunks: Buffer[] = [];
    stream.on('data', (chunk: Buffer) => chunks.push(chunk));

    const writer = await ParquetWriter.openStream(schema, stream);
    await writer.appendRow({ app: 'test-app2', event_datetime: new Date() });
    await writer.close();

    const fileBuffer = Buffer.concat(chunks);

    await clients.s3.send(
      new PutObjectCommand({
        Bucket: tableBucket,
        Key: s3Key,
        Body: fileBuffer,
      })
    );
    const result = await addDataFiles({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      lists: [
        {
          specId: 0,
          schemaId: 1,
          files: [
            {
              file: `s3://${tableBucket}/${s3Key}`,
              partitions: { app: 'test-app2' },
              recordCount: 1n,
              fileSize: BigInt(fileBuffer.length),
            },
          ],
        },
      ],
    });
    log('addDataFiles second result:', result);
  });

  await t.test('get metadata after add', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    log('metadata:', inspect(metadata, { depth: 99 }));
  });

  await t.test('query table with athena', async () => {
    const sql = `SELECT * FROM ${name}`;
    const queryResults = await executeQuery(namespace, sql);
    log('Query results:', inspect(queryResults, { depth: 99 }));
  });
});
