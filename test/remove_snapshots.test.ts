import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { strict as assert } from 'node:assert';
import { PassThrough } from 'node:stream';
import { config, clients } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';
import { PutObjectCommand } from '@aws-sdk/client-s3';
import { ParquetWriter, ParquetSchema } from 'parquetjs';

import { getMetadata, addSchema, addDataFiles, removeSnapshots } from '../src';

const schema = new ParquetSchema({
  app: { type: 'UTF8' },
  event_datetime: { type: 'TIMESTAMP_MILLIS' },
});

void test('removeSnapshots test', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_remove_snapshots',
    'test_table_snapshots',
    [{ name: 'app', type: 'string', required: true }]
  );

  await t.test('add schema', async () => {
    await addSchema({
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
  });

  const snapshotIds: bigint[] = [];

  await t.test('add first data file', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const bucketParts = metadata.location.split('/');
    const tableBucket = bucketParts[bucketParts.length - 1];
    assert(tableBucket);

    const s3Key = `data/app=app1/data-${Date.now()}.parquet`;
    const stream = new PassThrough();
    const chunks: Buffer[] = [];
    stream.on('data', (chunk: Buffer) => chunks.push(chunk));

    const writer = await ParquetWriter.openStream(schema, stream);
    await writer.appendRow({ app: 'app1', event_datetime: new Date() });
    await writer.close();

    const fileBuffer = Buffer.concat(chunks);

    await clients.s3.send(
      new PutObjectCommand({
        Bucket: tableBucket,
        Key: s3Key,
        Body: fileBuffer,
      })
    );
    await addDataFiles({
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
              partitions: { app: 'app1' },
              recordCount: 1n,
              fileSize: BigInt(fileBuffer.length),
            },
          ],
        },
      ],
    });

    const updatedMetadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const snapshotId = updatedMetadata['current-snapshot-id'];
    assert(snapshotId);
    snapshotIds.push(BigInt(snapshotId));
  });

  await t.test('add second data file', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const bucketParts = metadata.location.split('/');
    const tableBucket = bucketParts[bucketParts.length - 1];
    assert(tableBucket);

    const s3Key = `data/app=app2/data-${Date.now()}.parquet`;
    const stream = new PassThrough();
    const chunks: Buffer[] = [];
    stream.on('data', (chunk: Buffer) => chunks.push(chunk));

    const writer = await ParquetWriter.openStream(schema, stream);
    await writer.appendRow({ app: 'app2', event_datetime: new Date() });
    await writer.close();

    const fileBuffer = Buffer.concat(chunks);

    await clients.s3.send(
      new PutObjectCommand({
        Bucket: tableBucket,
        Key: s3Key,
        Body: fileBuffer,
      })
    );
    await addDataFiles({
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
              partitions: { app: 'app2' },
              recordCount: 1n,
              fileSize: BigInt(fileBuffer.length),
            },
          ],
        },
      ],
    });

    const updatedMetadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const snapshotId = updatedMetadata['current-snapshot-id'];
    assert(snapshotId);
    snapshotIds.push(BigInt(snapshotId));
  });

  await t.test('add third data file', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const bucketParts = metadata.location.split('/');
    const tableBucket = bucketParts[bucketParts.length - 1];
    assert(tableBucket);

    const s3Key = `data/app=app3/data-${Date.now()}.parquet`;
    const stream = new PassThrough();
    const chunks: Buffer[] = [];
    stream.on('data', (chunk: Buffer) => chunks.push(chunk));

    const writer = await ParquetWriter.openStream(schema, stream);
    await writer.appendRow({ app: 'app3', event_datetime: new Date() });
    await writer.close();

    const fileBuffer = Buffer.concat(chunks);

    await clients.s3.send(
      new PutObjectCommand({
        Bucket: tableBucket,
        Key: s3Key,
        Body: fileBuffer,
      })
    );
    await addDataFiles({
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
              partitions: { app: 'app3' },
              recordCount: 1n,
              fileSize: BigInt(fileBuffer.length),
            },
          ],
        },
      ],
    });

    const updatedMetadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const snapshotId = updatedMetadata['current-snapshot-id'];
    assert(snapshotId);
    snapshotIds.push(BigInt(snapshotId));
  });

  await t.test('add fourth data file', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const bucketParts = metadata.location.split('/');
    const tableBucket = bucketParts[bucketParts.length - 1];
    assert(tableBucket);

    const s3Key = `data/app=app4/data-${Date.now()}.parquet`;
    const stream = new PassThrough();
    const chunks: Buffer[] = [];
    stream.on('data', (chunk: Buffer) => chunks.push(chunk));

    const writer = await ParquetWriter.openStream(schema, stream);
    await writer.appendRow({ app: 'app4', event_datetime: new Date() });
    await writer.close();

    const fileBuffer = Buffer.concat(chunks);

    await clients.s3.send(
      new PutObjectCommand({
        Bucket: tableBucket,
        Key: s3Key,
        Body: fileBuffer,
      })
    );
    await addDataFiles({
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
              partitions: { app: 'app4' },
              recordCount: 1n,
              fileSize: BigInt(fileBuffer.length),
            },
          ],
        },
      ],
    });

    const updatedMetadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const snapshotId = updatedMetadata['current-snapshot-id'];
    assert(snapshotId);
    snapshotIds.push(BigInt(snapshotId));
  });

  await t.test('verify 4 snapshots exist', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    assert.equal(snapshotIds.length, 4);
    assert.equal(metadata.snapshots.length, 4);
    assert.equal(metadata['current-snapshot-id'], snapshotIds[3]);
    log('Snapshots before removal:', snapshotIds);
  });

  await t.test('remove oldest snapshot', async () => {
    const snapshot0 = snapshotIds[0];
    assert(snapshot0);
    await removeSnapshots({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      snapshotIds: [snapshot0],
    });
    log('Removed oldest snapshot:', snapshot0);
  });

  await t.test('remove second oldest snapshot', async () => {
    const snapshot1 = snapshotIds[1];
    assert(snapshot1);
    await removeSnapshots({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      snapshotIds: [snapshot1],
    });
    log('Removed second oldest snapshot:', snapshot1);
  });

  await t.test('verify 2 snapshots remain', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    assert.equal(metadata.snapshots.length, 2);
    const remainingIds = metadata.snapshots.map((s) => s['snapshot-id']);
    const snapshot2 = snapshotIds[2];
    const snapshot3 = snapshotIds[3];
    assert(snapshot2);
    assert(snapshot3);
    assert(remainingIds.includes(snapshot2));
    assert(remainingIds.includes(snapshot3));
    assert.equal(metadata['current-snapshot-id'], snapshot3);
    log('Remaining snapshots:', remainingIds);
  });
});
