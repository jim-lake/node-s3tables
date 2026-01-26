import { test } from './helpers/test_helper';
import { strict as assert } from 'node:assert';
import { PassThrough } from 'node:stream';
import { config, clients } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';
import { PutObjectCommand } from '@aws-sdk/client-s3';
import { ParquetWriter, ParquetSchema } from 'parquetjs';

import { getMetadata, addSchema, addDataFiles } from '../src';

const schema = new ParquetSchema({
  app: { type: 'UTF8' },
  event_datetime: { type: 'TIMESTAMP_MILLIS' },
});

void test('maxSnapshots test', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_max_snapshots',
    'test_table_max_snapshots',
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

  async function addFile(appName: string) {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const bucketParts = metadata.location.split('/');
    const tableBucket = bucketParts[bucketParts.length - 1];
    assert(tableBucket);

    const s3Key = `data/app=${appName}/data-${Date.now()}.parquet`;
    const stream = new PassThrough();
    const chunks: Buffer[] = [];
    stream.on('data', (chunk: Buffer) => chunks.push(chunk));

    const writer = await ParquetWriter.openStream(schema, stream);
    await writer.appendRow({ app: appName, event_datetime: new Date() });
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
      maxSnapshots: 3,
      lists: [
        {
          specId: 0,
          schemaId: 1,
          files: [
            {
              file: `s3://${tableBucket}/${s3Key}`,
              partitions: { app: appName },
              recordCount: 1n,
              fileSize: BigInt(fileBuffer.length),
            },
          ],
        },
      ],
    });
  }

  await t.test('add file 1 - expect 1 snapshot', async () => {
    await addFile('app1');
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    assert.equal(metadata.snapshots.length, 1);
  });

  await t.test('add file 2 - expect 2 snapshots', async () => {
    await addFile('app2');
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    assert.equal(metadata.snapshots.length, 2);
  });

  await t.test('add file 3 - expect 3 snapshots', async () => {
    await addFile('app3');
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    assert.equal(metadata.snapshots.length, 3);
  });

  await t.test('add file 4 - expect 3 snapshots', async () => {
    await addFile('app4');
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    assert.equal(metadata.snapshots.length, 3);
  });

  await t.test('add file 5 - expect 3 snapshots', async () => {
    await addFile('app5');
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    assert.equal(metadata.snapshots.length, 3);
  });

  await t.test('add file 6 - expect 3 snapshots', async () => {
    await addFile('app6');
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    assert.equal(metadata.snapshots.length, 3);
  });
});
