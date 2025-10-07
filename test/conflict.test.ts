import { strict as assert } from 'node:assert';
import { PassThrough } from 'node:stream';
import { setTimeout } from 'node:timers/promises';

import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { config } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';
import { PutObjectCommand } from '@aws-sdk/client-s3';
import { ParquetWriter, ParquetSchema } from 'parquetjs';
import { clients } from './helpers/aws_clients';

import { getMetadata, addSchema, addDataFiles } from '../src';

import type { AddDataFilesResult } from '../src';

const schema = new ParquetSchema({
  id: { type: 'INT32' },
  name: { type: 'UTF8' },
});

void test('conflict test', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_conflict',
    'test_table_conflict',
    [{ name: 'id', type: 'int', required: true }]
  );

  await t.test('add schema', async () => {
    await addSchema({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      schemaId: 1,
      fields: [
        { id: 1, name: 'id', type: 'int' as const, required: true },
        { id: 2, name: 'name', type: 'string' as const, required: true },
      ],
    });
  });

  async function createDataFile(id: number, nameValue: string) {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const bucketParts = metadata.location.split('/');
    const tableBucket = bucketParts[bucketParts.length - 1] ?? '';
    const s3Key = `data/file-${id}-${Date.now()}.parquet`;

    const stream = new PassThrough();
    const chunks: Buffer[] = [];
    stream.on('data', (chunk: Buffer) => chunks.push(chunk));

    const writer = await ParquetWriter.openStream(schema, stream);
    await writer.appendRow({ id, name: nameValue });
    await writer.close();

    const fileBuffer = Buffer.concat(chunks);

    await clients.s3.send(
      new PutObjectCommand({
        Bucket: tableBucket,
        Key: s3Key,
        Body: fileBuffer,
      })
    );

    return {
      file: `s3://${tableBucket}/${s3Key}`,
      partitions: {},
      recordCount: 1n,
      fileSize: BigInt(fileBuffer.length),
    };
  }

  await t.test('first commit', async () => {
    const file = await createDataFile(1, 'first');

    const result = await addDataFiles({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      lists: [{ specId: 0, schemaId: 1, files: [file] }],
    });

    log('First commit result:', result);
    assert(result.retriesNeeded === 0, 'First commit should not need retries');
  });

  await t.test('second commit', async () => {
    const file = await createDataFile(2, 'second');

    const result = await addDataFiles({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      lists: [{ specId: 0, schemaId: 1, files: [file] }],
    });

    log('Second commit result:', result);
    assert(result.retriesNeeded === 0, 'Second commit should not need retries');
  });

  await t.test('real parallel commits', async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = async (
      url: string | URL | Request,
      init?: RequestInit
    ) => {
      if (init?.method === 'POST') {
        log('delaying....');
        await setTimeout(1000);
      } else {
        log('not delaying');
      }
      return originalFetch(url, init);
    };

    const file1 = await createDataFile(200, 'real_parallel1');
    const file2 = await createDataFile(201, 'real_parallel2');

    async function _addFiles(file: Awaited<ReturnType<typeof createDataFile>>) {
      try {
        const result = await addDataFiles({
          tableBucketARN: config.tableBucketARN,
          namespace,
          name,
          lists: [{ specId: 0, schemaId: 1, files: [file] }],
        });
        return result;
      } catch (e) {
        return e;
      }
    }

    const results = await Promise.all([_addFiles(file1), _addFiles(file2)]);
    const result1 = results[0] as AddDataFilesResult | Error;
    const result2 = results[1] as AddDataFilesResult | Error;
    globalThis.fetch = originalFetch;
    log('Real parallel result 1:', result1);
    log('Real parallel result 2:', result2);
    if (result1 instanceof Error) {
      throw result1;
    } else if (result2 instanceof Error) {
      throw result2;
    }
    const totalRetries = result1.retriesNeeded + result2.retriesNeeded;
    log(`Real parallel total retries: ${totalRetries}`);

    // Both should succeed
    assert(result1.snapshotId > 0n, 'Result 1 should have valid snapshot ID');
    assert(result2.snapshotId > 0n, 'Result 2 should have valid snapshot ID');
    assert(totalRetries > 0, 'one branch must retry at least once');
  });
});
