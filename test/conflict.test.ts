import { strict as assert } from 'node:assert';
import { PassThrough } from 'node:stream';
import { setTimeout } from 'node:timers/promises';

import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { config } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';
import { queryRows } from './helpers/athena_helper';
import { PutObjectCommand } from '@aws-sdk/client-s3';
import { ParquetWriter, ParquetSchema } from 'parquetjs';
import { clients } from './helpers/aws_clients';

import { getMetadata, addSchema, addPartitionSpec, addDataFiles } from '../src';

import type { AddDataFilesResult } from '../src';

const schema = new ParquetSchema({
  id: { type: 'INT32' },
  name: { type: 'UTF8' },
  category: { type: 'UTF8' },
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
        { id: 3, name: 'category', type: 'string' as const, required: true },
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
          name: 'category',
          'source-id': 3,
          transform: 'identity',
        },
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
    await writer.appendRow({ id, name: nameValue, category: 'default' });
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
      partitions: { category: 'default' },
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
      lists: [{ specId: 1, schemaId: 1, files: [file] }],
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
      lists: [{ specId: 1, schemaId: 1, files: [file] }],
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
          lists: [{ specId: 1, schemaId: 1, files: [file] }],
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

  async function createMultipleFiles(
    baseId: number,
    namePrefix: string,
    category: string,
    count: number
  ) {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const bucketParts = metadata.location.split('/');
    const tableBucket = bucketParts[bucketParts.length - 1] ?? '';
    const s3Key = `data/multi-${baseId}-${Date.now()}.parquet`;

    const stream = new PassThrough();
    const chunks: Buffer[] = [];
    stream.on('data', (chunk: Buffer) => chunks.push(chunk));

    const writer = await ParquetWriter.openStream(schema, stream);
    for (let i = 0; i < count; i++) {
      await writer.appendRow({
        id: baseId + i,
        name: `${namePrefix}_${i}`,
        category,
      });
    }
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
      partitions: { category },
      recordCount: BigInt(count),
      fileSize: BigInt(fileBuffer.length),
    };
  }

  await t.test('verify data from both commits via athena', async () => {
    const rows = await queryRows<{ id: string; name: string }>(namespace, name);
    log('Athena query results:', rows);

    // Should have data from all commits: first (id=1), second (id=2), and parallel (id=200,201)
    assert(rows.length >= 4, `Expected at least 4 rows, got ${rows.length}`);

    const ids = rows.map((row) => parseInt(row.id)).sort((a, b) => a - b);
    assert(ids.includes(1), 'Should contain data from first commit (id=1)');
    assert(ids.includes(2), 'Should contain data from second commit (id=2)');
    assert(
      ids.includes(200),
      'Should contain data from parallel commit 1 (id=200)'
    );
    assert(
      ids.includes(201),
      'Should contain data from parallel commit 2 (id=201)'
    );

    const names = rows.map((row) => row.name);
    assert(names.includes('first'), 'Should contain name from first commit');
    assert(names.includes('second'), 'Should contain name from second commit');
    assert(
      names.includes('real_parallel1'),
      'Should contain name from parallel commit 1'
    );
    assert(
      names.includes('real_parallel2'),
      'Should contain name from parallel commit 2'
    );
  });

  await t.test(
    'three-way parallel commits with multiple partitions',
    async () => {
      const originalFetch = globalThis.fetch;
      globalThis.fetch = async (
        url: string | URL | Request,
        init?: RequestInit
      ) => {
        if (init?.method === 'POST') {
          await setTimeout(1000);
        }
        return originalFetch(url, init);
      };

      async function createCommitFiles(writerNum: number) {
        const files1 = await createMultipleFiles(
          1000 + writerNum * 100,
          `writer${writerNum}_cat1`,
          'cat1',
          10
        );
        const files2 = await createMultipleFiles(
          1000 + writerNum * 100 + 50,
          `writer${writerNum}_cat2`,
          'cat2',
          10
        );

        return [
          { specId: 1, schemaId: 1, files: [files1] },
          { specId: 1, schemaId: 1, files: [files2] },
        ];
      }

      async function commitFiles(writerNum: number) {
        try {
          const lists = await createCommitFiles(writerNum);
          const result = await addDataFiles({
            tableBucketARN: config.tableBucketARN,
            namespace,
            name,
            lists,
          });
          return result;
        } catch (e) {
          return e;
        }
      }

      const results = await Promise.all([
        commitFiles(1),
        commitFiles(2),
        commitFiles(3),
      ]);

      globalThis.fetch = originalFetch;

      const [result1, result2, result3] = results as (
        | AddDataFilesResult
        | Error
      )[];

      log('Three-way result 1:', result1);
      log('Three-way result 2:', result2);
      log('Three-way result 3:', result3);

      if (result1 instanceof Error) {
        throw result1;
      }
      if (result2 instanceof Error) {
        throw result2;
      }
      if (result3 instanceof Error) {
        throw result3;
      }

      const totalRetries =
        result1.retriesNeeded + result2.retriesNeeded + result3.retriesNeeded;
      log(`Three-way total retries: ${totalRetries}`);

      assert(result1.snapshotId > 0n, 'Result 1 should have valid snapshot ID');
      assert(result2.snapshotId > 0n, 'Result 2 should have valid snapshot ID');
      assert(result3.snapshotId > 0n, 'Result 3 should have valid snapshot ID');
      assert(totalRetries > 0, 'At least one commit should retry');
    }
  );

  await t.test('verify three-way parallel data via athena', async () => {
    const rows = await queryRows<{
      id: string;
      name: string;
      category: string;
    }>(namespace, name);
    log(`Total rows after three-way commits: ${rows.length}`);

    // Should have original 4 + 60 new rows (3 writers × 2 lists × 10 rows)
    assert(rows.length >= 64, `Expected at least 64 rows, got ${rows.length}`);

    // Check cat1 partition
    const cat1Rows = await queryRows<{
      id: string;
      name: string;
      category: string;
    }>(namespace, name, "category = 'cat1'");
    assert(
      cat1Rows.length >= 30,
      `Expected at least 30 cat1 rows, got ${cat1Rows.length}`
    );

    // Check cat2 partition
    const cat2Rows = await queryRows<{
      id: string;
      name: string;
      category: string;
    }>(namespace, name, "category = 'cat2'");
    assert(
      cat2Rows.length >= 30,
      `Expected at least 30 cat2 rows, got ${cat2Rows.length}`
    );

    // Verify writer data exists
    const writer1Rows = rows.filter((r) => r.name.includes('writer1'));
    const writer2Rows = rows.filter((r) => r.name.includes('writer2'));
    const writer3Rows = rows.filter((r) => r.name.includes('writer3'));

    assert(
      writer1Rows.length >= 20,
      `Expected at least 20 writer1 rows, got ${writer1Rows.length}`
    );
    assert(
      writer2Rows.length >= 20,
      `Expected at least 20 writer2 rows, got ${writer2Rows.length}`
    );
    assert(
      writer3Rows.length >= 20,
      `Expected at least 20 writer3 rows, got ${writer3Rows.length}`
    );
  });
});
