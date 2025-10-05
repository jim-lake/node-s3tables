import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { strict as assert } from 'node:assert';
import { inspect } from 'node:util';
import { PassThrough } from 'node:stream';
import { setTimeout } from 'node:timers/promises';
import {
  S3TablesClient,
  CreateNamespaceCommand,
  CreateTableCommand,
  DeleteTableCommand,
  DeleteNamespaceCommand,
} from '@aws-sdk/client-s3tables';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import {
  LakeFormationClient,
  AddLFTagsToResourceCommand,
} from '@aws-sdk/client-lakeformation';
import {
  AthenaClient,
  StartQueryExecutionCommand,
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
} from '@aws-sdk/client-athena';
import { ParquetWriter, ParquetSchema } from 'parquetjs';

import { getMetadata, addDataFiles } from '../src';

const tableBucketARN = process.env['TABLE_BUCKET_ARN'];
const catalogId = process.env['CATALOG_ID'];
const outputBucket = process.env['OUTPUT_BUCKET'];

if (!tableBucketARN) {
  throw new Error('environment requires TABLE_BUCKET_ARN');
}
if (!catalogId) {
  throw new Error('environment requires CATALOG_ID');
}
if (!outputBucket) {
  throw new Error('environment requires OUTPUT_BUCKET');
}

const client = new S3TablesClient();
const region = tableBucketARN.split(':')[3];
const s3Client = new S3Client(region ? { region } : {});
const LFClient = new LakeFormationClient(region ? { region } : {});
const athenaClient = new AthenaClient(region ? { region } : {});

async function queryRowCount(namespace: string, name: string): Promise<number> {
  assert(tableBucketARN, 'tableBucketARN is not defined');
  const bucketParts = tableBucketARN.split('/');
  const bucket = bucketParts[bucketParts.length - 1];
  assert(bucket, 'Could not extract bucket from tableBucketARN');
  const sql = `SELECT COUNT(*) as row_count FROM ${name}`;

  const { QueryExecutionId } = await athenaClient.send(
    new StartQueryExecutionCommand({
      QueryExecutionContext: {
        Catalog: `s3tablescatalog/${bucket}`,
        Database: namespace,
      },
      QueryString: sql,
      ResultConfiguration: {
        OutputLocation: `s3://${outputBucket ?? ''}/output`,
      },
    })
  );

  let result;
  let status = 'RUNNING';
  while (status === 'RUNNING' || status === 'QUEUED') {
    await setTimeout(200);
    result = await athenaClient.send(
      new GetQueryExecutionCommand({ QueryExecutionId })
    );
    status = result.QueryExecution?.Status?.State ?? 'FAILED';
  }

  if (status === 'SUCCEEDED') {
    const queryResults = await athenaClient.send(
      new GetQueryResultsCommand({ QueryExecutionId })
    );
    const rowCount = parseInt(
      queryResults.ResultSet?.Rows?.[1]?.Data?.[0]?.VarCharValue ?? '0',
      10
    );
    return rowCount;
  }
  assert.fail(`Athena query failed with status: ${status}`);
}

async function createParquetFile(
  tableBucket: string,
  fileIndex: number
): Promise<{ key: string; size: number }> {
  const s3Key = `data/app=test-app/data-${Date.now()}-${fileIndex}.parquet`;

  const schema = new ParquetSchema({
    app: { type: 'UTF8' },
    event_datetime: { type: 'TIMESTAMP_MILLIS' },
  });

  const stream = new PassThrough();
  const chunks: Buffer[] = [];
  stream.on('data', (chunk: Buffer) => chunks.push(chunk));

  const writer = await ParquetWriter.openStream(schema, stream);

  // Add 10 rows to each file
  for (let i = 0; i < 10; i++) {
    await writer.appendRow({
      app: 'test-app',
      event_datetime: new Date(Date.now() + i * 1000),
    });
  }

  await writer.close();

  const fileBuffer = Buffer.concat(chunks);

  await s3Client.send(
    new PutObjectCommand({ Bucket: tableBucket, Key: s3Key, Body: fileBuffer })
  );

  return { key: s3Key, size: fileBuffer.length };
}

void test('add multiple parquet files test', async (t) => {
  let namespace: string;
  let name: string;

  t.after(async () => {
    try {
      if (name) {
        await client.send(
          new DeleteTableCommand({ tableBucketARN, namespace, name })
        );
        console.log('Table deleted:', namespace, name);
      }
      if (namespace) {
        await client.send(
          new DeleteNamespaceCommand({ tableBucketARN, namespace })
        );
        console.log('Namespace deleted:', namespace);
      }
    } catch (error) {
      console.error('Cleanup failed:', error);
    }
  });

  await t.test('create namespace', async () => {
    namespace = `test_ns_add_${Math.floor(Math.random() * 10000)}`;
    const namespace_result = await client.send(
      new CreateNamespaceCommand({ tableBucketARN, namespace: [namespace] })
    );
    log('Namespace created:', namespace, namespace_result);
  });

  await t.test('add lake formation tag', async () => {
    const command = new AddLFTagsToResourceCommand({
      Resource: { Database: { CatalogId: catalogId, Name: namespace } },
      LFTags: [{ TagKey: 'AccessLevel', TagValues: ['Public'] }],
    });
    const response = await LFClient.send(command);
    log('add tag response:', response);
  });

  await t.test('create table', async () => {
    name = 'test_table_add';
    const table_result = await client.send(
      new CreateTableCommand({
        tableBucketARN,
        namespace,
        name,
        format: 'ICEBERG',
        metadata: {
          iceberg: {
            schema: {
              fields: [
                { name: 'app', type: 'string', required: true },
                {
                  name: 'event_datetime',
                  type: 'timestamp' as const,
                  required: true,
                },
              ],
            },
          },
        },
      })
    );
    log('Table created:', name, table_result);
  });

  await t.test('add first parquet file (10 rows)', async () => {
    const metadata = await getMetadata({ tableBucketARN, namespace, name });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    assert(tableBucket, 'Could not extract table bucket');

    const { key, size } = await createParquetFile(tableBucket, 1);

    const result = await addDataFiles({
      tableBucketARN,
      namespace,
      name,
      lists: [
        {
          specId: 0,
          schemaId: 0,
          files: [
            {
              file: `s3://${tableBucket}/${key}`,
              partitions: { app: 'test-app' },
              recordCount: 10n,
              fileSize: BigInt(size),
            },
          ],
        },
      ],
    });
    log('addDataFiles result 1:', result);

    const rowCount = await queryRowCount(namespace, name);
    log('Row count after first file:', rowCount);
    assert.strictEqual(rowCount, 10, `Expected 10 rows after first file, got ${rowCount}`);
  });

  await t.test('add second parquet file (10 rows)', async () => {
    const metadata = await getMetadata({ tableBucketARN, namespace, name });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    assert(tableBucket, 'Could not extract table bucket');

    const { key, size } = await createParquetFile(tableBucket, 2);

    const result = await addDataFiles({
      tableBucketARN,
      namespace,
      name,
      lists: [
        {
          specId: 0,
          schemaId: 0,
          files: [
            {
              file: `s3://${tableBucket}/${key}`,
              partitions: { app: 'test-app' },
              recordCount: 10n,
              fileSize: BigInt(size),
            },
          ],
        },
      ],
    });
    log('addDataFiles result 2:', result);

    const rowCount = await queryRowCount(namespace, name);
    log('Row count after second file:', rowCount);
    assert.strictEqual(rowCount, 20, `Expected 20 rows after second file, got ${rowCount}`);
  });

  await t.test('add third parquet file (10 rows)', async () => {
    const metadata = await getMetadata({ tableBucketARN, namespace, name });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    assert(tableBucket, 'Could not extract table bucket');

    const { key, size } = await createParquetFile(tableBucket, 3);

    const result = await addDataFiles({
      tableBucketARN,
      namespace,
      name,
      lists: [
        {
          specId: 0,
          schemaId: 0,
          files: [
            {
              file: `s3://${tableBucket}/${key}`,
              partitions: { app: 'test-app' },
              recordCount: 10n,
              fileSize: BigInt(size),
            },
          ],
        },
      ],
    });
    log('addDataFiles result 3:', result);

    const rowCount = await queryRowCount(namespace, name);
    log('Row count after third file:', rowCount);
    assert.strictEqual(rowCount, 30, `Expected 30 rows after third file, got ${rowCount}`);
  });

  await t.test('final metadata check', async () => {
    const metadata = await getMetadata({ tableBucketARN, namespace, name });
    log('Final metadata:', inspect(metadata, { depth: 99 }));
  });
});
