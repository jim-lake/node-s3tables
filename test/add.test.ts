import { test } from 'node:test';
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

import { getMetadata, addSchema, addDataFiles } from '../src';

const tableBucketARN = process.env['TABLE_BUCKET_ARN'] as string;
const catalogId = process.env['CATALOG_ID'] as string;
const outputBucket = process.env['OUTPUT_BUCKET'] as string;

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
  const bucket = tableBucketARN.split('/').slice(-1)[0];
  const sql = `SELECT COUNT(*) as row_count FROM ${name}`;

  const { QueryExecutionId } = await athenaClient.send(
    new StartQueryExecutionCommand({
      QueryExecutionContext: {
        Catalog: `s3tablescatalog/${bucket}`,
        Database: namespace,
      },
      QueryString: sql,
      ResultConfiguration: { OutputLocation: `s3://${outputBucket}/output` },
    })
  );

  let result;
  let status = 'RUNNING';
  while (status === 'RUNNING' || status === 'QUEUED') {
    await setTimeout(200);
    result = await athenaClient.send(
      new GetQueryExecutionCommand({ QueryExecutionId })
    );
    status = result.QueryExecution?.Status?.State!;
  }

  if (status === 'SUCCEEDED') {
    const queryResults = await athenaClient.send(
      new GetQueryResultsCommand({ QueryExecutionId })
    );
    const rowCount = parseInt(
      queryResults.ResultSet?.Rows?.[1]?.Data?.[0]?.VarCharValue || '0'
    );
    return rowCount;
  }
  throw new Error(`Athena query failed with status: ${status}`);
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
  stream.on('data', (chunk) => chunks.push(chunk));

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
    console.log('afterAll: cleanup');
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
    console.log('Namespace created:', namespace, namespace_result);
  });

  await t.test('add lake formation tag', async () => {
    const command = new AddLFTagsToResourceCommand({
      Resource: { Database: { CatalogId: catalogId, Name: namespace } },
      LFTags: [{ TagKey: 'AccessLevel', TagValues: ['Public'] }],
    });
    const response = await LFClient.send(command);
    console.log('add tag response:', response);
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
                  id: 2,
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
    console.log('Table created:', name, table_result);
  });

  await t.test('add first parquet file (10 rows)', async () => {
    const metadata = await getMetadata({ tableBucketARN, namespace, name });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    if (!tableBucket) {
      throw new Error('Could not extract table bucket');
    }

    const { key, size } = await createParquetFile(tableBucket, 1);

    const result = await addDataFiles({
      tableBucketARN,
      namespace,
      name,
      file: `s3://${tableBucket}/${key}`,
      schemaId: 0,
      specId: 0,
      partitions: { app: 'test-app' },
      recordCount: 10n,
      fileSize: BigInt(size),
    });
    console.log('addDataFiles result 1:', result);

    const rowCount = await queryRowCount(namespace, name);
    console.log('Row count after first file:', rowCount);
    if (rowCount !== 10) {
      throw new Error(`Expected 10 rows, got ${rowCount}`);
    }
  });

  await t.test('add second parquet file (10 rows)', async () => {
    const metadata = await getMetadata({ tableBucketARN, namespace, name });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    if (!tableBucket) {
      throw new Error('Could not extract table bucket');
    }

    const { key, size } = await createParquetFile(tableBucket, 2);

    const result = await addDataFiles({
      tableBucketARN,
      namespace,
      name,
      file: `s3://${tableBucket}/${key}`,
      schemaId: 0,
      specId: 0,
      partitions: { app: 'test-app' },
      recordCount: 10n,
      fileSize: BigInt(size),
    });
    console.log('addDataFiles result 2:', result);

    const rowCount = await queryRowCount(namespace, name);
    console.log('Row count after second file:', rowCount);
    if (rowCount !== 20) {
      throw new Error(`Expected 20 rows, got ${rowCount}`);
    }
  });

  await t.test('add third parquet file (10 rows)', async () => {
    const metadata = await getMetadata({ tableBucketARN, namespace, name });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    if (!tableBucket) {
      throw new Error('Could not extract table bucket');
    }

    const { key, size } = await createParquetFile(tableBucket, 3);

    const result = await addDataFiles({
      tableBucketARN,
      namespace,
      name,
      file: `s3://${tableBucket}/${key}`,
      schemaId: 0,
      specId: 0,
      partitions: { app: 'test-app' },
      recordCount: 10n,
      fileSize: BigInt(size),
    });
    console.log('addDataFiles result 3:', result);

    const rowCount = await queryRowCount(namespace, name);
    console.log('Row count after third file:', rowCount);
    if (rowCount !== 30) {
      throw new Error(`Expected 30 rows, got ${rowCount}`);
    }
  });

  await t.test('final metadata check', async () => {
    const metadata = await getMetadata({ tableBucketARN, namespace, name });
    console.log('Final metadata:', inspect(metadata, { depth: 99 }));
  });
});
