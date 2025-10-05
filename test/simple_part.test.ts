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

import { getMetadata, addPartitionSpec, addDataFiles } from '../src';

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

async function queryRowCount(
  namespace: string,
  name: string,
  whereClause?: string
): Promise<number> {
  const bucket = tableBucketARN.split('/').slice(-1)[0];
  const sql = `SELECT COUNT(*) as row_count FROM ${name}${whereClause ? ` WHERE ${whereClause}` : ''}`;

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
      queryResults.ResultSet?.Rows?.[1]?.Data?.[0]?.VarCharValue ?? '0',
      10
    );
    return rowCount;
  }
  throw new Error(`Athena query failed with status: ${status}`);
}

async function createParquetFile(
  tableBucket: string,
  appName: string,
  date: Date,
  fileIndex: number
): Promise<{ key: string; size: number }> {
  const dateStr = date.toISOString().split('T')[0];
  const s3Key = `data/app_name=${appName}/event_datetime_day=${dateStr}/data-${Date.now()}-${fileIndex}.parquet`;

  const schema = new ParquetSchema({
    app_name: { type: 'UTF8' },
    event_datetime: { type: 'TIMESTAMP_MILLIS' },
    detail: { type: 'UTF8' },
  });

  const stream = new PassThrough();
  const chunks: Buffer[] = [];
  stream.on('data', (chunk) => chunks.push(chunk));

  const writer = await ParquetWriter.openStream(schema, stream);

  for (let i = 0; i < 10; i++) {
    await writer.appendRow({
      app_name: appName,
      event_datetime: new Date(date.getTime() + i * 1000),
      detail: `Detail for ${appName} record ${i}`,
    });
  }

  await writer.close();

  const fileBuffer = Buffer.concat(chunks);

  await s3Client.send(
    new PutObjectCommand({ Bucket: tableBucket, Key: s3Key, Body: fileBuffer })
  );

  return { key: s3Key, size: fileBuffer.length };
}

void test('multi-level partitioning test', async (t) => {
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
    namespace = `test_ns_part_${Math.floor(Math.random() * 10000)}`;
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
    name = 'test_table_part';
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
                { name: 'app_name', type: 'string', required: true },
                { name: 'event_datetime', type: 'timestamp', required: true },
                { name: 'detail', type: 'string', required: false },
              ],
            },
          },
        },
      })
    );
    console.log('Table created:', name, table_result);
  });

  await t.test('add partition spec', async () => {
    const result = await addPartitionSpec({
      tableBucketARN,
      namespace,
      name,
      specId: 1,
      fields: [
        {
          'field-id': 1000,
          name: 'app_name',
          'source-id': 1,
          transform: 'identity',
        },
        {
          'field-id': 1001,
          name: 'event_datetime_day',
          'source-id': 2,
          transform: 'day',
        },
      ],
    });
    console.log('Partition spec added:', result);
  });

  await t.test('add files to app1/2024-01-01 partition', async () => {
    const metadata = await getMetadata({ tableBucketARN, namespace, name });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    if (!tableBucket) {
      throw new Error('Could not extract table bucket');
    }

    const date = new Date('2024-01-01');
    const { key, size } = await createParquetFile(tableBucket, 'app1', date, 1);

    const result = await addDataFiles({
      tableBucketARN,
      namespace,
      name,
      file: `s3://${tableBucket}/${key}`,
      schemaId: 0,
      specId: 1,
      partitions: { app_name: 'app1', event_datetime_day: '2024-01-01' },
      recordCount: 10n,
      fileSize: BigInt(size),
    });
    console.log('addDataFiles result app1/2024-01-01:', result);
  });

  await t.test('add files to app1/2024-01-02 partition', async () => {
    const metadata = await getMetadata({ tableBucketARN, namespace, name });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    if (!tableBucket) {
      throw new Error('tableBucket is undefined');
    }

    const date = new Date('2024-01-02');
    const { key, size } = await createParquetFile(tableBucket, 'app1', date, 2);

    const result = await addDataFiles({
      tableBucketARN,
      namespace,
      name,
      file: `s3://${tableBucket}/${key}`,
      schemaId: 0,
      specId: 1,
      partitions: { app_name: 'app1', event_datetime_day: '2024-01-02' },
      recordCount: 10n,
      fileSize: BigInt(size),
    });
    console.log('addDataFiles result app1/2024-01-02:', result);
  });

  await t.test('add files to app2/2024-01-01 partition', async () => {
    const metadata = await getMetadata({ tableBucketARN, namespace, name });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    if (!tableBucket) {
      throw new Error('tableBucket is undefined');
    }

    const date = new Date('2024-01-01');
    const { key, size } = await createParquetFile(tableBucket, 'app2', date, 3);

    const result = await addDataFiles({
      tableBucketARN,
      namespace,
      name,
      file: `s3://${tableBucket}/${key}`,
      schemaId: 0,
      specId: 1,
      partitions: { app_name: 'app2', event_datetime_day: '2024-01-01' },
      recordCount: 10n,
      fileSize: BigInt(size),
    });
    console.log('addDataFiles result app2/2024-01-01:', result);
  });

  await t.test('add files to app2/2024-01-02 partition', async () => {
    const metadata = await getMetadata({ tableBucketARN, namespace, name });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    if (!tableBucket) {
      throw new Error('tableBucket is undefined');
    }

    const date = new Date('2024-01-02');
    const { key, size } = await createParquetFile(tableBucket, 'app2', date, 4);

    const result = await addDataFiles({
      tableBucketARN,
      namespace,
      name,
      file: `s3://${tableBucket}/${key}`,
      schemaId: 0,
      specId: 1,
      partitions: { app_name: 'app2', event_datetime_day: '2024-01-02' },
      recordCount: 10n,
      fileSize: BigInt(size),
    });
    console.log('addDataFiles result app2/2024-01-02:', result);
  });

  await t.test('query total row count', async () => {
    const rowCount = await queryRowCount(namespace, name);
    console.log('Total row count:', rowCount);
    if (rowCount !== 40) {
      throw new Error(`Expected 40 rows, got ${rowCount}`);
    }
  });

  await t.test('query app1 partition', async () => {
    const rowCount = await queryRowCount(namespace, name, "app_name = 'app1'");
    console.log('App1 row count:', rowCount);
    if (rowCount !== 20) {
      throw new Error(`Expected 20 rows for app1, got ${rowCount}`);
    }
  });

  await t.test('query 2024-01-01 partition', async () => {
    const rowCount = await queryRowCount(
      namespace,
      name,
      "date(event_datetime) = date('2024-01-01')"
    );
    console.log('2024-01-01 row count:', rowCount);
    if (rowCount !== 20) {
      throw new Error(`Expected 20 rows for 2024-01-01, got ${rowCount}`);
    }
  });

  await t.test('query specific partition', async () => {
    const rowCount = await queryRowCount(
      namespace,
      name,
      "app_name = 'app1' AND date(event_datetime) = date('2024-01-01')"
    );
    console.log('App1 2024-01-01 row count:', rowCount);
    if (rowCount !== 10) {
      throw new Error(`Expected 10 rows for app1/2024-01-01, got ${rowCount}`);
    }
  });

  await t.test('final metadata check', async () => {
    const metadata = await getMetadata({ tableBucketARN, namespace, name });
    console.log('Final metadata:', inspect(metadata, { depth: 99 }));
  });
});
