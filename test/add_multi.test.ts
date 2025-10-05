import { test } from 'node:test';
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

import { getMetadata, addPartitionSpec, addDataFiles } from '../src';

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

async function queryRowCount(
  namespace: string,
  name: string,
  whereClause?: string
): Promise<number> {
  assert(tableBucketARN, 'tableBucketARN is not defined');
  const bucketParts = tableBucketARN.split('/');
  const bucket = bucketParts[bucketParts.length - 1];
  assert(bucket, 'Could not extract bucket from tableBucketARN');
  const sql = `SELECT COUNT(*) as row_count FROM ${name}${whereClause ? ` WHERE ${whereClause}` : ''}`;

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
  appName: string,
  date: Date,
  fileIndex: number
): Promise<{ key: string; size: number }> {
  const dateParts = date.toISOString().split('T');
  const dateStr = dateParts[0];
  assert(dateStr, 'Could not extract date string from ISO string');
  const s3Key = `data/app_name=${appName}/event_datetime_day=${dateStr}/data-${Date.now()}-${fileIndex}.parquet`;

  const schema = new ParquetSchema({
    app_name: { type: 'UTF8' },
    event_datetime: { type: 'TIMESTAMP_MILLIS' },
    detail: { type: 'UTF8' },
  });

  const stream = new PassThrough();
  const chunks: Buffer[] = [];
  stream.on('data', (chunk: Buffer) => chunks.push(chunk));

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

void test('multi-file multi-partition test', async (t) => {
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
    namespace = `test_ns_multi_${Math.floor(Math.random() * 10000)}`;
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
    name = 'test_table_multi';
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

  await t.test('add multiple files to multiple partitions', async () => {
    const metadata = await getMetadata({ tableBucketARN, namespace, name });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    assert(tableBucket, 'Could not extract table bucket');

    // Create files for different partitions
    const files = await Promise.all([
      createParquetFile(tableBucket, 'app1', new Date('2024-01-01'), 1),
      createParquetFile(tableBucket, 'app1', new Date('2024-01-02'), 2),
      createParquetFile(tableBucket, 'app2', new Date('2024-01-01'), 3),
      createParquetFile(tableBucket, 'app2', new Date('2024-01-02'), 4),
    ]);

    const result = await addDataFiles({
      tableBucketARN,
      namespace,
      name,
      lists: [
        {
          specId: 1,
          schemaId: 0,
          files: [
            {
              file: `s3://${tableBucket}/${files[0].key}`,
              partitions: {
                app_name: 'app1',
                event_datetime_day: '2024-01-01',
              },
              recordCount: 10n,
              fileSize: BigInt(files[0].size),
            },
            {
              file: `s3://${tableBucket}/${files[1].key}`,
              partitions: {
                app_name: 'app1',
                event_datetime_day: '2024-01-02',
              },
              recordCount: 10n,
              fileSize: BigInt(files[1].size),
            },
            {
              file: `s3://${tableBucket}/${files[2].key}`,
              partitions: {
                app_name: 'app2',
                event_datetime_day: '2024-01-01',
              },
              recordCount: 10n,
              fileSize: BigInt(files[2].size),
            },
            {
              file: `s3://${tableBucket}/${files[3].key}`,
              partitions: {
                app_name: 'app2',
                event_datetime_day: '2024-01-02',
              },
              recordCount: 10n,
              fileSize: BigInt(files[3].size),
            },
          ],
        },
      ],
    });
    console.log('addDataFiles result for all partitions:', result);
  });

  await t.test('verify total row count', async () => {
    const rowCount = await queryRowCount(namespace, name);
    console.log('Total row count:', rowCount);
    assert.strictEqual(rowCount, 40, `Expected 40 total rows, got ${rowCount}`);
  });

  await t.test('verify app1 partition', async () => {
    const rowCount = await queryRowCount(namespace, name, "app_name = 'app1'");
    console.log('App1 row count:', rowCount);
    assert.strictEqual(rowCount, 20, `Expected 20 rows for app1, got ${rowCount}`);
  });

  await t.test('verify app2 partition', async () => {
    const rowCount = await queryRowCount(namespace, name, "app_name = 'app2'");
    console.log('App2 row count:', rowCount);
    assert.strictEqual(rowCount, 20, `Expected 20 rows for app2, got ${rowCount}`);
  });

  await t.test('verify 2024-01-01 partition', async () => {
    const rowCount = await queryRowCount(
      namespace,
      name,
      "date(event_datetime) = date('2024-01-01')"
    );
    console.log('2024-01-01 row count:', rowCount);
    assert.strictEqual(rowCount, 20, `Expected 20 rows for 2024-01-01, got ${rowCount}`);
  });

  await t.test('verify 2024-01-02 partition', async () => {
    const rowCount = await queryRowCount(
      namespace,
      name,
      "date(event_datetime) = date('2024-01-02')"
    );
    console.log('2024-01-02 row count:', rowCount);
    assert.strictEqual(rowCount, 20, `Expected 20 rows for 2024-01-02, got ${rowCount}`);
  });

  await t.test('verify specific partition combinations', async () => {
    const combinations = [
      { where: "app_name = 'app1' AND date(event_datetime) = date('2024-01-01')", expected: 10 },
      { where: "app_name = 'app1' AND date(event_datetime) = date('2024-01-02')", expected: 10 },
      { where: "app_name = 'app2' AND date(event_datetime) = date('2024-01-01')", expected: 10 },
      { where: "app_name = 'app2' AND date(event_datetime) = date('2024-01-02')", expected: 10 },
    ];

    for (const { where, expected } of combinations) {
      const rowCount = await queryRowCount(namespace, name, where);
      console.log(`Row count for ${where}:`, rowCount);
      assert.strictEqual(rowCount, expected, `Expected ${expected} rows for ${where}, got ${rowCount}`);
    }
  });

  await t.test('add multiple files in multiple lists', async () => {
    const metadata = await getMetadata({ tableBucketARN, namespace, name });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    assert(tableBucket, 'Could not extract table bucket');

    // Create 4 more files for different partitions
    const files = await Promise.all([
      createParquetFile(tableBucket, 'app3', new Date('2024-01-01'), 5),
      createParquetFile(tableBucket, 'app3', new Date('2024-01-02'), 6),
      createParquetFile(tableBucket, 'app4', new Date('2024-01-01'), 7),
      createParquetFile(tableBucket, 'app4', new Date('2024-01-02'), 8),
    ]);

    const result = await addDataFiles({
      tableBucketARN,
      namespace,
      name,
      lists: [
        {
          specId: 1,
          schemaId: 0,
          files: [
            {
              file: `s3://${tableBucket}/${files[0].key}`,
              partitions: {
                app_name: 'app3',
                event_datetime_day: '2024-01-01',
              },
              recordCount: 10n,
              fileSize: BigInt(files[0].size),
            },
            {
              file: `s3://${tableBucket}/${files[1].key}`,
              partitions: {
                app_name: 'app3',
                event_datetime_day: '2024-01-02',
              },
              recordCount: 10n,
              fileSize: BigInt(files[1].size),
            },
          ],
        },
        {
          specId: 1,
          schemaId: 0,
          files: [
            {
              file: `s3://${tableBucket}/${files[2].key}`,
              partitions: {
                app_name: 'app4',
                event_datetime_day: '2024-01-01',
              },
              recordCount: 10n,
              fileSize: BigInt(files[2].size),
            },
            {
              file: `s3://${tableBucket}/${files[3].key}`,
              partitions: {
                app_name: 'app4',
                event_datetime_day: '2024-01-02',
              },
              recordCount: 10n,
              fileSize: BigInt(files[3].size),
            },
          ],
        },
      ],
    });
    console.log('addDataFiles result for multiple lists:', result);
  });

  await t.test('verify total row count after second batch', async () => {
    const rowCount = await queryRowCount(namespace, name);
    console.log('Total row count after second batch:', rowCount);
    assert.strictEqual(rowCount, 80, `Expected 80 total rows after second batch, got ${rowCount}`);
  });

  await t.test('verify new app partitions', async () => {
    const app3Count = await queryRowCount(namespace, name, "app_name = 'app3'");
    const app4Count = await queryRowCount(namespace, name, "app_name = 'app4'");
    console.log('App3 row count:', app3Count);
    console.log('App4 row count:', app4Count);
    assert.strictEqual(app3Count, 20, `Expected 20 rows for app3, got ${app3Count}`);
    assert.strictEqual(app4Count, 20, `Expected 20 rows for app4, got ${app4Count}`);
  });

  await t.test('final metadata check', async () => {
    const metadata = await getMetadata({ tableBucketARN, namespace, name });
    console.log('Final metadata:', inspect(metadata, { depth: 99 }));
  });
});
