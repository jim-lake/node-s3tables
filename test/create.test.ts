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
} from '@aws-sdk/client-lakeformation'; // ES Modules import
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

void test('create s3tables test', async (t) => {
  let namespace: string;
  let name: string;
  let tableArn: string;

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
    namespace = `test_ns_create_${Math.floor(Math.random() * 10000)}`;
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
    name = 'test_table1';
    const table_result = await client.send(
      new CreateTableCommand({
        tableBucketARN,
        namespace,
        name,
        format: 'ICEBERG',
        metadata: {
          iceberg: {
            schema: {
              fields: [{ name: 'app', type: 'string', required: true }],
            },
          },
        },
      })
    );
    tableArn = table_result.tableARN as string;
    console.log('Table created:', name, table_result);
  });
  await t.test('add schema', async () => {
    const add_result = await addSchema({
      tableBucketARN,
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
    console.log('add_result:', add_result);
  });
  await t.test('get metadata by tableARN', async () => {
    const metadata_by_arn = await getMetadata({ tableArn });
    console.log('metadata_by_arn:', inspect(metadata_by_arn, { depth: 99 }));
  });
  await t.test('create parquet file and add to table', async () => {
    const metadata = await getMetadata({ tableBucketARN, namespace, name });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    const s3Key = `data/app=test-app/data-${Date.now()}.parquet`;

    const schema = new ParquetSchema({
      app: { type: 'UTF8' },
      event_datetime: { type: 'TIMESTAMP_MILLIS' },
    });

    const stream = new PassThrough();
    const chunks: Buffer[] = [];
    stream.on('data', (chunk) => chunks.push(chunk));

    const writer = await ParquetWriter.openStream(schema, stream);
    await writer.appendRow({ app: 'test-app', event_datetime: new Date() });
    await writer.close();

    const fileBuffer = Buffer.concat(chunks);

    await s3Client.send(
      new PutObjectCommand({
        Bucket: tableBucket,
        Key: s3Key,
        Body: fileBuffer,
      })
    );
    const result = await addDataFiles({
      tableBucketARN,
      namespace,
      name,
      file: `s3://${tableBucket}/${s3Key}`,
      schemaId: 1,
      specId: 0,
      partitions: { app: 'test-app' },
      recordCount: 1n,
      fileSize: BigInt(fileBuffer.length),
    });
    console.log('addDataFiles result:', result);
  });
  await t.test('get metadata after add', async () => {
    const metadata = await getMetadata({ tableBucketARN, namespace, name });
    console.log('metadata:', inspect(metadata, { depth: 99 }));
  });
  await t.test('query table with athena', async () => {
    const bucket = tableBucketARN.split('/').slice(-1)[0];
    const sql = `SELECT * FROM ${name}`;

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
      console.log('Athena query succeeded');
      const queryResults = await athenaClient.send(
        new GetQueryResultsCommand({ QueryExecutionId })
      );
      console.log('Query results:', inspect(queryResults, { depth: 99 }));
    } else {
      throw new Error(`Athena query failed with status: ${status}`);
    }
  });
});
