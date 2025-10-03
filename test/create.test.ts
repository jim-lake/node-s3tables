import { test } from 'node:test';
import { inspect } from 'node:util';
import {
  S3TablesClient,
  CreateNamespaceCommand,
  CreateTableCommand,
  DeleteTableCommand,
  DeleteNamespaceCommand,
} from '@aws-sdk/client-s3tables';

import { getMetadata, addSchema } from '../src';

const tableBucketARN = process.env['TABLE_BUCKET_ARN'] as string;

const client = new S3TablesClient();

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
    namespace = `test_namespace_${Date.now()}`;
    const namespace_result = await client.send(
      new CreateNamespaceCommand({ tableBucketARN, namespace: [namespace] })
    );
    console.log('Namespace created:', namespace, namespace_result);
  });
  await t.test('create table', async () => {
    name = `test_table_${Date.now()}`;
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
  await t.test('get metadata by tableBucketARN', async () => {
    const metadata = await getMetadata({ tableBucketARN, namespace, name });
    console.log('metadata:', inspect(metadata, { depth: 99 }));
  });
});
