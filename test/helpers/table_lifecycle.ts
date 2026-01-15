import { log } from './log_helper';
import {
  clients,
  config,
  CreateNamespaceCommand,
  CreateTableCommand,
  DeleteTableCommand,
  DeleteNamespaceCommand,
} from './aws_clients';
import { AddLFTagsToResourceCommand } from '@aws-sdk/client-lakeformation';
import type { TestContext } from 'node:test';

export interface TableSetup {
  namespace: string;
  name: string;
  tableArn?: string;
}

export async function setupTable(
  t: TestContext,
  namePrefix: string,
  tableName: string,
  schema: { name: string; type: string; required: boolean }[]
): Promise<TableSetup> {
  const namespace = `${namePrefix}_${Math.floor(Math.random() * 10000)}`;
  const name = tableName;

  t.after(async () => {
    if (process.env['NO_CLEANUP']) {
      log('afterAll: skip cleanup:', namespace, name);
      return;
    }
    log('afterAll: cleanup:', namespace, name);
    try {
      if (name) {
        await clients.s3Tables.send(
          new DeleteTableCommand({
            tableBucketARN: config.tableBucketARN,
            namespace,
            name,
          })
        );
        log('Table deleted:', namespace, name);
      }
      if (namespace) {
        await clients.s3Tables.send(
          new DeleteNamespaceCommand({
            tableBucketARN: config.tableBucketARN,
            namespace,
          })
        );
        log('Namespace deleted:', namespace);
      }
    } catch (error) {
      console.error('Cleanup failed:', error);
    }
  });

  // Create namespace
  const namespace_result = await clients.s3Tables.send(
    new CreateNamespaceCommand({
      tableBucketARN: config.tableBucketARN,
      namespace: [namespace],
    })
  );
  log('Namespace created:', namespace, namespace_result);

  // Add lake formation tag
  const command = new AddLFTagsToResourceCommand({
    Resource: { Database: { CatalogId: config.catalogId, Name: namespace } },
    LFTags: [{ TagKey: 'AccessLevel', TagValues: ['Public'] }],
  });
  const response = await clients.lakeFormation.send(command);
  log('add tag response:', response);

  // Create table
  const table_result = await clients.s3Tables.send(
    new CreateTableCommand({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      format: 'ICEBERG',
      metadata: { iceberg: { schema: { fields: schema } } },
    })
  );
  const tableArn = table_result.tableARN;
  log('Table created:', name, table_result);

  return { namespace, name, ...(tableArn && { tableArn }) };
}
