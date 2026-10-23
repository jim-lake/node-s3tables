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
  schema: { id?: number; name: string; type: string; required: boolean }[],
  properties?: Record<string, string>
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

  /*
   * Add lake formation tag.
   * This grants Athena query permissions via the `AccessLevel: Public` LF-tag.
   * It requires the caller to be a Lake Formation admin and for the LF-tag to
   * exist. When the namespace already inherits IAM_ALLOWED_PRINCIPALS -> ALL
   * (the Lake Formation default), the tag is not required for queries to work,
   * so a missing LF-tag or insufficient LF permissions is tolerated here.
   * Set SKIP_LF_TAG=1 to skip this step entirely.
   */
  if (process.env['SKIP_LF_TAG']) {
    log('add LF tag skipped: SKIP_LF_TAG set');
  } else {
    try {
      const command = new AddLFTagsToResourceCommand({
        Resource: {
          Database: { CatalogId: config.catalogId, Name: namespace },
        },
        LFTags: [{ TagKey: 'AccessLevel', TagValues: ['Public'] }],
      });
      const response = await clients.lakeFormation.send(command);
      log('add tag response:', response);
    } catch (error) {
      log('add LF tag skipped (non-fatal):', (error as Error).message);
    }
  }

  // Create table
  const table_result = await clients.s3Tables.send(
    new CreateTableCommand({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      format: 'ICEBERG',
      metadata: {
        iceberg: {
          schema: { fields: schema },
          /*
           * Optional Iceberg table properties (for example snapshot-retention
           * settings like `history.expire.max-snapshot-age-ms`). Set at create
           * time via the S3 Tables CreateTable metadata so the table is created
           * with them in one call.
           */
          ...(properties && { properties }),
        },
      },
    })
  );
  const tableArn = table_result.tableARN;
  log('Table created:', name, table_result);

  return { namespace, name, ...(tableArn && { tableArn }) };
}
