import { strict as assert } from 'node:assert';
import { setTimeout } from 'node:timers/promises';
import { clients, config } from './aws_clients';
import {
  StartQueryExecutionCommand,
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
} from '@aws-sdk/client-athena';

export async function queryRowCount(
  namespace: string,
  name: string,
  whereClause?: string
): Promise<number> {
  const bucketParts = config.tableBucketARN.split('/');
  const bucket = bucketParts[bucketParts.length - 1];
  assert(bucket, 'Could not extract bucket from tableBucketARN');
  const sql = `SELECT COUNT(*) as row_count FROM ${name}${whereClause ? ` WHERE ${whereClause}` : ''}`;

  const { QueryExecutionId } = await clients.athena.send(
    new StartQueryExecutionCommand({
      QueryExecutionContext: {
        Catalog: `s3tablescatalog/${bucket}`,
        Database: namespace,
      },
      QueryString: sql,
      ResultConfiguration: {
        OutputLocation: `s3://${config.outputBucket}/output`,
      },
    })
  );

  let result;
  let status = 'RUNNING';
  while (status === 'RUNNING' || status === 'QUEUED') {
    await setTimeout(200);
    result = await clients.athena.send(
      new GetQueryExecutionCommand({ QueryExecutionId })
    );
    status = result.QueryExecution?.Status?.State ?? 'FAILED';
  }

  if (status === 'SUCCEEDED') {
    const queryResults = await clients.athena.send(
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

export async function executeQuery(namespace: string, sql: string) {
  const bucketParts = config.tableBucketARN.split('/');
  const bucket = bucketParts[bucketParts.length - 1];
  assert(bucket, 'Could not extract bucket from tableBucketARN');

  const { QueryExecutionId } = await clients.athena.send(
    new StartQueryExecutionCommand({
      QueryExecutionContext: {
        Catalog: `s3tablescatalog/${bucket}`,
        Database: namespace,
      },
      QueryString: sql,
      ResultConfiguration: {
        OutputLocation: `s3://${config.outputBucket}/output`,
      },
    })
  );

  let result;
  let status = 'RUNNING';
  while (status === 'RUNNING' || status === 'QUEUED') {
    await setTimeout(200);
    result = await clients.athena.send(
      new GetQueryExecutionCommand({ QueryExecutionId })
    );
    status = result.QueryExecution?.Status?.State ?? 'FAILED';
  }

  if (status === 'SUCCEEDED') {
    const queryResults = await clients.athena.send(
      new GetQueryResultsCommand({ QueryExecutionId })
    );
    return queryResults;
  }
  assert.fail(`Athena query failed with status: ${status}`);
}
