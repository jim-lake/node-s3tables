import { strict as assert } from 'node:assert';
import { setTimeout } from 'node:timers/promises';
import { clients, config } from './aws_clients';
import {
  StartQueryExecutionCommand,
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
} from '@aws-sdk/client-athena';

export async function queryRows<T = Record<string, unknown>>(
  namespace: string,
  name: string,
  whereClause?: string
): Promise<T[]> {
  const bucketParts = config.tableBucketARN.split('/');
  const bucket = bucketParts[bucketParts.length - 1];
  assert(bucket, 'Could not extract bucket from tableBucketARN');
  const sql = `SELECT * FROM ${name}${whereClause ? ` WHERE ${whereClause}` : ''}`;

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
    const allRows = [];
    let nextToken: string | undefined;

    do {
      const queryResults = await clients.athena.send(
        new GetQueryResultsCommand({ QueryExecutionId, NextToken: nextToken })
      );

      const rows = queryResults.ResultSet?.Rows ?? [];
      allRows.push(...rows);
      nextToken = queryResults.NextToken;
    } while (nextToken);

    if (allRows.length === 0) {
      return [];
    }

    const firstRow = allRows[0];
    if (!firstRow?.Data) {
      return [];
    }

    const headers = firstRow.Data.map((col) => col.VarCharValue ?? '');
    return allRows.slice(1).map((row) => {
      const obj: Record<string, unknown> = {};
      row.Data?.forEach((col, i) => {
        const header = headers[i];
        if (header) {
          obj[header] = col.VarCharValue;
        }
      });
      return obj as T;
    });
  }
  const errorReason =
    result?.QueryExecution?.Status?.StateChangeReason ?? 'Unknown';
  assert.fail(
    `Athena query failed with status: ${status}. Reason: ${errorReason}`
  );
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
  const errorReason =
    result?.QueryExecution?.Status?.StateChangeReason ?? 'Unknown';
  assert.fail(
    `Athena query failed with status: ${status}. Reason: ${errorReason}`
  );
}
