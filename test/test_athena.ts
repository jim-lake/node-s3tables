import {
  AthenaClient,
  StartQueryExecutionCommand,
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
} from '@aws-sdk/client-athena';
import { setTimeout } from 'node:timers/promises';

const TABLE_BUCKET_ARN = process.env['TABLE_BUCKET_ARN'] as string;
const OUTPUT_BUCKET = process.env['OUTPUT_BUCKET'] as string;

const bucket = TABLE_BUCKET_ARN.split('/').slice(-1)[0];
const client = new AthenaClient({});
const namespace = process.argv[2];
const sql = process.argv[3];

if (!namespace || !sql) {
  console.error('Usage: tsxtest_athena.ts <namespace> "SELECT * FROM table"');
  process.exit(1);
}
if (!bucket) {
  console.error(
    'table bucket not found, make sure TABLE_BUCKET_ARN is set in the env'
  );
}
if (!OUTPUT_BUCKET) {
  console.error(
    'output bucket not found, make sure OUTPUT_BUCKET is set in the env'
  );
}

async function runQuery() {
  const { QueryExecutionId } = await client.send(
    new StartQueryExecutionCommand({
      QueryExecutionContext: {
        Catalog: `s3tablescatalog/${bucket}`,
        Database: namespace,
      },
      QueryString: sql,
      ResultConfiguration: { OutputLocation: `s3://${OUTPUT_BUCKET}/output` },
    })
  );

  let result;
  let status = 'RUNNING';
  while (status === 'RUNNING' || status === 'QUEUED') {
    await setTimeout(200);
    result = await client.send(
      new GetQueryExecutionCommand({ QueryExecutionId })
    );
    status = result.QueryExecution?.Status?.State!;
  }

  if (status === 'SUCCEEDED') {
    const { ResultSet } = await client.send(
      new GetQueryResultsCommand({ QueryExecutionId })
    );
    console.log(JSON.stringify(ResultSet, null, 2));
  } else {
    console.error('Query failed:', status, result);
  }
}

runQuery().catch(console.error);
