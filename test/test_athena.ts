import {
  AthenaClient,
  StartQueryExecutionCommand,
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
} from '@aws-sdk/client-athena';

const client = new AthenaClient({});
const sql = process.argv[2];

if (!sql) {
  console.error('Usage: ts-node test_athena.ts "SELECT * FROM table"');
  process.exit(1);
}

async function runQuery() {
  const { QueryExecutionId } = await client.send(
    new StartQueryExecutionCommand({
      QueryString: sql,
      ResultConfiguration: { OutputLocation: process.env.OUTPUT_BUCKET },
    })
  );

  let status = 'RUNNING';
  while (status === 'RUNNING' || status === 'QUEUED') {
    const { QueryExecution } = await client.send(
      new GetQueryExecutionCommand({ QueryExecutionId })
    );
    status = QueryExecution?.Status?.State!;
    if (status === 'RUNNING' || status === 'QUEUED')
      await new Promise((r) => setTimeout(r, 1000));
  }

  if (status === 'SUCCEEDED') {
    const { ResultSet } = await client.send(
      new GetQueryResultsCommand({ QueryExecutionId })
    );
    console.log(JSON.stringify(ResultSet, null, 2));
  } else {
    console.error('Query failed:', status);
  }
}

runQuery().catch(console.error);
