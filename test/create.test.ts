import { test } from 'node:test';
import { S3TablesClient, CreateNamespaceCommand, CreateTableCommand, DeleteTableCommand, DeleteNamespaceCommand } from '@aws-sdk/client-s3tables';

const TABLE_BUCKET_ARN = process.env['TABLE_BUCKET_ARN'] as string;

const timestamp = Date.now();
const TEMP_NAMESPACE = `test${timestamp}`;
const TEMP_TABLE = `table${timestamp}`;

console.log(`Using temp namespace: ${TEMP_NAMESPACE}`);
console.log(`Using temp table: ${TEMP_TABLE}`);

const client = new S3TablesClient({ region: 'us-west-2' });

let tableArn: string;
let created = { namespace: false, table: false };

async function cleanup() {
  try {
    if (created.table) {
      await client.send(new DeleteTableCommand({
        tableBucketARN: TABLE_BUCKET_ARN,
        namespace: [TEMP_NAMESPACE],
        name: TEMP_TABLE
      }));
      console.log('Table deleted');
    }
    
    if (created.namespace) {
      await client.send(new DeleteNamespaceCommand({
        tableBucketARN: TABLE_BUCKET_ARN,
        namespace: [TEMP_NAMESPACE]
      }));
      console.log('Namespace deleted');
    }
  } catch (error) {
    console.error('Cleanup failed:', error);
  }
}

void test('s3tables test', async () => {
  try {
    // Create namespace
    await client.send(new CreateNamespaceCommand({
      tableBucketARN: TABLE_BUCKET_ARN,
      namespace: [TEMP_NAMESPACE]
    }));
    created.namespace = true;
    console.log('Namespace created');

    // Create table
    const result = await client.send(new CreateTableCommand({
      tableBucketARN: TABLE_BUCKET_ARN,
      namespace: [TEMP_NAMESPACE],
      name: TEMP_TABLE,
      format: 'ICEBERG'
    }));
    
    tableArn = result.tableARN!;
    created.table = true;
    console.log('Table created:', tableArn);

    console.log('Test completed successfully');
    await cleanup();
    
  } catch (error) {
    console.error('Test failed:', error);
    await cleanup();
    throw error;
  }
});
