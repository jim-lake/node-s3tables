import { test } from 'node:test';
import * as util from 'node:util';

import { getMetadata, addSchema, addPartitionSpec } from '../src';

const TABLE_ARN =
  'arn:aws:s3tables:us-west-2:298262171104:bucket/dc-test-table-bucket/table/cd431039-6e5a-4039-a9c8-e47f24b53440';
const TABLE_BUCKET_ARN =
  'arn:aws:s3tables:us-west-2:298262171104:bucket/dc-test-table-bucket';
const NAMESPACE = 'dc_test_table_bucket_namespace';

void test('get metadata by table arn', async () => {
  const result = await getMetadata({ tableArn: TABLE_ARN });
  console.log('result:', util.inspect(result, { depth: 99 }));
});
void test('update table schema', async () => {
  const opts = {
    tableBucketARN: TABLE_BUCKET_ARN,
    namespace: NAMESPACE,
    name: 'daily_sales',
    schemaId: 2,
    fields: [
      { id: 1, name: 'sale_date', required: false, type: 'date' as const },
      {
        id: 2,
        name: 'product_category',
        required: false,
        type: 'string' as const,
      },
      { id: 3, name: 'sales_amount', required: false, type: 'double' as const },
      {
        id: 4,
        name: 'line_description',
        required: false,
        type: 'string' as const,
      },
      {
        id: 5,
        name: 'details',
        required: false,
        type: 'string' as const,
      },
    ],
  };
  const result = await addSchema(opts);
  console.log('result:', util.inspect(result, { depth: 99 }));
});
void test('get metadata after schema', async () => {
  const result = await getMetadata({ tableArn: TABLE_ARN });
  console.log('result:', util.inspect(result, { depth: 99 }));
});

void test('update table partition spec', async () => {
  const opts = {
    tableBucketARN: TABLE_BUCKET_ARN,
    namespace: NAMESPACE,
    name: 'daily_sales',
    specId: 1,
    fields: [
      {
        'field-id': 1000,
        name: 'sale_date',
        'source-id': 1,
        transform: 'day' as const,
      },
      {
        'field-id': 1001,
        name: 'product_category',
        'source-id': 2,
        transform: 'identity' as const,
      },
    ],
  };
  const result = await addPartitionSpec(opts);
  console.log('result:', util.inspect(result, { depth: 99 }));
});
void test('get metadata after spec', async () => {
  const result = await getMetadata({ tableArn: TABLE_ARN });
  console.log('result:', util.inspect(result, { depth: 99 }));
});
