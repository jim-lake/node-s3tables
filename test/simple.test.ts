import { test } from 'node:test';
import * as util from 'node:util';

import {
  getMetadata,
  addSchema,
  addPartitionSpec,
  addDataFiles,
  setCurrentCommit,
} from '../src';

const TABLE_ARN = process.env['TABLE_ARN'] as string;
const TABLE_BUCKET_ARN = process.env['TABLE_BUCKET_ARN'] as string;
const NAMESPACE = process.env['TABLE_NAMESPACE'] as string;

void test('read metadata by table arn', async () => {
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
      { id: 5, name: 'details', required: false, type: 'string' as const },
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
    specId: 4,
    fields: [
      {
        'field-id': 1004,
        name: 'sale_datetime_day',
        'source-id': 6,
        transform: 'day' as const,
      },
    ],
  };
  const result = await addPartitionSpec(opts);
  console.log('result:', util.inspect(result, { depth: 99 }));
});
void test('read metadata after partition spec', async () => {
  const result = await getMetadata({ tableArn: TABLE_ARN });
  console.log('result:', util.inspect(result, { depth: 99 }));
});

void test('add parquet file', async () => {
  const opts = {
    tableBucketARN: TABLE_BUCKET_ARN,
    namespace: NAMESPACE,
    name: 'daily_sales',
    file: 's3://cd431039-6e5a-4039-5a7wnqq6tdc1agnk98thcbnc1fq6rusw2b--table-s3/data/ABCDEFG/1234-new.parquet',
    schemaId: 3,
    specId: 4,
    partitions: { sale_datetime_day: '2025-10-01' },
    recordCount: 1n,
    fileSize: 1119n,
  };
  const result = await addDataFiles(opts);
  console.log('result:', util.inspect(result, { depth: 99 }));
});
void test('set current commit', async () => {
  const opts = {
    tableBucketARN: TABLE_BUCKET_ARN,
    namespace: NAMESPACE,
    name: 'daily_sales',
    snapshotId: 4183020680887155442n,
  };
  const result = await setCurrentCommit(opts);
  console.log('result:', util.inspect(result, { depth: 99 }));
});
