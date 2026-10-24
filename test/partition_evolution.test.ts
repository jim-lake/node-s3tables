import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { strict as assert } from 'node:assert';
import { inspect } from 'node:util';
import { PassThrough } from 'node:stream';
import { config, clients } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';
import { queryRows } from './helpers/athena_helper';
import { PutObjectCommand } from '@aws-sdk/client-s3';
import { ParquetWriter, ParquetSchema } from '@jim-lake/parquetjs';

import {
  getMetadata,
  addPartitionSpec,
  setDefaultSpec,
  addDataFiles,
} from '../src';

import type { IcebergPartitionField } from '../src';

const SCHEMA = [
  { name: 'app', type: 'string', required: true },
  { name: 'event_datetime', type: 'timestamp', required: false },
  { name: 'test_double', type: 'double', required: false },
];

const parquetSchema = new ParquetSchema({
  app: { type: 'UTF8' },
  event_datetime: { type: 'TIMESTAMP_MILLIS' },
  test_double: { type: 'DOUBLE' },
});

interface Row {
  app: string;
  event_datetime?: string;
  test_double?: string;
}

async function getTableBucket(
  namespace: string,
  name: string
): Promise<string> {
  const metadata = await getMetadata({
    tableBucketARN: config.tableBucketARN,
    namespace,
    name,
  });
  const parts = metadata.location.split('/');
  const bucket = parts[parts.length - 1];
  assert(bucket, 'Could not extract table bucket from metadata location');
  return bucket;
}

function specFieldNames(
  metadata: {
    'partition-specs': { 'spec-id': number; fields: { name: string }[] }[];
  },
  specId: number
): string[] {
  const spec = metadata['partition-specs'].find((s) => s['spec-id'] === specId);
  assert(spec, `spec ${specId} should exist`);
  return spec.fields.map((f) => f.name);
}

async function findSpecId(
  namespace: string,
  name: string,
  fields: IcebergPartitionField[]
): Promise<number | undefined> {
  const metadata = await getMetadata({
    tableBucketARN: config.tableBucketARN,
    namespace,
    name,
  });
  const match = metadata['partition-specs'].find((spec) => {
    if (spec.fields.length !== fields.length) {
      return false;
    }
    return spec.fields.every((f, i) => {
      const other = fields[i];
      return (
        f.name === other?.name &&
        f['source-id'] === other['source-id'] &&
        f.transform === other.transform
      );
    });
  });
  return match?.['spec-id'];
}

async function addRow(
  namespace: string,
  name: string,
  tableBucket: string,
  app: string,
  partitions: Record<string, string | number>
): Promise<void> {
  const specId = (
    await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    })
  )['default-spec-id'];

  const eventDate = new Date('2025-08-06T12:00:00Z');
  const s3Key = `data/${app}-${Date.now()}.parquet`;

  const stream = new PassThrough();
  const chunks: Buffer[] = [];
  stream.on('data', (chunk: Buffer) => chunks.push(chunk));

  const writer = await ParquetWriter.openStream(parquetSchema, stream);
  await writer.appendRow({ app, event_datetime: eventDate, test_double: 3.5 });
  await writer.close();

  const fileBuffer = Buffer.concat(chunks);
  await clients.s3.send(
    new PutObjectCommand({ Bucket: tableBucket, Key: s3Key, Body: fileBuffer })
  );

  const result = await addDataFiles({
    tableBucketARN: config.tableBucketARN,
    namespace,
    name,
    lists: [
      {
        specId,
        schemaId: 0,
        files: [
          {
            file: `s3://${tableBucket}/${s3Key}`,
            partitions,
            recordCount: 1n,
            fileSize: BigInt(fileBuffer.length),
          },
        ],
      },
    ],
  });
  log(`addDataFiles (spec ${specId}, ${app}) result:`, result);
}

async function assertRowsReadable(
  namespace: string,
  name: string,
  expectedCount: number
): Promise<void> {
  const rows = await queryRows<Row>(namespace, name);
  log('query rows:', inspect(rows, { depth: 99 }));
  assert.equal(rows.length, expectedCount, `should have ${expectedCount} rows`);
  for (const row of rows) {
    assert('app' in row, 'app column should be present');
    assert('event_datetime' in row, 'event_datetime column should be present');
    assert('test_double' in row, 'test_double column should be present');
  }
}

const appSpec: IcebergPartitionField[] = [
  { 'field-id': 1000, name: 'app', 'source-id': 1, transform: 'identity' },
];
const dateSpec: IcebergPartitionField[] = [
  {
    'field-id': 1000,
    name: 'event_datetime_day',
    'source-id': 2,
    transform: 'day',
  },
];

void test('partition evolution: none -> app -> none', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_part_none',
    'test_table_part_none',
    SCHEMA
  );

  await t.test('data while unpartitioned (spec 0)', async () => {
    const tableBucket = await getTableBucket(namespace, name);
    await addRow(namespace, name, tableBucket, 'app-a', {});
    await assertRowsReadable(namespace, name, 1);
  });

  await t.test('partition by app (spec 1)', async () => {
    const result = await addPartitionSpec({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      specId: 1,
      fields: appSpec,
    });
    assert.equal(result.metadata['default-spec-id'], 1);
    const tableBucket = await getTableBucket(namespace, name);
    await addRow(namespace, name, tableBucket, 'app-b', { app: 'app-b' });
    await assertRowsReadable(namespace, name, 2);
  });

  await t.test('revert to unpartitioned (existing spec 0)', async () => {
    const result = await setDefaultSpec({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      specId: 0,
    });
    assert.equal(
      result.metadata['default-spec-id'],
      0,
      'default spec should be the unpartitioned spec 0'
    );
    assert.deepEqual(specFieldNames(result.metadata, 0), []);
    const tableBucket = await getTableBucket(namespace, name);
    await addRow(namespace, name, tableBucket, 'app-c', {});
    await assertRowsReadable(namespace, name, 3);
  });
});

void test('partition evolution: none -> app -> date -> app', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_part_cycle',
    'test_table_part_cycle',
    SCHEMA
  );

  await t.test('partition by app (spec 1)', async () => {
    const result = await addPartitionSpec({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      specId: 1,
      fields: appSpec,
    });
    assert.equal(result.metadata['default-spec-id'], 1);
    const tableBucket = await getTableBucket(namespace, name);
    await addRow(namespace, name, tableBucket, 'app-a', { app: 'app-a' });
    await assertRowsReadable(namespace, name, 1);
  });

  await t.test('partition by event_datetime day (spec 2)', async () => {
    const result = await addPartitionSpec({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      specId: 2,
      fields: dateSpec,
    });
    assert.equal(result.metadata['default-spec-id'], 2);
    const tableBucket = await getTableBucket(namespace, name);
    await addRow(namespace, name, tableBucket, 'app-b', {
      event_datetime_day: '2025-08-06',
    });
    await assertRowsReadable(namespace, name, 2);
  });

  await t.test('revert to partition by app (existing spec 1)', async () => {
    const existing = await findSpecId(namespace, name, appSpec);
    assert(existing !== undefined, 'app spec should already exist');
    assert.equal(existing, 1, 'the app spec should be spec 1');

    const result = await setDefaultSpec({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      specId: existing,
    });
    assert.equal(result.metadata['default-spec-id'], 1);
    assert.deepEqual(specFieldNames(result.metadata, 1), ['app']);
    const tableBucket = await getTableBucket(namespace, name);
    await addRow(namespace, name, tableBucket, 'app-c', { app: 'app-c' });
    await assertRowsReadable(namespace, name, 3);
  });
});

void test('partition evolution: none -> app -> app+date -> double+date', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_part_multi',
    'test_table_part_multi',
    SCHEMA
  );

  const appDateSpec: IcebergPartitionField[] = [
    { 'field-id': 1000, name: 'app', 'source-id': 1, transform: 'identity' },
    {
      'field-id': 1001,
      name: 'event_datetime_day',
      'source-id': 2,
      transform: 'day',
    },
  ];
  const doubleDateSpec: IcebergPartitionField[] = [
    {
      'field-id': 1002,
      name: 'test_double',
      'source-id': 3,
      transform: 'identity',
    },
    {
      'field-id': 1001,
      name: 'event_datetime_day',
      'source-id': 2,
      transform: 'day',
    },
  ];

  await t.test('partition by app (spec 1)', async () => {
    const result = await addPartitionSpec({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      specId: 1,
      fields: appSpec,
    });
    assert.equal(result.metadata['default-spec-id'], 1);
    const tableBucket = await getTableBucket(namespace, name);
    await addRow(namespace, name, tableBucket, 'app-a', { app: 'app-a' });
    await assertRowsReadable(namespace, name, 1);
  });

  await t.test('partition by app + event_datetime day (spec 2)', async () => {
    const result = await addPartitionSpec({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      specId: 2,
      fields: appDateSpec,
    });
    assert.equal(result.metadata['default-spec-id'], 2);
    assert.deepEqual(specFieldNames(result.metadata, 2), [
      'app',
      'event_datetime_day',
    ]);
    const tableBucket = await getTableBucket(namespace, name);
    await addRow(namespace, name, tableBucket, 'app-b', {
      app: 'app-b',
      event_datetime_day: '2025-08-06',
    });
    await assertRowsReadable(namespace, name, 2);
  });

  await t.test(
    'partition by test_double + event_datetime day (spec 3)',
    async () => {
      const result = await addPartitionSpec({
        tableBucketARN: config.tableBucketARN,
        namespace,
        name,
        specId: 3,
        fields: doubleDateSpec,
      });
      assert.equal(result.metadata['default-spec-id'], 3);
      assert.deepEqual(specFieldNames(result.metadata, 3), [
        'test_double',
        'event_datetime_day',
      ]);
      const tableBucket = await getTableBucket(namespace, name);
      await addRow(namespace, name, tableBucket, 'app-c', {
        test_double: 3.5,
        event_datetime_day: '2025-08-06',
      });
      await assertRowsReadable(namespace, name, 3);
    }
  );
});
