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

import { getMetadata, addSchema, setCurrentSchema, addDataFiles } from '../src';

import type { IcebergSchemaField } from '../src';

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

async function getCurrentFields(
  namespace: string,
  name: string
): Promise<IcebergSchemaField[]> {
  const metadata = await getMetadata({
    tableBucketARN: config.tableBucketARN,
    namespace,
    name,
  });
  const current = metadata.schemas.find(
    (s) => s['schema-id'] === metadata['current-schema-id']
  );
  assert(current, 'Could not find current schema in metadata');
  return current.fields;
}

function schemaFieldsEqual(
  a: IcebergSchemaField[],
  b: IcebergSchemaField[]
): boolean {
  if (a.length !== b.length) {
    return false;
  }
  return a.every((field, i) => {
    const other = b[i];
    return (
      field.id === other?.id &&
      field.name === other.name &&
      field.required === other.required &&
      JSON.stringify(field.type) === JSON.stringify(other.type)
    );
  });
}

async function findSchemaId(
  namespace: string,
  name: string,
  fields: IcebergSchemaField[]
): Promise<number | undefined> {
  const metadata = await getMetadata({
    tableBucketARN: config.tableBucketARN,
    namespace,
    name,
  });
  const match = metadata.schemas.find((s) =>
    schemaFieldsEqual(s.fields, fields)
  );
  return match?.['schema-id'];
}

interface AppRow {
  app: string;
  event_datetime?: string;
}

async function writeAndAddFile(
  namespace: string,
  name: string,
  tableBucket: string,
  schemaId: number,
  columns: 'app' | 'app_and_datetime'
): Promise<void> {
  const parquetSchema =
    columns === 'app_and_datetime'
      ? new ParquetSchema({
          app: { type: 'UTF8' },
          event_datetime: { type: 'TIMESTAMP_MILLIS' },
        })
      : new ParquetSchema({ app: { type: 'UTF8' } });

  const s3Key = `data/app=test-app/schema${schemaId}-${Date.now()}.parquet`;

  const stream = new PassThrough();
  const chunks: Buffer[] = [];
  stream.on('data', (chunk: Buffer) => chunks.push(chunk));

  const writer = await ParquetWriter.openStream(parquetSchema, stream);
  if (columns === 'app_and_datetime') {
    await writer.appendRow({ app: 'test-app', event_datetime: new Date() });
  } else {
    await writer.appendRow({ app: 'test-app' });
  }
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
        specId: 0,
        schemaId,
        files: [
          {
            file: `s3://${tableBucket}/${s3Key}`,
            partitions: { app: 'test-app' },
            recordCount: 1n,
            fileSize: BigInt(fileBuffer.length),
          },
        ],
      },
    ],
  });
  log(`addDataFiles (schema ${schemaId}) result:`, result);
}

async function writeStringColumns(
  namespace: string,
  name: string,
  tableBucket: string,
  schemaId: number,
  columns: string[]
): Promise<void> {
  const schemaShape: Record<string, { type: 'UTF8' }> = {};
  const row: Record<string, string> = {};
  for (const col of columns) {
    schemaShape[col] = { type: 'UTF8' };
    row[col] = `${col}-val`;
  }
  const parquetSchema = new ParquetSchema(schemaShape);

  const partitionKey = columns[0];
  if (!partitionKey) {
    throw new Error('at least one column is required');
  }
  const partitionValue = `${partitionKey}-val`;
  const s3Key = `data/${partitionKey}=${partitionValue}/schema${schemaId}-${Date.now()}.parquet`;

  const stream = new PassThrough();
  const chunks: Buffer[] = [];
  stream.on('data', (chunk: Buffer) => chunks.push(chunk));

  const writer = await ParquetWriter.openStream(parquetSchema, stream);
  await writer.appendRow(row);
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
        specId: 0,
        schemaId,
        files: [
          {
            file: `s3://${tableBucket}/${s3Key}`,
            partitions: { [partitionKey]: partitionValue },
            recordCount: 1n,
            fileSize: BigInt(fileBuffer.length),
          },
        ],
      },
    ],
  });
  log(`addDataFiles (schema ${schemaId}, [${columns.join()}]) result:`, result);
}

void test('schema evolution: add column then delete column', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_schema_evolution',
    'test_table_schema_evolution',
    [{ name: 'app', type: 'string', required: true }]
  );

  await t.test('add column: schema 1 = app + event_datetime', async () => {
    const current = await getCurrentFields(namespace, name);
    assert.deepEqual(
      current.map((f) => f.name),
      ['app'],
      'initial schema should contain only `app`'
    );

    const fields: IcebergSchemaField[] = [
      ...current,
      {
        id: 2,
        name: 'event_datetime',
        type: 'timestamp',
        // added columns on an existing table must be optional
        required: false,
      },
    ];

    const add_result = await addSchema({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      schemaId: 1,
      fields,
    });
    log('addSchema (add column) result:', add_result);

    assert.equal(
      add_result.metadata['current-schema-id'],
      1,
      'current-schema-id should be 1 after adding a column'
    );
    const schema1 = add_result.metadata.schemas.find(
      (s) => s['schema-id'] === 1
    );
    assert(schema1, 'schema 1 should exist');
    assert.deepEqual(
      schema1.fields.map((f) => f.name),
      ['app', 'event_datetime'],
      'schema 1 should contain app and event_datetime'
    );
  });

  await t.test('add data file and query under schema 1', async () => {
    const tableBucket = await getTableBucket(namespace, name);
    await writeAndAddFile(namespace, name, tableBucket, 1, 'app_and_datetime');

    const rows = await queryRows<AppRow>(namespace, name);
    log('rows after add column:', inspect(rows, { depth: 99 }));

    assert(rows.length >= 1, 'should return at least one row');
    const row = rows[0];
    assert(row, 'expected a row');
    assert(
      'event_datetime' in row,
      'query result should include the new event_datetime column'
    );
    assert.equal(row.app, 'test-app', 'app column should be populated');
    assert(
      row.event_datetime,
      'event_datetime should have a value in the new row'
    );
  });

  await t.test('delete column: revert to app-only schema', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const current = metadata.schemas.find(
      (s) => s['schema-id'] === metadata['current-schema-id']
    );
    assert(current, 'current schema should exist');
    assert.deepEqual(
      current.fields.map((f) => f.name),
      ['app', 'event_datetime'],
      'schema before delete should contain app and event_datetime'
    );

    const fields = current.fields.filter((f) => f.name !== 'event_datetime');
    // the post-drop schema equals the initial `[app]` schema already in history
    const targetId = await findSchemaId(namespace, name, fields);
    assert(
      targetId !== undefined,
      'app-only schema should already exist in history'
    );
    assert.notEqual(
      targetId,
      metadata['current-schema-id'],
      'target schema should differ from the current one'
    );

    const result = await setCurrentSchema({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      schemaId: targetId,
    });
    log('setCurrentSchema (delete column) result:', result);

    assert.equal(
      result.metadata['current-schema-id'],
      targetId,
      'current-schema-id should be the app-only schema id'
    );
    const resulting = result.metadata.schemas.find(
      (s) => s['schema-id'] === targetId
    );
    assert(resulting, 'resulting current schema should exist');
    assert.deepEqual(
      resulting.fields.map((f) => f.name),
      ['app'],
      'current schema should contain only app after deletion'
    );
  });

  await t.test('add data file and query after deletion', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const schemaId = metadata['current-schema-id'];
    const tableBucket = await getTableBucket(namespace, name);
    await writeAndAddFile(namespace, name, tableBucket, schemaId, 'app');

    const rows = await queryRows<AppRow>(namespace, name);
    log('rows after delete column:', inspect(rows, { depth: 99 }));

    assert(rows.length >= 1, 'should return at least one row');
    for (const row of rows) {
      assert(
        !('event_datetime' in row),
        'event_datetime column should no longer be present after deletion'
      );
      assert.equal(row.app, 'test-app', 'app column should still be populated');
    }
  });
});

void test('schema evolution: start 1 column, add 2, remove 1', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_schema_evolution_add2',
    'test_table_add2_remove1',
    [{ name: 'a', type: 'string', required: true }]
  );

  await t.test('add two columns: a + b + c', async () => {
    const result = await addSchema({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      schemaId: 1,
      fields: [
        { id: 1, name: 'a', type: 'string', required: true },
        { id: 2, name: 'b', type: 'string', required: false },
        { id: 3, name: 'c', type: 'string', required: false },
      ],
    });
    const current = result.metadata.schemas.find(
      (s) => s['schema-id'] === result.metadata['current-schema-id']
    );
    assert(current, 'current schema should exist');
    assert.deepEqual(
      current.fields.map((f) => f.name),
      ['a', 'b', 'c'],
      'schema should contain a, b, c after adding two columns'
    );
  });

  await t.test('data file and query with a, b, c', async () => {
    const tableBucket = await getTableBucket(namespace, name);
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    await writeStringColumns(
      namespace,
      name,
      tableBucket,
      metadata['current-schema-id'],
      ['a', 'b', 'c']
    );
    const rows = await queryRows<Record<string, string>>(namespace, name);
    log('rows after add 2:', inspect(rows, { depth: 99 }));
    const row = rows[0];
    assert(row, 'expected a row');
    assert('a' in row && 'b' in row && 'c' in row, 'a, b, c should be present');
  });

  await t.test('remove column c: a + b', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const current = metadata.schemas.find(
      (s) => s['schema-id'] === metadata['current-schema-id']
    );
    assert(current, 'current schema should exist');
    const fields = current.fields.filter((f) => f.name !== 'c');

    const result = await addSchema({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      schemaId: 2,
      fields,
    });
    const resultingId = result.metadata['current-schema-id'];
    const resulting = result.metadata.schemas.find(
      (s) => s['schema-id'] === resultingId
    );
    assert(resulting, 'resulting schema should exist');
    assert.deepEqual(
      resulting.fields.map((f) => f.name),
      ['a', 'b'],
      'schema should contain a, b after removing c'
    );
  });

  await t.test('data file and query with a, b (c gone)', async () => {
    const tableBucket = await getTableBucket(namespace, name);
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    await writeStringColumns(
      namespace,
      name,
      tableBucket,
      metadata['current-schema-id'],
      ['a', 'b']
    );
    const rows = await queryRows<Record<string, string>>(namespace, name);
    log('rows after remove c:', inspect(rows, { depth: 99 }));
    assert(rows.length >= 1, 'should return at least one row');
    for (const row of rows) {
      assert(!('c' in row), 'column c should be gone after deletion');
      assert('a' in row && 'b' in row, 'a and b should still be present');
    }
  });
});

void test('schema evolution: start 2 columns, add 1, remove an original', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_schema_evolution_orig',
    'test_table_add1_remove_orig',
    [
      { name: 'a', type: 'string', required: true },
      { name: 'b', type: 'string', required: false },
    ]
  );

  await t.test('add one column: a + b + c', async () => {
    const result = await addSchema({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      schemaId: 1,
      fields: [
        { id: 1, name: 'a', type: 'string', required: true },
        { id: 2, name: 'b', type: 'string', required: false },
        { id: 3, name: 'c', type: 'string', required: false },
      ],
    });
    const current = result.metadata.schemas.find(
      (s) => s['schema-id'] === result.metadata['current-schema-id']
    );
    assert(current, 'current schema should exist');
    assert.deepEqual(
      current.fields.map((f) => f.name),
      ['a', 'b', 'c'],
      'schema should contain a, b, c after adding one column'
    );
  });

  await t.test('data file and query with a, b, c', async () => {
    const tableBucket = await getTableBucket(namespace, name);
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    await writeStringColumns(
      namespace,
      name,
      tableBucket,
      metadata['current-schema-id'],
      ['a', 'b', 'c']
    );
    const rows = await queryRows<Record<string, string>>(namespace, name);
    log('rows after add 1:', inspect(rows, { depth: 99 }));
    const row = rows[0];
    assert(row, 'expected a row');
    assert('a' in row && 'b' in row && 'c' in row, 'a, b, c should be present');
  });

  await t.test('remove original column a: b + c', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const current = metadata.schemas.find(
      (s) => s['schema-id'] === metadata['current-schema-id']
    );
    assert(current, 'current schema should exist');
    // dropping required original column `a` (id 1) leaves b + c
    const fields = current.fields.filter((f) => f.name !== 'a');

    const result = await addSchema({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      schemaId: 2,
      fields,
    });
    const resultingId = result.metadata['current-schema-id'];
    const resulting = result.metadata.schemas.find(
      (s) => s['schema-id'] === resultingId
    );
    assert(resulting, 'resulting schema should exist');
    assert.deepEqual(
      resulting.fields.map((f) => f.name),
      ['b', 'c'],
      'schema should contain b, c after removing original a'
    );
  });

  await t.test('data file and query with b, c (a gone)', async () => {
    const tableBucket = await getTableBucket(namespace, name);
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    await writeStringColumns(
      namespace,
      name,
      tableBucket,
      metadata['current-schema-id'],
      ['b', 'c']
    );
    const rows = await queryRows<Record<string, string>>(namespace, name);
    log('rows after remove a:', inspect(rows, { depth: 99 }));
    assert(rows.length >= 1, 'should return at least one row');
    for (const row of rows) {
      assert(!('a' in row), 'original column a should be gone after deletion');
      assert('b' in row && 'c' in row, 'b and c should still be present');
    }
  });
});
