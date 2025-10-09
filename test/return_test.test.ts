import { test } from './helpers/test_helper';
import { strict as assert } from 'node:assert';
import { config } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';

import { addSchema, addPartitionSpec, getMetadata } from '../src';

void test('return value verification test', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_return',
    'test_table_return',
    [{ name: 'id', type: 'long', required: true }]
  );

  await t.test('addSchema return properties', async () => {
    const result = await addSchema({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      schemaId: 1,
      fields: [
        { id: 1, name: 'id', type: 'long' as const, required: true },
        { id: 2, name: 'name', type: 'string' as const, required: false },
      ],
    });

    assert(typeof result === 'object', 'Result should be an object');
    assert('metadata' in result, 'Result should have metadata property');
    assert(
      'metadata-location' in result,
      'Result should have metadata-location property'
    );
    assert(
      typeof result['metadata-location'] === 'string',
      'metadata-location should be a string'
    );
    assert(
      result['metadata-location'].startsWith('s3://'),
      'metadata-location should be an S3 URL'
    );

    const { metadata } = result;
    assert(typeof metadata === 'object', 'metadata should be an object');
    assert(
      typeof metadata['last-column-id'] === 'number',
      'last-column-id should be a number'
    );
    assert(
      typeof metadata['current-schema-id'] === 'number',
      'current-schema-id should be a number'
    );
    assert(Array.isArray(metadata.schemas), 'schemas should be an array');
    assert(Array.isArray(metadata.snapshots), 'snapshots should be an array');
    assert(
      typeof metadata['default-spec-id'] === 'number',
      'default-spec-id should be a number'
    );
    assert(
      Array.isArray(metadata['partition-specs']),
      'partition-specs should be an array'
    );
    assert(
      typeof metadata['last-partition-id'] === 'number',
      'last-partition-id should be a number'
    );
    assert(
      typeof metadata.location === 'string',
      'location should be a string'
    );
    assert(
      typeof metadata['current-snapshot-id'] === 'number' ||
        typeof metadata['current-snapshot-id'] === 'bigint',
      'current-snapshot-id should be number or bigint'
    );
    assert(
      metadata['current-snapshot-id'] === -1 ||
        metadata['current-snapshot-id'] > 0,
      'current-snapshot-id should be -1 or positive'
    );

    assert(
      metadata['current-schema-id'] === 1,
      'current-schema-id should be 1'
    );
    assert(metadata['last-column-id'] === 2, 'last-column-id should be 2');
  });

  await t.test('addPartitionSpec return properties', async () => {
    const result = await addPartitionSpec({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      specId: 1,
      fields: [
        {
          'field-id': 1000,
          name: 'id_bucket',
          'source-id': 1,
          transform: 'bucket[10]',
        },
      ],
    });

    assert(typeof result === 'object', 'Result should be an object');
    assert('metadata' in result, 'Result should have metadata property');
    assert(
      'metadata-location' in result,
      'Result should have metadata-location property'
    );
    assert(
      typeof result['metadata-location'] === 'string',
      'metadata-location should be a string'
    );
    assert(
      result['metadata-location'].startsWith('s3://'),
      'metadata-location should be an S3 URL'
    );

    const { metadata } = result;
    assert(typeof metadata === 'object', 'metadata should be an object');
    assert(
      typeof metadata['last-column-id'] === 'number',
      'last-column-id should be a number'
    );
    assert(
      typeof metadata['current-schema-id'] === 'number',
      'current-schema-id should be a number'
    );
    assert(Array.isArray(metadata.schemas), 'schemas should be an array');
    assert(Array.isArray(metadata.snapshots), 'snapshots should be an array');
    assert(
      typeof metadata['default-spec-id'] === 'number',
      'default-spec-id should be a number'
    );
    assert(
      Array.isArray(metadata['partition-specs']),
      'partition-specs should be an array'
    );
    assert(
      typeof metadata['last-partition-id'] === 'number',
      'last-partition-id should be a number'
    );
    assert(
      typeof metadata.location === 'string',
      'location should be a string'
    );
    assert(
      typeof metadata['current-snapshot-id'] === 'number' ||
        typeof metadata['current-snapshot-id'] === 'bigint',
      'current-snapshot-id should be number or bigint'
    );
    assert(
      metadata['current-snapshot-id'] === -1 ||
        metadata['current-snapshot-id'] > 0,
      'current-snapshot-id should be -1 or positive'
    );

    assert(metadata['default-spec-id'] === 1, 'default-spec-id should be 1');
    assert(
      metadata['last-partition-id'] === 1000,
      'last-partition-id should be 1000'
    );
  });

  await t.test('getMetadata return properties', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });

    assert(typeof metadata === 'object', 'metadata should be an object');
    assert(
      typeof metadata['last-column-id'] === 'number',
      'last-column-id should be a number'
    );
    assert(
      typeof metadata['current-schema-id'] === 'number',
      'current-schema-id should be a number'
    );
    assert(Array.isArray(metadata.schemas), 'schemas should be an array');
    assert(Array.isArray(metadata.snapshots), 'snapshots should be an array');
    assert(
      typeof metadata['default-spec-id'] === 'number',
      'default-spec-id should be a number'
    );
    assert(
      Array.isArray(metadata['partition-specs']),
      'partition-specs should be an array'
    );
    assert(
      typeof metadata['last-partition-id'] === 'number',
      'last-partition-id should be a number'
    );
    assert(
      typeof metadata['current-snapshot-id'] === 'number' ||
        typeof metadata['current-snapshot-id'] === 'bigint',
      'current-snapshot-id should be number or bigint'
    );
    assert(
      metadata['current-snapshot-id'] === -1 ||
        metadata['current-snapshot-id'] > 0,
      'current-snapshot-id should be -1 or positive'
    );
    assert(
      typeof metadata.location === 'string',
      'location should be a string'
    );
  });
});
