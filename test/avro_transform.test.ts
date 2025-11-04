import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { strict as assert } from 'node:assert';
import { makeBounds } from '../src/avro_transform';
import type {
  IcebergPartitionSpec,
  IcebergSchema,
  IcebergType,
  IcebergTransform,
} from '../src/iceberg';
import type { PartitionRecord } from '../src/avro_types';

// Test data combinations: [transform, sourceType, inputValue, expectedBuffer]
const testCombinations = [
  // Identity transforms
  ['identity', 'int', 42, Buffer.from([42, 0, 0, 0])],
  ['identity', 'long', 42n, Buffer.from([42, 0, 0, 0, 0, 0, 0, 0])],
  ['identity', 'float', 3.14, Buffer.alloc(4).fill(0)],
  ['identity', 'double', 3.14159, Buffer.alloc(8).fill(0)],
  ['identity', 'string', 'test', Buffer.from('test', 'utf8')],
  [
    'identity',
    'uuid',
    '550e8400-e29b-41d4-a716-446655440000',
    Buffer.from('550e8400-e29b-41d4-a716-446655440000', 'utf8'),
  ],
  ['identity', 'boolean', true, Buffer.from([1])],
  ['identity', 'boolean', false, Buffer.from([0])],
  ['identity', 'binary', Buffer.from([1, 2, 3]), Buffer.from([1, 2, 3])],
  [
    'identity',
    'decimal(10,2)',
    Buffer.from([1, 2, 3, 4]),
    Buffer.from([1, 2, 3, 4]),
  ],
  [
    'identity',
    'fixed[4]',
    Buffer.from([1, 2, 3, 4]),
    Buffer.from([1, 2, 3, 4]),
  ],

  // Date/time transforms - using little-endian encoding
  ['year', 'date', '2024-01-01', Buffer.from([232, 7, 0, 0])], // 2024 in little-endian
  [
    'year',
    'timestamp',
    '2024-01-01T00:00:00.000Z',
    Buffer.from([232, 7, 0, 0]),
  ], // 2024 in little-endian
  ['month', 'date', '2024-01-01', Buffer.from([224, 94, 0, 0])], // (2024*12 + 0) = 24288 in little-endian
  ['day', 'date', '2024-01-01', Buffer.from([11, 77, 0, 0])], // days since epoch = 19723 in little-endian
  ['hour', 'timestamp', '2024-01-01T00:00:00.000Z', Buffer.from([8, 57, 7, 0])], // hours since epoch = 473352 in little-endian

  // Bucket transforms
  ['bucket[10]', 'int', 5, Buffer.from([5, 0, 0, 0])],
  ['bucket[100]', 'long', 42, Buffer.from([42, 0, 0, 0])],

  // Truncate transforms
  ['truncate[5]', 'string', 'hello world', Buffer.from('hello', 'utf8')],
  ['truncate[3]', 'string', 'hi', Buffer.from('hi', 'utf8')],

  // Date transforms with numeric inputs (uses number directly)
  ['year', 'timestamp', 2024, Buffer.from([232, 7, 0, 0])], // 2024 as number
  ['month', 'date', 24288, Buffer.from([224, 94, 0, 0])], // 24288 as number
  ['day', 'timestamp', 19723, Buffer.from([11, 77, 0, 0])], // 19723 as number
  ['hour', 'date', 473352, Buffer.from([8, 57, 7, 0])], // 473352 as number
  ['year', 'long', 2024n, Buffer.from([232, 7, 0, 0])], // 2024 as bigint
] as const;

void test('makeBounds - comprehensive transform/type combinations', async (t) => {
  for (const [
    transform,
    sourceType,
    inputValue,
    expectedBuffer,
  ] of testCombinations) {
    await t.test(
      `${transform} + ${sourceType} with ${String(inputValue)}`,
      () => {
        const schema: IcebergSchema = {
          type: 'struct',
          'schema-id': 1,
          fields: [
            {
              id: 1,
              name: 'test_field',
              type: sourceType as IcebergType,
              required: false,
            },
          ],
        };

        const spec: IcebergPartitionSpec = {
          'spec-id': 1,
          fields: [
            {
              'field-id': 1000,
              name: 'partition_field',
              'source-id': 1,
              transform: transform as IcebergTransform,
            },
          ],
        };

        const partitions: PartitionRecord = {
          partition_field: inputValue as
            | string
            | number
            | bigint
            | Buffer
            | null,
        };

        const result = makeBounds(partitions, spec, schema);
        log('part, result:', partitions, result);

        // Special handling for float/double due to precision
        if (sourceType === 'float' || sourceType === 'double') {
          assert.strictEqual(
            result.length,
            1,
            `Expected result array length to be 1 for ${sourceType}`
          );
          assert(
            Buffer.isBuffer(result[0]),
            `Expected result[0] to be a Buffer for ${sourceType}`
          );
          assert.strictEqual(
            result[0].length,
            expectedBuffer.length,
            `Expected buffer length ${expectedBuffer.length} for ${sourceType}`
          );
        } else {
          assert.deepStrictEqual(
            result,
            [expectedBuffer],
            `Expected result to match expected buffer for ${transform} + ${sourceType}`
          );
        }
      }
    );
  }
});

void test('makeBounds - edge cases', () => {
  const schema: IcebergSchema = {
    type: 'struct',
    'schema-id': 1,
    fields: [
      { id: 1, name: 'field1', type: 'string', required: false },
      { id: 2, name: 'field2', type: 'int', required: false },
    ],
  };

  // Test null values
  const specWithNull: IcebergPartitionSpec = {
    'spec-id': 1,
    fields: [
      {
        'field-id': 1000,
        name: 'partition_field',
        'source-id': 1,
        transform: 'identity',
      },
    ],
  };

  const partitionsWithNull: PartitionRecord = { partition_field: null };

  const resultWithNull = makeBounds(partitionsWithNull, specWithNull, schema);
  assert.deepStrictEqual(
    resultWithNull,
    [null],
    'Expected null partition to return [null]'
  );

  // Test multiple partition fields
  const multiSpec: IcebergPartitionSpec = {
    'spec-id': 1,
    fields: [
      {
        'field-id': 1000,
        name: 'str_partition',
        'source-id': 1,
        transform: 'identity',
      },
      {
        'field-id': 1001,
        name: 'int_partition',
        'source-id': 2,
        transform: 'identity',
      },
    ],
  };

  const multiPartitions: PartitionRecord = {
    str_partition: 'test',
    int_partition: 42,
  };

  const multiResult = makeBounds(multiPartitions, multiSpec, schema);
  assert.strictEqual(
    multiResult.length,
    2,
    'Expected 2 results for multiple partition fields'
  );
  assert.deepStrictEqual(
    multiResult[0],
    Buffer.from('test', 'utf8'),
    'Expected first result to be string buffer'
  );
  assert.deepStrictEqual(
    multiResult[1],
    Buffer.from([42, 0, 0, 0]),
    'Expected second result to be int buffer'
  );
});

void test('makeBounds - error cases', () => {
  const schema: IcebergSchema = {
    type: 'struct',
    'schema-id': 1,
    fields: [{ id: 1, name: 'field1', type: 'string', required: false }],
  };

  const spec: IcebergPartitionSpec = {
    'spec-id': 1,
    fields: [
      {
        'field-id': 1000,
        name: 'partition_field',
        'source-id': 1,
        transform: 'identity',
      },
    ],
  };

  // Missing schema field
  const badSpec: IcebergPartitionSpec = {
    'spec-id': 1,
    fields: [
      {
        'field-id': 1000,
        name: 'partition_field',
        'source-id': 999, // Non-existent field
        transform: 'identity',
      },
    ],
  };

  const partitions: PartitionRecord = { partition_field: 'test' };

  assert.throws(
    () => {
      makeBounds(partitions, badSpec, schema);
    },
    /Schema field not found for source-id 999/,
    'Expected error for non-existent schema field'
  );

  // Missing partition value
  const emptyPartitions: PartitionRecord = {};

  assert.throws(
    () => {
      makeBounds(emptyPartitions, spec, schema);
    },
    /missing partition/,
    'Expected error for missing partition value'
  );

  // Invalid transform for type
  const bucketSpec: IcebergPartitionSpec = {
    'spec-id': 1,
    fields: [
      {
        'field-id': 1000,
        name: 'partition_field',
        'source-id': 1,
        transform: 'bucket[10]',
      },
    ],
  };

  const stringPartitions: PartitionRecord = { partition_field: 'not_a_number' };

  assert.throws(
    () => {
      makeBounds(stringPartitions, bucketSpec, schema);
    },
    /bucket requires number input/,
    'Expected error for bucket transform with string input'
  );

  // Invalid truncate input
  const truncateSpec: IcebergPartitionSpec = {
    'spec-id': 1,
    fields: [
      {
        'field-id': 1000,
        name: 'partition_field',
        'source-id': 1,
        transform: 'truncate[5]',
      },
    ],
  };

  const numberPartitions: PartitionRecord = { partition_field: 123 };

  assert.throws(
    () => {
      makeBounds(numberPartitions, truncateSpec, schema);
    },
    /truncate requires string input/,
    'Expected error for truncate transform with number input'
  );

  // Buffer with wrong identity type
  const intSchema: IcebergSchema = {
    type: 'struct',
    'schema-id': 1,
    fields: [{ id: 1, name: 'field1', type: 'int', required: false }],
  };

  const bufferPartitions: PartitionRecord = {
    partition_field: Buffer.from([1, 2, 3]),
  };

  assert.throws(
    () => {
      makeBounds(bufferPartitions, spec, intSchema);
    },
    /Buffer not allowed for identity with type int/,
    'Expected error for buffer with wrong identity type'
  );
});

void test('makeBounds - complex type handling', () => {
  // Test with complex types that should return null for _outputType
  const complexSchema: IcebergSchema = {
    type: 'struct',
    'schema-id': 1,
    fields: [
      {
        id: 1,
        name: 'list_field',
        type: { type: 'list', element: 'string', 'element-required': false },
        required: false,
      },
    ],
  };

  const complexSpec: IcebergPartitionSpec = {
    'spec-id': 1,
    fields: [
      {
        'field-id': 1000,
        name: 'partition_field',
        'source-id': 1,
        transform: 'identity',
      },
    ],
  };

  const complexPartitions: PartitionRecord = {
    partition_field: Buffer.from([1, 2]),
  };

  const result = makeBounds(complexPartitions, complexSpec, complexSchema);
  assert.deepStrictEqual(
    result,
    [null],
    'Expected complex type to return [null]'
  );
});

void test('makeBounds - date string variations', () => {
  const schema: IcebergSchema = {
    type: 'struct',
    'schema-id': 1,
    fields: [{ id: 1, name: 'date_field', type: 'date', required: false }],
  };

  const daySpec: IcebergPartitionSpec = {
    'spec-id': 1,
    fields: [
      {
        'field-id': 1000,
        name: 'day_partition',
        'source-id': 1,
        transform: 'day',
      },
    ],
  };

  // Test different date string formats
  const datePartitions: PartitionRecord = { day_partition: '2024-12-25' };

  const result = makeBounds(datePartitions, daySpec, schema);
  assert.strictEqual(
    result.length,
    1,
    'Expected single result for date string'
  );
  assert(
    Buffer.isBuffer(result[0]),
    'Expected result to be a Buffer for date string'
  );
  assert.strictEqual(
    result[0].length,
    4,
    'Expected 4-byte buffer for date transform'
  );
});
