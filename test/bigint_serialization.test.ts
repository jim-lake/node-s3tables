/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-base-to-string */
import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { strict as assert } from 'node:assert';
import { BigIntType } from '../src/avro_types';

void test('BigIntType behavior investigation', () => {
  // Test what toJSON actually returns
  const testValues = [
    42n,
    1000n,
    BigInt(Number.MAX_SAFE_INTEGER),
    BigInt('9223372036854775807'),
  ];

  testValues.forEach((_value) => {
    const jsonResult = BigIntType.toJSON();
    log(`BigIntType.toJSON() = ${jsonResult} (type: ${typeof jsonResult})`);
  });

  // Test buffer serialization/deserialization
  testValues.forEach((value) => {
    const buffer = BigIntType.toBuffer(value);
    const restored = BigIntType.fromBuffer(buffer);
    log(
      `Buffer round-trip: ${value} -> ${Buffer.from(buffer).toString('hex')} -> ${restored}`
    );
    assert.strictEqual(
      restored,
      value,
      `Buffer round-trip should work for ${value}`
    );
  });

  // Test isValid
  const validValues = [42n, 0n, -1n, BigInt(Number.MAX_SAFE_INTEGER)];
  const invalidValues: unknown[] = [42, '42', null, undefined, {}];

  validValues.forEach((value) => {
    assert.strictEqual(
      BigIntType.isValid(value),
      true,
      `${value} should be valid`
    );
  });

  invalidValues.forEach((value) => {
    assert.strictEqual(
      BigIntType.isValid(value),
      false,
      `${String(value)} should be invalid`
    );
  });

  // Test compare
  assert.strictEqual(
    BigIntType.compare(42n, 42n),
    0,
    'Equal values should compare to 0'
  );
  assert.strictEqual(
    BigIntType.compare(41n, 42n),
    -1,
    'Smaller value should compare to -1'
  );
  assert.strictEqual(
    BigIntType.compare(43n, 42n),
    1,
    'Larger value should compare to 1'
  );
});

void test('BigIntType edge cases with large values', () => {
  // Test values around different boundaries
  const testCases = [
    { name: 'small positive', value: 42n },
    { name: 'small negative', value: -42n },
    { name: '32-bit max', value: BigInt(2147483647) },
    { name: '32-bit min', value: BigInt(-2147483648) },
    { name: 'uint32 max', value: BigInt(4294967295) },
    { name: 'safe int max', value: BigInt(Number.MAX_SAFE_INTEGER) },
    { name: 'over safe int', value: BigInt(Number.MAX_SAFE_INTEGER) + 1n },
    { name: 'int64 max', value: BigInt('9223372036854775807') },
    { name: 'int64 min', value: BigInt('-9223372036854775808') },
  ];

  testCases.forEach(({ name, value }) => {
    log(`Testing ${name}: ${value}`);

    // Buffer serialization should always work
    const buffer = BigIntType.toBuffer(value);
    const restored = BigIntType.fromBuffer(buffer);
    assert.strictEqual(
      restored,
      value,
      `Buffer round-trip should work for ${name}`
    );

    // JSON serialization behavior
    const jsonResult = BigIntType.toJSON();
    log(`  JSON result: ${jsonResult} (type: ${typeof jsonResult})`);

    /*
     * Note: The avsc library's BigIntType.toJSON returns 'long' string, not a number
     * This is actually correct behavior for Avro schema type identification
     * The actual serialization uses buffer encoding which preserves precision
     */
  });
});
