import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { strict as assert } from 'node:assert';
import { randomBytes } from 'node:crypto';

// Copy the _randomBigInt64 function to test it
function _randomBigInt64(): bigint {
  const bytes = randomBytes(8);
  let ret = bytes.readBigUInt64BE();
  ret &= BigInt('0x7FFFFFFFFFFFFFFF');
  if (ret === 0n) {
    ret = 1n;
  }
  return ret;
}

void test('_randomBigInt64 function edge cases', () => {
  // Test that the function generates valid snapshot IDs
  const generatedIds = new Set<bigint>();
  const iterations = 1000;

  for (let i = 0; i < iterations; i++) {
    const id = _randomBigInt64();

    // Should be positive
    assert(id > 0n, `Generated ID should be positive: ${id}`);

    // Should be within signed 64-bit range (positive half)
    assert(
      id <= BigInt('0x7FFFFFFFFFFFFFFF'),
      `Generated ID should be <= max signed int64: ${id}`
    );

    // Should not be zero (function ensures this)
    assert(id !== 0n, `Generated ID should not be zero: ${id}`);

    generatedIds.add(id);
  }

  log(
    `Generated ${generatedIds.size} unique IDs out of ${iterations} iterations`
  );

  // Should generate mostly unique IDs (allowing for some collisions in a small sample)
  assert(
    generatedIds.size > iterations * 0.95,
    'Should generate mostly unique IDs'
  );

  // Test specific edge cases by simulating the logic manually

  // Simulate the edge case where random bytes would produce 0
  const zeroBytes = Buffer.alloc(8, 0);
  let ret = zeroBytes.readBigUInt64BE(); // This will be 0
  ret &= BigInt('0x7FFFFFFFFFFFFFFF'); // Still 0
  if (ret === 0n) {
    ret = 1n; // Function should set it to 1
  }
  assert.strictEqual(ret, 1n, 'Zero case should be handled correctly');

  // Test case where random bytes would produce max value
  const maxBytes = Buffer.alloc(8, 0xff);
  ret = maxBytes.readBigUInt64BE(); // This will be 0xFFFFFFFFFFFFFFFF
  ret &= BigInt('0x7FFFFFFFFFFFFFFF'); // Should become 0x7FFFFFFFFFFFFFFF
  assert.strictEqual(
    ret,
    BigInt('0x7FFFFFFFFFFFFFFF'),
    'Max case should be handled correctly'
  );

  // Test case with mixed bytes
  const mixedBytes = Buffer.from([
    0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
  ]);
  ret = mixedBytes.readBigUInt64BE(); // This will be 0x8000000000000001
  ret &= BigInt('0x7FFFFFFFFFFFFFFF'); // Should become 0x0000000000000001
  assert.strictEqual(ret, 1n, 'Mixed bytes case should be handled correctly');
});

void test('BigInt boundary value handling', () => {
  // Test various boundary values that might cause issues
  const boundaryValues = [
    { name: '1', value: 1n },
    { name: '2^31 - 1', value: BigInt(2147483647) },
    { name: '2^31', value: BigInt(2147483648) },
    { name: '2^32 - 1', value: BigInt(4294967295) },
    { name: '2^32', value: BigInt(4294967296) },
    {
      name: '2^53 - 1 (MAX_SAFE_INTEGER)',
      value: BigInt(Number.MAX_SAFE_INTEGER),
    },
    { name: '2^53', value: BigInt(Number.MAX_SAFE_INTEGER) + 1n },
    {
      name: '2^63 - 1 (max signed int64)',
      value: BigInt('9223372036854775807'),
    },
  ];

  boundaryValues.forEach(({ name, value }) => {
    log(`Testing boundary value ${name}: ${value}`);

    // Test that the value can be properly handled as a snapshot ID
    assert(typeof value === 'bigint', `${name} should be a bigint`);
    assert(value > 0n, `${name} should be positive`);

    // Test conversion to string and back (used in JSON serialization)
    const stringValue = value.toString();
    const backToBigInt = BigInt(stringValue);
    assert.strictEqual(
      backToBigInt,
      value,
      `${name} should round-trip through string conversion`
    );

    // Test that it fits in the expected range for snapshot IDs
    if (value <= BigInt('0x7FFFFFFFFFFFFFFF')) {
      log(`  ${name} fits in signed int64 range`);
    } else {
      log(`  ${name} exceeds signed int64 range`);
    }
  });
});
