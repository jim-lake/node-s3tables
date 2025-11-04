import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { strict as assert } from 'node:assert';
import { config } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';
import { createSimpleParquetFile } from './helpers/parquet_helper';

import { getMetadata, addDataFiles, setCurrentCommit } from '../src';

void test('snapshot ID edge cases - small and large integers', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_snapshot_edge',
    'test_table_snapshot_edge',
    [
      { name: 'app', type: 'string', required: true },
      { name: 'event_datetime', type: 'timestamp', required: true },
    ]
  );

  let metadata = await getMetadata({
    tableBucketARN: config.tableBucketARN,
    namespace,
    name,
  });
  const tableBucket = metadata.location.split('/').slice(-1)[0];
  assert(tableBucket, 'Could not extract table bucket');

  // Test sequence: small -> small -> large -> small
  const smallId1 = 42n; // Small int < 2^32
  const smallId2 = 1000n; // Another small int < 2^32
  const largeId = 5000000000n; // Large int > 2^32
  const smallId3 = 123n; // Back to small int < 2^32

  await t.test('add snapshot with small ID (42)', async () => {
    const { key, size } = await createSimpleParquetFile(tableBucket, 1);

    const result = await addDataFiles({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      snapshotId: smallId1,
      lists: [
        {
          specId: 0,
          schemaId: 0,
          files: [
            {
              file: `s3://${tableBucket}/${key}`,
              partitions: { app: 'test-app' },
              recordCount: 10n,
              fileSize: BigInt(size),
            },
          ],
        },
      ],
    });

    log('Small ID 1 result:', result);
    assert.strictEqual(result.snapshotId, smallId1);

    metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    assert.strictEqual(BigInt(metadata['current-snapshot-id']), smallId1);
  });

  await t.test('add snapshot with another small ID (1000)', async () => {
    const { key, size } = await createSimpleParquetFile(tableBucket, 2);

    const result = await addDataFiles({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      snapshotId: smallId2,
      lists: [
        {
          specId: 0,
          schemaId: 0,
          files: [
            {
              file: `s3://${tableBucket}/${key}`,
              partitions: { app: 'test-app' },
              recordCount: 10n,
              fileSize: BigInt(size),
            },
          ],
        },
      ],
    });

    log('Small ID 2 result:', result);
    assert.strictEqual(result.snapshotId, smallId2);
    assert.strictEqual(result.parentSnapshotId, smallId1);

    metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    assert.strictEqual(BigInt(metadata['current-snapshot-id']), smallId2);
  });

  await t.test('add snapshot with large ID (5000000000)', async () => {
    const { key, size } = await createSimpleParquetFile(tableBucket, 3);

    const result = await addDataFiles({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      snapshotId: largeId,
      lists: [
        {
          specId: 0,
          schemaId: 0,
          files: [
            {
              file: `s3://${tableBucket}/${key}`,
              partitions: { app: 'test-app' },
              recordCount: 10n,
              fileSize: BigInt(size),
            },
          ],
        },
      ],
    });

    log('Large ID result:', result);
    assert.strictEqual(result.snapshotId, largeId);
    assert.strictEqual(result.parentSnapshotId, smallId2);

    metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    assert.strictEqual(BigInt(metadata['current-snapshot-id']), largeId);
  });

  await t.test('add snapshot back to small ID (123)', async () => {
    const { key, size } = await createSimpleParquetFile(tableBucket, 4);

    const result = await addDataFiles({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      snapshotId: smallId3,
      lists: [
        {
          specId: 0,
          schemaId: 0,
          files: [
            {
              file: `s3://${tableBucket}/${key}`,
              partitions: { app: 'test-app' },
              recordCount: 10n,
              fileSize: BigInt(size),
            },
          ],
        },
      ],
    });

    log('Small ID 3 result:', result);
    assert.strictEqual(result.snapshotId, smallId3);
    assert.strictEqual(result.parentSnapshotId, largeId);

    metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    assert.strictEqual(BigInt(metadata['current-snapshot-id']), smallId3);
  });

  await t.test('test setCurrentCommit with large ID', async () => {
    await setCurrentCommit({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      snapshotId: largeId,
    });

    metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    assert.strictEqual(BigInt(metadata['current-snapshot-id']), largeId);
  });

  await t.test('test setCurrentCommit back to small ID', async () => {
    await setCurrentCommit({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      snapshotId: smallId3,
    });

    metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    assert.strictEqual(BigInt(metadata['current-snapshot-id']), smallId3);
  });

  await t.test('verify all snapshots exist in metadata', async () => {
    metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });

    const snapshotIds = metadata.snapshots.map((s) => BigInt(s['snapshot-id']));
    log('All snapshot IDs:', snapshotIds);

    assert(snapshotIds.includes(smallId1), 'Should contain first small ID');
    assert(snapshotIds.includes(smallId2), 'Should contain second small ID');
    assert(snapshotIds.includes(largeId), 'Should contain large ID');
    assert(snapshotIds.includes(smallId3), 'Should contain third small ID');

    // Verify sequence numbers are properly incremented
    const snapshots = metadata.snapshots.sort(
      (a, b) => a['sequence-number'] - b['sequence-number']
    );

    for (let i = 1; i < snapshots.length; i++) {
      const prevSnapshot = snapshots[i - 1];
      const currSnapshot = snapshots[i];
      if (prevSnapshot && currSnapshot) {
        const prevSeq = BigInt(prevSnapshot['sequence-number']);
        const currSeq = BigInt(currSnapshot['sequence-number']);
        assert(
          currSeq > prevSeq,
          `Sequence numbers should be increasing: ${prevSeq} -> ${currSeq}`
        );
      }
    }
  });
});

void test('edge case integer values in partition transforms', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_int_edge',
    'test_table_int_edge',
    [
      { name: 'small_int', type: 'int', required: false },
      { name: 'large_long', type: 'long', required: false },
      { name: 'event_datetime', type: 'timestamp', required: true },
    ]
  );

  const metadata = await getMetadata({
    tableBucketARN: config.tableBucketARN,
    namespace,
    name,
  });
  const tableBucket = metadata.location.split('/').slice(-1)[0];
  assert(tableBucket, 'Could not extract table bucket');

  await t.test('test with max 32-bit integer values', async () => {
    const { key, size } = await createSimpleParquetFile(tableBucket, 1);

    const maxInt32 = 2147483647; // 2^31 - 1
    const maxUint32 = 4294967295; // 2^32 - 1

    const result = await addDataFiles({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      lists: [
        {
          specId: 0,
          schemaId: 0,
          files: [
            {
              file: `s3://${tableBucket}/${key}`,
              partitions: {
                small_int: maxInt32,
                large_long: BigInt(maxUint32) + 1n, // Just over 32-bit
              },
              recordCount: 10n,
              fileSize: BigInt(size),
            },
          ],
        },
      ],
    });

    log('Max int32 result:', result);
    assert(result.snapshotId > 0n, 'Should have valid snapshot ID');
  });

  await t.test('test with very large bigint values', async () => {
    const { key, size } = await createSimpleParquetFile(tableBucket, 2);

    const veryLargeBigInt = BigInt('9223372036854775807'); // Max int64

    const result = await addDataFiles({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      snapshotId: veryLargeBigInt - 1000n, // Use large snapshot ID
      lists: [
        {
          specId: 0,
          schemaId: 0,
          files: [
            {
              file: `s3://${tableBucket}/${key}`,
              partitions: { small_int: 1, large_long: veryLargeBigInt },
              recordCount: 10n,
              fileSize: BigInt(size),
            },
          ],
        },
      ],
    });

    log('Very large bigint result:', result);
    assert.strictEqual(result.snapshotId, veryLargeBigInt - 1000n);
  });
});
