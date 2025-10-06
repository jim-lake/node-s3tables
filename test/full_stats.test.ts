import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { strict as assert } from 'node:assert';
import { inspect } from 'node:util';
import { config } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';
import { queryRows } from './helpers/athena_helper';
import { PassThrough } from 'node:stream';
import { PutObjectCommand } from '@aws-sdk/client-s3';
import { ParquetWriter, ParquetSchema } from 'parquetjs';
import { clients } from './helpers/aws_clients';

import { getMetadata, addDataFiles } from '../src';
import type { AddFile } from '../src/manifest';

async function createParquetFileWithStats(
  tableBucket: string,
  fileIndex: number
): Promise<{ key: string; size: number; stats: Partial<AddFile> }> {
  const s3Key = `data/app=test-app/data-${Date.now()}-${fileIndex}.parquet`;

  const schema = new ParquetSchema({
    app: { type: 'UTF8' },
    event_datetime: { type: 'TIMESTAMP_MILLIS' },
  });

  const stream = new PassThrough();
  const chunks: Buffer[] = [];
  stream.on('data', (chunk: Buffer) => chunks.push(chunk));

  const writer = await ParquetWriter.openStream(schema, stream);

  for (let i = 0; i < 10; i++) {
    await writer.appendRow({
      app: 'test-app',
      event_datetime: new Date(Date.now() + i * 1000),
    });
  }

  // Get stats from envelopeWriter before closing
  const envelopeWriter = writer.envelopeWriter;

  await writer.close();
  const fileBuffer = Buffer.concat(chunks);

  // Extract stats from rowGroups
  const rowGroup = envelopeWriter.rowGroups[0];
  if (!rowGroup) {
    throw new Error('No row group found in parquet file');
  }

  const columnSizes: Record<string, bigint> = {};
  const valueCounts: Record<string, bigint> = {};
  const nullValueCounts: Record<string, bigint> = {};
  const lowerBounds: Record<string, Buffer> = {};
  const upperBounds: Record<string, Buffer> = {};

  for (const rg of envelopeWriter.rowGroups) {
    for (const column of rg.columns) {
      const fieldName = column.meta_data.path_in_schema[0];
      if (!fieldName) {
        throw new Error('Missing field name in parquet column metadata');
      }

      columnSizes[fieldName] =
        (columnSizes[fieldName] ?? 0n) +
        BigInt(column.meta_data.total_compressed_size);
      valueCounts[fieldName] =
        (valueCounts[fieldName] ?? 0n) +
        BigInt(column.meta_data.statistics.distinct_count);
      nullValueCounts[fieldName] =
        (nullValueCounts[fieldName] ?? 0n) +
        BigInt(column.meta_data.statistics.null_count);

      const minValue = column.meta_data.statistics.min_value;
      const maxValue = column.meta_data.statistics.max_value;

      // Update lower bounds
      const shouldUpdateLower =
        !lowerBounds[fieldName] ||
        Buffer.compare(minValue, lowerBounds[fieldName]) < 0;
      if (shouldUpdateLower) {
        lowerBounds[fieldName] = minValue;
      }

      // Update upper bounds
      const shouldUpdateUpper =
        !upperBounds[fieldName] ||
        Buffer.compare(maxValue, upperBounds[fieldName]) > 0;
      if (shouldUpdateUpper) {
        upperBounds[fieldName] = maxValue;
      }
    }
  }

  const stats = {
    columnSizes,
    valueCounts,
    nullValueCounts,
    lowerBounds,
    upperBounds,
  };

  await clients.s3.send(
    new PutObjectCommand({ Bucket: tableBucket, Key: s3Key, Body: fileBuffer })
  );

  return { key: s3Key, size: fileBuffer.length, stats };
}

void test('add parquet files with full stats test', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_full_stats',
    'test_table_full_stats',
    [
      { name: 'app', type: 'string', required: true },
      { name: 'event_datetime', type: 'timestamp', required: true },
    ]
  );

  await t.test('add first parquet file with full stats (10 rows)', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    assert(tableBucket, 'Could not extract table bucket');

    const { key, size, stats } = await createParquetFileWithStats(
      tableBucket,
      1
    );

    // Log stats before validation
    log('Extracted stats:', inspect(stats, { depth: 3 }));

    // Validate stats are properly extracted
    assert(stats.columnSizes, 'columnSizes should be populated');
    assert(stats.valueCounts, 'valueCounts should be populated');
    assert(stats.nullValueCounts, 'nullValueCounts should be populated');
    assert(stats.lowerBounds, 'lowerBounds should be populated');
    assert(stats.upperBounds, 'upperBounds should be populated');

    log('Column sizes:', stats.columnSizes);
    assert(
      stats.columnSizes['app'] && stats.columnSizes['app'] > 0n,
      'app column size should be > 0'
    );
    assert(
      stats.columnSizes['event_datetime'] &&
        stats.columnSizes['event_datetime'] > 0n,
      'event_datetime column size should be > 0'
    );

    log('Value counts:', stats.valueCounts);
    assert(
      stats.valueCounts['app'] === 1n,
      'app value count should be 1 (distinct)'
    );
    assert(
      stats.valueCounts['event_datetime'] === 10n,
      'event_datetime value count should be 10'
    );

    log('Null counts:', stats.nullValueCounts);
    assert(stats.nullValueCounts['app'] === 0n, 'app null count should be 0');
    assert(
      stats.nullValueCounts['event_datetime'] === 0n,
      'event_datetime null count should be 0'
    );

    log('Bounds exist:', {
      appLower: Buffer.isBuffer(stats.lowerBounds['app']),
      appUpper: Buffer.isBuffer(stats.upperBounds['app']),
    });
    assert(
      Buffer.isBuffer(stats.lowerBounds['app']),
      'app lower bound should be Buffer'
    );
    assert(
      Buffer.isBuffer(stats.upperBounds['app']),
      'app upper bound should be Buffer'
    );

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
              partitions: { app: 'test-app' },
              recordCount: 10n,
              fileSize: BigInt(size),
              columnSizes: stats.columnSizes,
              valueCounts: stats.valueCounts,
              nullValueCounts: stats.nullValueCounts,
              lowerBounds: stats.lowerBounds,
              upperBounds: stats.upperBounds,
            },
          ],
        },
      ],
    });
    log('addDataFiles result 1:', result);

    const rows = await queryRows(namespace, name);
    log('Row count after first file:', rows.length);
    assert.strictEqual(
      rows.length,
      10,
      `Expected 10 rows after first file, got ${rows.length}`
    );
  });

  await t.test(
    'add second parquet file with full stats (10 rows)',
    async () => {
      const metadata = await getMetadata({
        tableBucketARN: config.tableBucketARN,
        namespace,
        name,
      });
      const tableBucket = metadata.location.split('/').slice(-1)[0];
      assert(tableBucket, 'Could not extract table bucket');

      const { key, size, stats } = await createParquetFileWithStats(
        tableBucket,
        2
      );

      // Log stats before validation
      log('Second file stats:', inspect(stats, { depth: 3 }));

      // Validate stats are properly extracted for second file
      assert(stats.columnSizes, 'columnSizes should be populated');
      assert(stats.valueCounts, 'valueCounts should be populated');
      assert(stats.nullValueCounts, 'nullValueCounts should be populated');
      assert(stats.lowerBounds, 'lowerBounds should be populated');
      assert(stats.upperBounds, 'upperBounds should be populated');

      log('Second file column sizes:', stats.columnSizes);
      assert(
        stats.columnSizes['app'] && stats.columnSizes['app'] > 0n,
        'app column size should be > 0'
      );
      assert(
        stats.columnSizes['event_datetime'] &&
          stats.columnSizes['event_datetime'] > 0n,
        'event_datetime column size should be > 0'
      );

      log('Second file value counts:', stats.valueCounts);
      assert(
        stats.valueCounts['app'] === 1n,
        'app value count should be 1 (distinct)'
      );
      assert(
        stats.valueCounts['event_datetime'] === 10n,
        'event_datetime value count should be 10'
      );

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
                partitions: { app: 'test-app' },
                recordCount: 10n,
                fileSize: BigInt(size),
                columnSizes: stats.columnSizes,
                valueCounts: stats.valueCounts,
                nullValueCounts: stats.nullValueCounts,
                lowerBounds: stats.lowerBounds,
                upperBounds: stats.upperBounds,
              },
            ],
          },
        ],
      });
      log('addDataFiles result 2:', result);

      const rows = await queryRows(namespace, name);
      log('Row count after second file:', rows.length);
      assert.strictEqual(
        rows.length,
        20,
        `Expected 20 rows after second file, got ${rows.length}`
      );
    }
  );

  await t.test('final metadata and athena validation', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });

    // Log metadata before validation
    log(
      'Final metadata snapshots:',
      metadata.snapshots.map((s) => ({
        id: s['snapshot-id'],
        summary: s.summary,
      }))
    );

    // Validate that stats were written to metadata
    assert(metadata.snapshots.length >= 2, 'Should have at least 2 snapshots');
    const latestSnapshot = metadata.snapshots[metadata.snapshots.length - 1];

    log('Latest snapshot:', latestSnapshot);
    assert(latestSnapshot, 'Latest snapshot should exist');
    assert(latestSnapshot.summary, 'Snapshot should have summary');

    log('Latest snapshot summary:', latestSnapshot.summary);
    assert(
      latestSnapshot.summary['added-records'] === '10',
      'Should have 10 added records in latest snapshot'
    );
    assert(
      latestSnapshot.summary['added-data-files'] === '1',
      'Should have 1 data file in latest snapshot'
    );

    // Validate final row count
    const finalRowCount = await queryRows(namespace, name);
    log('Final row count:', finalRowCount);
    assert.strictEqual(
      finalRowCount,
      20,
      `Expected 20 total rows, got ${finalRowCount}`
    );
  });
});
