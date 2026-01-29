import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { strict as assert } from 'node:assert';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import * as avsc from 'avsc';
import * as zlib from 'node:zlib';
import { config } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';
import { createPartitionedParquetFile } from './helpers/parquet_helper';
import { getMetadata, addPartitionSpec, addDataFiles } from '../src';
import { getS3Client, parseS3Url } from '../src/s3_tools';
import { ManifestListType } from '../src/avro_schema';
import type { ManifestListRecord } from '../src/avro_types';

void test('date partition bounds test', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_date_bounds',
    'test_table_date_bounds',
    [
      { name: 'event_datetime', type: 'timestamp', required: true },
      { name: 'detail', type: 'string', required: false },
    ]
  );

  await t.test('add partition spec', async () => {
    await addPartitionSpec({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      specId: 1,
      fields: [
        {
          'field-id': 1000,
          name: 'event_datetime_day',
          'source-id': 1,
          transform: 'day',
        },
      ],
    });
  });

  await t.test('add files with dates that compare incorrectly as buffers', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    const tableBucket = metadata.location.split('/').slice(-1)[0];
    assert(tableBucket, 'Could not extract table bucket');

    // 2027-07-01 and 2024-06-26 as int32le will compare incorrectly as raw buffers
    const date1 = new Date('2027-07-01');
    const date2 = new Date('2024-06-26');
    
    const { key: key1, size: size1 } = await createPartitionedParquetFile(
      tableBucket,
      'app1',
      date1,
      1
    );
    const { key: key2, size: size2 } = await createPartitionedParquetFile(
      tableBucket,
      'app1',
      date2,
      2
    );

    // Add both files in the same group so they go in the same manifest
    await addDataFiles({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
      lists: [
        {
          specId: 1,
          schemaId: 0,
          files: [
            {
              file: `s3://${tableBucket}/${key1}`,
              partitions: { event_datetime_day: '2027-07-01' },
              recordCount: 10n,
              fileSize: BigInt(size1),
            },
            {
              file: `s3://${tableBucket}/${key2}`,
              partitions: { event_datetime_day: '2024-06-26' },
              recordCount: 10n,
              fileSize: BigInt(size2),
            },
          ],
        },
      ],
    });
  });

  await t.test('verify manifest list bounds', async () => {
    const metadata = await getMetadata({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    
    const snapshot = metadata.snapshots[0];
    assert(snapshot, 'No snapshot found');
    
    const manifestListPath = snapshot['manifest-list'];
    const { bucket, key } = parseS3Url(manifestListPath);
    
    const s3Client = getS3Client({ region: config.region });
    const cmd = new GetObjectCommand({ Bucket: bucket, Key: key });
    const response = await s3Client.send(cmd);
    const body = await response.Body?.transformToByteArray();
    assert(body, 'No manifest list body');
    
    const records: ManifestListRecord[] = await new Promise((resolve, reject) => {
      const results: ManifestListRecord[] = [];
      const decoder = new avsc.streams.BlockDecoder({
        codecs: { deflate: zlib.inflateRaw },
        parseHook: () => ManifestListType,
      });
      
      decoder.on('data', (record: ManifestListRecord) => {
        results.push(record);
      });
      
      decoder.on('end', () => resolve(results));
      decoder.on('error', reject);
      
      decoder.write(Buffer.from(body));
      decoder.end();
    });
    
    assert.strictEqual(records.length, 1, 'Expected 1 manifest');
    const manifest = records[0];
    
    assert(manifest.partitions, 'No partitions in manifest');
    assert.strictEqual(manifest.partitions.length, 1, 'Expected 1 partition field');
    
    const partition = manifest.partitions[0];
    assert(partition.lower_bound, 'No lower_bound');
    assert(partition.upper_bound, 'No upper_bound');
    
    // Convert to Buffer if needed
    const lowerBound = Buffer.isBuffer(partition.lower_bound) 
      ? partition.lower_bound 
      : Buffer.from(partition.lower_bound);
    const upperBound = Buffer.isBuffer(partition.upper_bound)
      ? partition.upper_bound
      : Buffer.from(partition.upper_bound);
    
    // Convert dates to days since epoch (int32)
    const date1 = new Date('2027-07-01');
    const date2 = new Date('2024-06-26');
    const days1 = Math.floor(date1.getTime() / 86400000);
    const days2 = Math.floor(date2.getTime() / 86400000);
    
    const lowerDays = lowerBound.readInt32LE();
    const upperDays = upperBound.readInt32LE();
    
    log('Date 1 (2027-07-01) days:', days1);
    log('Date 2 (2024-06-26) days:', days2);
    log('Lower bound days:', lowerDays);
    log('Upper bound days:', upperDays);
    
    // Verify bounds are correct
    assert.strictEqual(lowerDays, Math.min(days1, days2), 'Lower bound should be the earlier date');
    assert.strictEqual(upperDays, Math.max(days1, days2), 'Upper bound should be the later date');
    
    // Verify that raw buffer comparison would give wrong answer
    const bufferCompare = Buffer.compare(lowerBound, upperBound);
    log('Buffer.compare result:', bufferCompare);
    log('Correct int32 comparison:', lowerDays < upperDays ? -1 : lowerDays > upperDays ? 1 : 0);
    
    // The test proves that the code correctly compares int32 values, not raw buffers
    // If it used raw buffer comparison, the bounds would be swapped
    assert.notStrictEqual(bufferCompare, lowerDays < upperDays ? -1 : 1, 
      'Raw buffer comparison should give different result than int32 comparison');
  });
});
