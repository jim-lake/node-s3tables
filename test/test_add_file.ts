import { config } from './helpers/aws_clients';
import { createPartitionedParquetFile } from './helpers/parquet_helper';

import { getMetadata, addDataFiles } from '../src';

const namespace = process.argv[2];
const name = process.argv[3];

if (!namespace || !name) {
  console.log('Usage:', process.argv[1], '<namespace> <name>');
  process.exit(-1);
}

await main();
async function main() {
  const metadata = await getMetadata({
    tableBucketARN: config.tableBucketARN,
    namespace,
    name,
  });
  const tableBucket = metadata.location.split('/').slice(-1)[0];
  if (!tableBucket) {
    console.error('Could not extract table bucket');
    process.exit(-2);
  }

  const files = await Promise.all([
    createPartitionedParquetFile(
      tableBucket,
      'app1',
      new Date('2024-01-01'),
      1
    ),
  ]);

  const result = await addDataFiles({
    tableBucketARN: config.tableBucketARN,
    namespace,
    name,
    lists: [
      {
        specId: 1,
        schemaId: 0,
        files: [
          {
            file: `s3://${tableBucket}/${files[0].key}`,
            partitions: { app_name: 'app1', event_datetime_day: '2024-01-01' },
            recordCount: 10n,
            fileSize: BigInt(files[0].size),
          },
        ],
      },
    ],
  });
  console.log('addDataFiles result for all partitions:', result);
}
