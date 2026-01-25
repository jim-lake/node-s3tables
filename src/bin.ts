/* eslint-disable no-console */
import { manifestCompact } from 'node-s3tables';

const [tableBucketARN, namespace, name] = process.argv.slice(2);

if (!tableBucketARN || !namespace || !name) {
  console.error(
    'Usage: node-s3tables compact <tableBucketARN> <namespace> <name>'
  );
  process.exit(-1);
}

manifestCompact({ tableBucketARN, namespace, name })
  .then((result: unknown) => {
    console.log('Compact result:', result);
    process.exit(0);
  })
  .catch((error: unknown) => {
    if (error instanceof Error) {
      console.error('Error:', error.message);
    } else {
      console.error('Error:', error);
    }
    process.exit(1);
  });
