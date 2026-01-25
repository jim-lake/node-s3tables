/* eslint-disable no-console */
import { manifestCompact } from 'node-s3tables';

const [tableBucketARN, namespace, name, forceRewrite] = process.argv.slice(2);

if (!tableBucketARN || !namespace || !name) {
  console.error(
    'Usage: node-s3tables compact <tableBucketARN> <namespace> <name> [forceRewrite]'
  );
  process.exit(-1);
}

console.log(
  'Compact:',
  tableBucketARN,
  namespace,
  name,
  'forceRewrite:',
  Boolean(forceRewrite)
);
manifestCompact({
  tableBucketARN,
  namespace,
  name,
  forceRewrite: Boolean(forceRewrite),
})
  .then((result: unknown) => {
    console.log('Compact result:', result);
    process.exit(0);
  })
  .catch((error: unknown) => {
    console.error('Error:', error);
    process.exit(1);
  });
