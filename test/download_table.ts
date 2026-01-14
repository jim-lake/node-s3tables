import { GetObjectCommand } from '@aws-sdk/client-s3';
import { writeFile, mkdir } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import * as avsc from 'avsc';
import * as zlib from 'node:zlib';

import { getS3Client, parseS3Url } from '../src/s3_tools';
import { getMetadata } from '../src/metadata';
import { stringify } from '../src/json';
import { AvroRegistry } from '../src/avro_types';

import type { AwsCredentialIdentity } from '@aws-sdk/types';
import type { IcebergMetadata } from '../src/iceberg';

export interface DownloadTableParams {
  tableArn?: string;
  tableBucketARN?: string;
  namespace?: string;
  name?: string;
  outputDir: string;
  region?: string;
  credentials?: AwsCredentialIdentity;
}

export async function downloadTable(
  params: DownloadTableParams
): Promise<void> {
  const { outputDir, region, credentials } = params;

  const metadata: IcebergMetadata = params.tableArn
    ? await getMetadata({
        tableArn: params.tableArn,
        ...(region && { region }),
        ...(credentials && { credentials }),
      })
    : params.tableBucketARN && params.namespace && params.name
      ? await getMetadata({
          tableBucketARN: params.tableBucketARN,
          namespace: params.namespace,
          name: params.name,
          ...(region && { region }),
          ...(credentials && { credentials }),
        })
      : (() => {
          throw new Error(
            'must provide either tableArn or tableBucketARN+namespace+name'
          );
        })();

  const tableRegion =
    params.tableBucketARN?.split(':')[3] ??
    params.tableArn?.split(':')[3] ??
    region ??
    'us-east-1';
  const s3Client = getS3Client({ region: tableRegion, credentials });

  /* Save metadata.json */
  const metadataPath = join(outputDir, 'metadata.json');
  await mkdir(dirname(metadataPath), { recursive: true });
  const metadataStr = stringify(metadata);
  if (!metadataStr) {
    throw new Error('failed to stringify metadata');
  }
  await writeFile(metadataPath, metadataStr);

  /* Download all manifest lists and their manifests */
  for (const snapshot of metadata.snapshots) {
    const manifestListUrl = snapshot['manifest-list'];
    const { bucket: mlBucket, key: mlKey } = parseS3Url(manifestListUrl);
    if (!mlKey) {
      throw new Error('invalid manifest list key');
    }

    /* Download manifest list */
    const mlPath = join(outputDir, mlKey);
    await mkdir(dirname(mlPath), { recursive: true });
    const mlCmd = new GetObjectCommand({ Bucket: mlBucket, Key: mlKey });
    const mlResponse = await s3Client.send(mlCmd);
    const mlBody = await mlResponse.Body?.transformToByteArray();
    if (!mlBody) {
      throw new Error('missing manifest list body');
    }
    await writeFile(mlPath, Buffer.from(mlBody));

    /* Parse manifest list to get manifest paths */
    const manifestPaths = await parseManifestList(Buffer.from(mlBody));

    /* Download each manifest */
    for (const manifestPath of manifestPaths) {
      const { bucket: mBucket, key: mKey } = parseS3Url(manifestPath);
      if (!mKey) {
        throw new Error('invalid manifest key');
      }
      const mPath = join(outputDir, mKey);
      await mkdir(dirname(mPath), { recursive: true });
      const mCmd = new GetObjectCommand({ Bucket: mBucket, Key: mKey });
      const mResponse = await s3Client.send(mCmd);
      const mBody = await mResponse.Body?.transformToByteArray();
      if (!mBody) {
        throw new Error('missing manifest body');
      }
      await writeFile(mPath, Buffer.from(mBody));
    }
  }
}

async function parseManifestList(buffer: Buffer): Promise<string[]> {
  return new Promise((resolve, reject) => {
    const paths: string[] = [];
    const decoder = new avsc.streams.BlockDecoder({
      codecs: { deflate: zlib.inflateRaw },
      parseHook(schema) {
        return avsc.Type.forSchema(schema, { registry: { ...AvroRegistry } });
      },
    });

    decoder.on('data', (record: { manifest_path: string }) => {
      paths.push(record.manifest_path);
    });
    decoder.on('end', () => {
      resolve(paths);
    });
    decoder.on('error', reject);

    /* eslint-disable-next-line @typescript-eslint/no-require-imports, @typescript-eslint/no-var-requires */
    const { Readable } = require('node:stream') as typeof import('node:stream');
    Readable.from(buffer).pipe(decoder);
  });
}
