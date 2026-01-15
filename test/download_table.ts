import { GetObjectCommand } from '@aws-sdk/client-s3';
import { writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import * as avsc from 'avsc';
import * as zlib from 'node:zlib';
import { Readable } from 'node:stream';

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
    '';
  const s3Client = getS3Client({ region: tableRegion, credentials });

  /* Save metadata.json */
  const metadataPath = join(outputDir, 'metadata.json');
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
    const mlPath = join(outputDir, mlKey.split('/').pop() ?? mlKey);
    const mlCmd = new GetObjectCommand({ Bucket: mlBucket, Key: mlKey });
    const mlResponse = await s3Client.send(mlCmd);
    const mlBody = await mlResponse.Body?.transformToByteArray();
    if (!mlBody) {
      throw new Error('missing manifest list body');
    }
    await writeFile(mlPath, Buffer.from(mlBody));

    /* Parse manifest list to get manifest paths */
    const manifestPaths = await _parseManifestList(Buffer.from(mlBody));

    /* Download each manifest */
    for (const manifestPath of manifestPaths) {
      const { bucket: mBucket, key: mKey } = parseS3Url(manifestPath);
      if (!mKey) {
        throw new Error('invalid manifest key');
      }
      const mPath = join(outputDir, mKey.split('/').pop() ?? mKey);
      const mCmd = new GetObjectCommand({ Bucket: mBucket, Key: mKey });
      const mResponse = await s3Client.send(mCmd);
      const mBody = await mResponse.Body?.transformToByteArray();
      if (!mBody) {
        throw new Error('missing manifest body');
      }
      await writeFile(mPath, Buffer.from(mBody));

      /* Parse manifest to get data file paths */
      const dataFilePaths = await _parseManifest(Buffer.from(mBody));

      /* Download each parquet file */
      for (const dataFilePath of dataFilePaths) {
        const { bucket: dBucket, key: dKey } = parseS3Url(dataFilePath);
        if (!dKey) {
          throw new Error('invalid data file key');
        }
        const dPath = join(outputDir, dKey.split('/').pop() ?? dKey);
        const dCmd = new GetObjectCommand({ Bucket: dBucket, Key: dKey });
        const dResponse = await s3Client.send(dCmd);
        const dBody = await dResponse.Body?.transformToByteArray();
        if (!dBody) {
          throw new Error('missing data file body');
        }
        await writeFile(dPath, Buffer.from(dBody));
      }
    }
  }
}
async function _parseManifestList(buffer: Buffer): Promise<string[]> {
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

    Readable.from(buffer).pipe(decoder);
  });
}

async function _parseManifest(buffer: Buffer): Promise<string[]> {
  return new Promise((resolve, reject) => {
    const paths: string[] = [];
    const decoder = new avsc.streams.BlockDecoder({
      codecs: { deflate: zlib.inflateRaw },
      parseHook(schema) {
        return avsc.Type.forSchema(schema, { registry: { ...AvroRegistry } });
      },
    });

    decoder.on('data', (record: { data_file: { file_path: string } }) => {
      paths.push(record.data_file.file_path);
    });
    decoder.on('end', () => {
      resolve(paths);
    });
    decoder.on('error', reject);

    Readable.from(buffer).pipe(decoder);
  });
}

if (require.main === module) {
  const tableBucketARN = process.argv[2];
  const namespace = process.argv[3];
  const name = process.argv[4];
  const outputDir = process.argv[5];
  if (!tableBucketARN || !namespace || !name || !outputDir) {
    console.log(
      'Usage:',
      process.argv[1],
      '<tableBucketARN> <namespace> <name> <outputDir>'
    );
    process.exit(-1);
  } else {
    void downloadTable({ tableBucketARN, namespace, name, outputDir });
  }
}
