import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} from '@aws-sdk/client-s3';
import { S3TablesClient } from '@aws-sdk/client-s3tables';
import { Upload } from '@aws-sdk/lib-storage';
import * as avsc from 'avsc';
import { PassThrough } from 'node:stream';

import { fixupMetadata } from './avro_helper';
import { ManifestListType } from './avro_schema';

import type { Readable } from 'node:stream';
import type { AwsCredentialIdentity } from '@aws-sdk/types';
import type { ManifestListRecord } from './avro_types';

const S3_REGEX = /^s3:\/\/([^/]+)\/(.+)$/;
export function parseS3Url(url: string) {
  const match = S3_REGEX.exec(url);
  if (!match) {
    throw new Error('Invalid S3 URL');
  }
  return { bucket: match[1], key: match[2] };
}

const g_s3Map = new Map<
  string | undefined,
  Map<object | undefined, S3Client>
>();
const g_s3TablesMap = new Map<
  string | undefined,
  Map<object | undefined, S3TablesClient>
>();

export interface GetS3ClientParams {
  region?: string;
  credentials?: AwsCredentialIdentity | undefined;
}
export function getS3Client(params: GetS3ClientParams) {
  const { region, credentials } = params;
  let ret = g_s3Map.get(region)?.get(credentials);
  if (!ret) {
    const opts: ConstructorParameters<typeof S3Client>[0] = {};
    if (region) {
      opts.region = region;
    }
    if (credentials) {
      opts.credentials = credentials;
    }
    ret = new S3Client(opts);
    _setMap(g_s3Map, region, credentials, ret);
  }
  return ret;
}
export function getS3TablesClient(params: GetS3ClientParams) {
  const { region, credentials } = params;
  let ret = g_s3TablesMap.get(region)?.get(credentials);
  if (!ret) {
    const opts: ConstructorParameters<typeof S3TablesClient>[0] = {};
    if (region) {
      opts.region = region;
    }
    if (credentials) {
      opts.credentials = credentials;
    }
    ret = new S3TablesClient(opts);
    _setMap(g_s3TablesMap, region, credentials, ret);
  }
  return ret;
}
function _setMap<T>(
  map: Map<string | undefined, Map<object | undefined, T>>,
  region: string | undefined,
  credentials: AwsCredentialIdentity | undefined,
  client: T
) {
  let region_map = map.get(region);
  if (!region_map) {
    region_map = new Map();
  }
  region_map.set(credentials, client);
}

export interface WriteS3FileParams {
  credentials?: AwsCredentialIdentity | undefined;
  region: string;
  bucket: string;
  key: string;
  body: Buffer;
}
export async function writeS3File(params: WriteS3FileParams) {
  const { credentials, region, bucket, key, body } = params;

  const s3 = getS3Client({ region, credentials });
  const command = new PutObjectCommand({
    Bucket: bucket,
    Key: key,
    Body: body,
  });
  await s3.send(command);
}
export interface UpdateManifestListParams {
  credentials?: AwsCredentialIdentity | undefined;
  region: string;
  bucket: string;
  key: string;
  outKey: string;
  metadata?: Record<string, string | Buffer>;
  prepend: ManifestListRecord[];
}
export async function updateManifestList(params: UpdateManifestListParams) {
  const { region, credentials, bucket, key, outKey, prepend } = params;
  if (params.metadata) {
    fixupMetadata(params.metadata);
  }
  const s3 = getS3Client({ region, credentials });
  const get = new GetObjectCommand({ Bucket: bucket, Key: key });
  const response = await s3.send(get);
  const source = response.Body as Readable | undefined;
  if (!source) {
    throw new Error('failed to get source manifest list');
  }
  const passthrough = new PassThrough();
  const decoder = new avsc.streams.BlockDecoder({
    parseHook: () => ManifestListType,
  });
  const encoder = new avsc.streams.BlockEncoder(ManifestListType, {
    codec: 'deflate',
    metadata: params.metadata,
  } as ConstructorParameters<typeof avsc.streams.BlockEncoder>[1]);
  encoder.pipe(passthrough);
  for (const record of prepend) {
    encoder.write(record);
  }
  const upload = new Upload({
    client: s3,
    params: { Bucket: bucket, Key: outKey, Body: passthrough },
  });
  const stream_promise = new Promise<void>((resolve, reject) => {
    decoder.on('error', reject);
    decoder.on('data', (record: unknown) => {
      encoder.write(record);
    });
    decoder.on('end', () => {
      encoder.end();
    });
    decoder.on('finish', () => {
      resolve();
    });
    source.pipe(decoder);
  });
  await Promise.all([stream_promise, upload.done()]);
}
