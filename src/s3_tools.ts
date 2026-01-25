import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} from '@aws-sdk/client-s3';
import { S3TablesClient } from '@aws-sdk/client-s3tables';
import { Upload } from '@aws-sdk/lib-storage';
import * as avsc from 'avsc';
import { PassThrough, Transform } from 'node:stream';
import * as zlib from 'node:zlib';

import { fixupMetadata } from './avro_helper';
import { ManifestListSchema, ManifestListType } from './avro_schema';
import { AvroRegistry, ListContent } from './avro_types';
import { translateRecord } from './schema_translator';

import type { Readable } from 'node:stream';
import type { AwsCredentialIdentity } from '@aws-sdk/types';
import type { Schema, Type } from 'avsc';
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

class ByteCounter extends Transform {
  public bytes = 0;
  override _transform(
    chunk: Buffer,
    _encoding: BufferEncoding,
    callback: (error?: Error | null, data?: unknown) => void
  ): void {
    this.bytes += chunk.length;
    callback(null, chunk);
  }
}

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
  region_map ??= new Map();
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
  const metadata = params.metadata
    ? fixupMetadata(params.metadata)
    : params.metadata;
  const s3 = getS3Client({ region, credentials });
  const get = new GetObjectCommand({ Bucket: bucket, Key: key });
  const response = await s3.send(get);
  const source = response.Body as Readable | undefined;
  if (!source) {
    throw new Error('failed to get source manifest list');
  }
  const passthrough = new PassThrough();
  let sourceSchema: unknown;
  const decoder = new avsc.streams.BlockDecoder({
    codecs: { deflate: zlib.inflateRaw },
    parseHook(schema) {
      sourceSchema = schema;
      return avsc.Type.forSchema(schema as unknown as avsc.Schema, {
        registry: { ...AvroRegistry },
      });
    },
  });
  const encoder = new avsc.streams.BlockEncoder(ManifestListType, {
    codec: 'deflate',
    codecs: { deflate: zlib.deflateRaw },
    metadata,
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
    source.on('error', (err) => {
      reject(err);
    });
    passthrough.on('error', (err) => {
      reject(err);
    });
    encoder.on('error', (err) => {
      reject(err);
    });
    decoder.on('error', (err) => {
      reject(err);
    });
    decoder.on('data', (record: unknown) => {
      const translated = translateRecord(
        sourceSchema,
        ManifestListSchema,
        record
      ) as ManifestListRecord;
      if (
        translated.content !== ListContent.DATA ||
        translated.added_files_count > 0 ||
        translated.existing_files_count > 0
      ) {
        if (!encoder.write(translated)) {
          decoder.pause();
          encoder.once('drain', () => decoder.resume());
        }
      }
    });
    decoder.on('end', () => {
      encoder.end();
    });
    encoder.on('finish', () => {
      resolve();
    });
    source.pipe(decoder);
  });
  await Promise.all([stream_promise, upload.done()]);
}
export interface StreamWriteAvroParams<T> {
  credentials?: AwsCredentialIdentity | undefined;
  region: string;
  bucket: string;
  key: string;
  metadata?: Record<string, string | Buffer>;
  avroType: Type;
  iter: AsyncIterable<T[]>;
}
export async function streamWriteAvro<T>(
  params: StreamWriteAvroParams<T>
): Promise<number> {
  const { region, credentials, bucket, key } = params;
  const metadata = params.metadata
    ? fixupMetadata(params.metadata)
    : params.metadata;
  const s3 = getS3Client({ region, credentials });
  const encoder = new avsc.streams.BlockEncoder(params.avroType, {
    codec: 'deflate',
    codecs: { deflate: zlib.deflateRaw },
    metadata,
  } as ConstructorParameters<typeof avsc.streams.BlockEncoder>[1]);
  const counter = new ByteCounter();
  encoder.pipe(counter);
  const upload = new Upload({
    client: s3,
    params: { Bucket: bucket, Key: key, Body: counter },
  });
  async function _abortUpload() {
    try {
      await upload.abort();
    } catch {
      // noop
    }
  }

  const upload_promise = upload.done();
  let found_err: Error | undefined;
  upload_promise.catch((err: unknown) => {
    found_err ??= err as Error;
  });
  encoder.on('error', (err: Error) => {
    found_err ??= err;
    void _abortUpload();
  });
  for await (const batch of params.iter) {
    if (found_err) {
      void _abortUpload();
      throw found_err;
    }
    for (const record of batch) {
      encoder.write(record);
    }
  }
  encoder.end();
  await upload_promise;
  if (found_err) {
    void _abortUpload();
    throw found_err;
  }
  return counter.bytes;
}
export interface DownloadAvroParams {
  credentials?: AwsCredentialIdentity | undefined;
  region: string;
  bucket: string;
  key: string;
  avroSchema: Schema;
}
export async function downloadAvro<T>(
  params: DownloadAvroParams
): Promise<T[]> {
  const { region, credentials, bucket, key, avroSchema } = params;
  const s3 = getS3Client({ region, credentials });
  const get = new GetObjectCommand({ Bucket: bucket, Key: key });
  const response = await s3.send(get);
  const source = response.Body as Readable | undefined;
  if (!source) {
    throw new Error('failed to get source manifest list');
  }
  let sourceSchema: unknown;
  const decoder = new avsc.streams.BlockDecoder({
    codecs: { deflate: zlib.inflateRaw },
    parseHook(schema) {
      sourceSchema = schema;
      return avsc.Type.forSchema(schema as unknown as avsc.Schema, {
        registry: { ...AvroRegistry },
      });
    },
  });
  const records: T[] = [];
  const stream_promise = new Promise<void>((resolve, reject) => {
    source.on('error', (err) => {
      reject(err);
    });
    decoder.on('error', (err) => {
      reject(err);
    });
    decoder.on('data', (record: unknown) => {
      const translated = translateRecord(sourceSchema, avroSchema, record);
      records.push(translated as T);
    });
    decoder.on('end', () => {
      resolve();
    });
    source.pipe(decoder);
  });
  await stream_promise;
  return records;
}
