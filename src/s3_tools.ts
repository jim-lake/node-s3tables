import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} from '@aws-sdk/client-s3';
import { S3TablesClient } from '@aws-sdk/client-s3tables';
import { Upload } from '@aws-sdk/lib-storage';
import * as avsc from '@jim-lake/avsc';
import { PassThrough, Transform } from 'node:stream';
import * as zlib from 'node:zlib';

import { fixupMetadata } from './avro_helper';
import { ManifestListSchema, ManifestListType } from './avro_schema';
import { AvroRegistry, ListContent } from './avro_types';
import { translateRecord } from './schema_translator';

import type { Readable } from 'node:stream';
import type { Schema, Type } from '@jim-lake/avsc';
import type { ManifestListRecord } from './avro_types';
import type { AwsConfig } from './config';

const S3_REGEX = /^s3:\/\/([^/]+)\/(.+)$/;
export function parseS3Url(url: string): { bucket: string; key: string } {
  const match = S3_REGEX.exec(url);
  if (!match?.[1] || !match[2]) {
    throw new Error('Invalid S3 URL');
  }
  return { bucket: match[1], key: match[2] };
}

const g_s3Map = new Map<string, S3Client>();
const g_s3TablesMap = new Map<string, S3TablesClient>();

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

/**
 * Build a stable cache key for a client config. Non-serializable values
 * (functions such as `credentials` providers, class instances) are keyed by a
 * per-object identity id so distinct instances get distinct clients while
 * identical plain-object configs are reused.
 */
const g_identityIds = new WeakMap<object, number>();
let g_identityCounter = 0;
function _identityId(obj: object): number {
  let id = g_identityIds.get(obj);
  if (id === undefined) {
    id = ++g_identityCounter;
    g_identityIds.set(obj, id);
  }
  return id;
}
function _configKey(config: object): string {
  return JSON.stringify(config, (_key, value: unknown) => {
    if (typeof value === 'function') {
      return `fn#${_identityId(value)}`;
    }
    if (
      value &&
      typeof value === 'object' &&
      value.constructor !== Object &&
      !Array.isArray(value)
    ) {
      return `obj#${_identityId(value)}`;
    }
    return value;
  });
}

export function getS3Client(config: AwsConfig): S3Client {
  const key = _configKey(config.s3ClientConfig);
  let ret = g_s3Map.get(key);
  if (!ret) {
    ret = new S3Client(config.s3ClientConfig);
    g_s3Map.set(key, ret);
  }
  return ret;
}
export function getS3TablesClient(config: AwsConfig): S3TablesClient {
  const key = _configKey(config.s3TablesClientConfig);
  let ret = g_s3TablesMap.get(key);
  if (!ret) {
    ret = new S3TablesClient(config.s3TablesClientConfig);
    g_s3TablesMap.set(key, ret);
  }
  return ret;
}

export interface WriteS3FileParams {
  config: AwsConfig;
  bucket: string;
  key: string;
  body: Buffer;
}
export async function writeS3File(params: WriteS3FileParams) {
  const { bucket, key, body } = params;

  const s3 = getS3Client(params.config);
  const command = new PutObjectCommand({
    Bucket: bucket,
    Key: key,
    Body: body,
  });
  await s3.send(command);
}
export interface UpdateManifestListParams {
  config: AwsConfig;
  bucket: string;
  key: string;
  outKey: string;
  metadata?: Record<string, string | Buffer>;
  prepend: ManifestListRecord[];
}
export async function updateManifestList(params: UpdateManifestListParams) {
  const { bucket, key, outKey, prepend } = params;
  const metadata = params.metadata
    ? fixupMetadata(params.metadata)
    : params.metadata;
  const s3 = getS3Client(params.config);
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
      return avsc.Type.forSchema(schema, { registry: { ...AvroRegistry } });
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
  config: AwsConfig;
  bucket: string;
  key: string;
  metadata?: Record<string, string | Buffer>;
  avroType: Type;
  iter: AsyncIterable<T[]>;
}
export async function streamWriteAvro<T>(
  params: StreamWriteAvroParams<T>
): Promise<number> {
  const { bucket, key } = params;
  const metadata = params.metadata
    ? fixupMetadata(params.metadata)
    : params.metadata;
  const s3 = getS3Client(params.config);
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
  config: AwsConfig;
  bucket: string;
  key: string;
  avroSchema: Schema;
}
export async function downloadAvro<T>(
  params: DownloadAvroParams
): Promise<T[]> {
  const { bucket, key, avroSchema } = params;
  const s3 = getS3Client(params.config);
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
      return avsc.Type.forSchema(schema, { registry: { ...AvroRegistry } });
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
