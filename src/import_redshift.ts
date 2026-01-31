import { randomUUID } from 'node:crypto';
import { createZstdDecompress } from 'node:zlib';
import { Readable, Transform, Writable, PassThrough } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import { ParquetWriter, ParquetSchema } from 'parquetjs';
import { addDataFiles } from './add_data_files';
import { parse } from './json';
import { getMetadata } from './metadata';
import { parseS3Url, getS3Client } from './s3_tools';

import type { AwsCredentialIdentity } from '@aws-sdk/types';
import type { AddDataFilesResult, AddFileList } from './add_data_files';
import type { AddFile } from './manifest';
import type {
  IcebergMetadata,
  IcebergSchema,
  IcebergSchemaField,
} from './iceberg';

export default { importRedshiftManifest };

export interface ImportRedshiftManifestParams {
  credentials?: AwsCredentialIdentity;
  tableBucketARN: string;
  namespace: string;
  name: string;
  redshiftManifestUrl: string;
  schemaId?: number;
  specId?: number;
  retryCount?: number | undefined;
}

export async function importRedshiftManifest(
  params: ImportRedshiftManifestParams
): Promise<AddDataFilesResult> {
  const { credentials } = params;
  const region = params.tableBucketARN.split(':')[3];
  if (!region) {
    throw new Error('bad tableBucketARN');
  }
  const manifest = await _downloadRedshift(params);
  manifest.entries.sort((a, b) => {
    const sizeA = Number(a.meta?.content_length ?? 0);
    const sizeB = Number(b.meta?.content_length ?? 0);
    return sizeB - sizeA;
  });

  const metadata = await getMetadata(params);
  const bucket = metadata.location.split('/').slice(-1)[0];
  if (!bucket) {
    throw new Error('bad manifest location');
  }

  const import_prefix = `data/${randomUUID()}/`;
  const lists: AddFileList[] = [];
  const BATCH_SIZE = 10;
  let lastResult: AddDataFilesResult | null = null;

  for (const entry of manifest.entries) {
    const { url } = entry;
    const file = url.split('/').pop()?.replace('.json.zst', '.parquet') ?? '';
    const parts = [...url.matchAll(/\/([^=/]*=[^/=]*)/g)].map(
      (m) => m[1] ?? ''
    );
    const partitions: Record<string, string> = {};
    for (const part of parts) {
      const [part_key, part_value] = part.split('=');
      partitions[part_key ?? ''] = part_value ?? '';
    }
    const keys = Object.keys(partitions);
    const specId = params.specId ?? _findSpec(metadata, keys);
    const schemaId = params.schemaId ?? _findSchema(metadata, manifest);
    const schema = metadata.schemas.find((s) => s['schema-id'] === schemaId);
    if (!schema) {
      throw new Error(`schema ${schemaId} not found`);
    }

    let list = lists.find(
      (l) => l.schemaId === schemaId && l.specId === specId
    );
    if (!list) {
      list = { specId, schemaId, files: [] };
      lists.push(list);
    }
    const part_path = parts.length > 0 ? `${parts.join('/')}/` : '';
    const key = import_prefix + part_path + file;
    const { s3Url, stats } = await _convertJsonToParquet({
      credentials,
      region,
      bucket,
      key,
      url,
      schema,
      partitions,
    });
    list.files.push({ file: s3Url, partitions, ...stats });

    if (lists.reduce((sum, l) => sum + l.files.length, 0) >= BATCH_SIZE) {
      lastResult = await addDataFiles({
        credentials,
        tableBucketARN: params.tableBucketARN,
        namespace: params.namespace,
        name: params.name,
        lists,
        retryCount: params.retryCount,
      });
      lists.length = 0;
    }
  }

  if (lists.length > 0 && lists.some((l) => l.files.length > 0)) {
    lastResult = await addDataFiles({
      credentials,
      tableBucketARN: params.tableBucketARN,
      namespace: params.namespace,
      name: params.name,
      lists,
      retryCount: params.retryCount,
    });
  }

  if (!lastResult) {
    throw new Error('No files were processed');
  }

  return lastResult;
}

interface RedshiftManifest {
  entries: {
    url: string;
    meta: { content_length: number | bigint; record_count: number | bigint };
  }[];
  schema: { elements: { name: string }[] };
}

async function _downloadRedshift(params: ImportRedshiftManifestParams) {
  const s3_client = getS3Client(params);
  const { bucket, key } = parseS3Url(params.redshiftManifestUrl);
  const get_file_cmd = new GetObjectCommand({ Bucket: bucket, Key: key });
  const file_response = await s3_client.send(get_file_cmd);
  const body = await file_response.Body?.transformToString();
  if (!body) {
    throw new Error('missing body');
  }
  return parse(body) as unknown as RedshiftManifest;
}

interface ConvertJsonToParquetParams {
  credentials?: AwsCredentialIdentity | undefined;
  region: string;
  bucket: string;
  key: string;
  url: string;
  schema: IcebergSchema;
  partitions?: Record<string, string>;
}

interface ConvertJsonToParquetResult {
  s3Url: string;
  stats: Omit<AddFile, 'file' | 'partitions'>;
}

async function _convertJsonToParquet(
  params: ConvertJsonToParquetParams
): Promise<ConvertJsonToParquetResult> {
  const { bucket: sourceBucket, key: sourceKey } = parseS3Url(params.url);
  if (!sourceBucket || !sourceKey) {
    throw new Error(`bad entry url: ${params.url}`);
  }
  const s3_client = getS3Client(params);

  const get = new GetObjectCommand({ Bucket: sourceBucket, Key: sourceKey });
  const { Body } = await s3_client.send(get);
  if (!Body) {
    throw new Error(`body missing for file: ${params.url}`);
  }

  const parquetSchema = _buildParquetSchema(params.schema);
  const chunks: Buffer[] = [];
  const parquetStream = new PassThrough();
  parquetStream.on('data', (chunk) => chunks.push(chunk));

  const writer = await ParquetWriter.openStream(parquetSchema, parquetStream);

  let recordCount = 0n;

  await pipeline(
    Readable.from(Body as any),
    createZstdDecompress(),
    _createJsonLineTransform(params.schema),
    new Writable({
      objectMode: true,
      async write(row, _encoding, callback) {
        try {
          await writer.appendRow(row);
          recordCount++;
          callback();
        } catch (err) {
          callback(err as Error);
        }
      },
    })
  );

  await writer.close();
  const fileBuffer = Buffer.concat(chunks);

  const stats = _extractStatsFromWriter(writer, params.schema);

  const upload = new Upload({
    client: s3_client,
    params: { Bucket: params.bucket, Key: params.key, Body: fileBuffer },
  });
  await upload.done();

  return {
    s3Url: `s3://${params.bucket}/${params.key}`,
    stats: { ...stats, fileSize: BigInt(fileBuffer.length), recordCount },
  };
}

function _buildParquetSchema(schema: IcebergSchema) {
  const fields: Record<string, { type: string; optional?: boolean }> = {};
  for (const field of schema.fields) {
    const type = typeof field.type === 'string' ? field.type : 'string';
    let parquetType: string;
    switch (type) {
      case 'int':
        parquetType = 'INT32';
        break;
      case 'long':
        parquetType = 'INT64';
        break;
      case 'float':
        parquetType = 'FLOAT';
        break;
      case 'double':
        parquetType = 'DOUBLE';
        break;
      case 'boolean':
        parquetType = 'BOOLEAN';
        break;
      case 'timestamp':
      case 'timestamptz':
        parquetType = 'TIMESTAMP_MICROS';
        break;
      case 'date':
        parquetType = 'DATE';
        break;
      default:
        parquetType = 'UTF8';
    }
    fields[field.name] = { type: parquetType, optional: !field.required };
  }
  return new ParquetSchema(fields);
}

function _extractStatsFromWriter(
  writer: any,
  schema: IcebergSchema
): Omit<AddFile, 'file' | 'partitions' | 'fileSize' | 'recordCount'> {
  const rowGroups = writer.envelopeWriter?.rowGroups || [];

  const columnSizes: Record<string, bigint> = {};
  const valueCounts: Record<string, bigint> = {};
  const nullValueCounts: Record<string, bigint> = {};
  const lowerBounds: Record<string, Buffer> = {};
  const upperBounds: Record<string, Buffer> = {};

  for (const rg of rowGroups) {
    for (const column of rg.columns) {
      const fieldName = column.meta_data?.path_in_schema?.[0];
      if (fieldName && column.meta_data) {
        const compressedSize = column.meta_data.total_compressed_size;
        if (compressedSize != null) {
          columnSizes[fieldName] =
            (columnSizes[fieldName] ?? 0n) + BigInt(compressedSize);
        }

        const numValues = column.meta_data.num_values;
        if (numValues != null) {
          valueCounts[fieldName] =
            (valueCounts[fieldName] ?? 0n) + BigInt(numValues);
        }

        const stats = column.meta_data.statistics;
        if (stats) {
          const nullCount = stats.null_count;
          if (nullCount != null) {
            nullValueCounts[fieldName] =
              (nullValueCounts[fieldName] ?? 0n) + BigInt(nullCount);
          }

          const field = schema.fields.find((f) => f.name === fieldName);
          const fieldType =
            field && typeof field.type === 'string' ? field.type : null;

          if (stats.min_value) {
            const minBuf = _encodeStatValue(stats.min_value, fieldType);
            if (
              minBuf &&
              (!lowerBounds[fieldName] ||
                Buffer.compare(minBuf, lowerBounds[fieldName]) < 0)
            ) {
              lowerBounds[fieldName] = minBuf;
            }
          }

          if (stats.max_value) {
            const maxBuf = _encodeStatValue(stats.max_value, fieldType);
            if (
              maxBuf &&
              (!upperBounds[fieldName] ||
                Buffer.compare(maxBuf, upperBounds[fieldName]) > 0)
            ) {
              upperBounds[fieldName] = maxBuf;
            }
          }
        }
      }
    }
  }

  return {
    columnSizes: Object.keys(columnSizes).length > 0 ? columnSizes : null,
    valueCounts: Object.keys(valueCounts).length > 0 ? valueCounts : null,
    nullValueCounts:
      Object.keys(nullValueCounts).length > 0 ? nullValueCounts : null,
    lowerBounds: Object.keys(lowerBounds).length > 0 ? lowerBounds : null,
    upperBounds: Object.keys(upperBounds).length > 0 ? upperBounds : null,
  };
}

function _encodeStatValue(
  value: Buffer | string,
  fieldType: string | null
): Buffer | null {
  if (Buffer.isBuffer(value)) {
    return value;
  }
  if (typeof value === 'string') {
    return Buffer.from(value, 'utf8');
  }
  return null;
}

function _createJsonLineTransform(schema: IcebergSchema): Transform {
  let buffer = '';
  return new Transform({
    objectMode: false,
    writableObjectMode: false,
    readableObjectMode: true,
    transform(chunk: Buffer, _encoding, callback) {
      buffer += chunk.toString('utf8');
      const lines = buffer.split('\n');
      buffer = lines.pop() ?? '';
      for (const line of lines) {
        if (line.trim()) {
          const json = parse(line) as Record<string, unknown>;
          this.push(_normalizeRow(json, schema));
        }
      }
      callback();
    },
    flush(callback) {
      if (buffer.trim()) {
        const json = parse(buffer) as Record<string, unknown>;
        this.push(_normalizeRow(json, schema));
      }
      callback();
    },
  });
}

function _normalizeRow(
  json: Record<string, unknown>,
  schema: IcebergSchema
): Record<string, unknown> {
  const row: Record<string, unknown> = {};
  for (const field of schema.fields) {
    const value = json[field.name];
    const type = typeof field.type === 'string' ? field.type : 'string';

    if (value === null || value === undefined) {
      if (field.required) {
        row[field.name] = _getDefaultValue(type);
      } else {
        row[field.name] = null;
      }
    } else if (type === 'timestamp' || type === 'timestamptz') {
      row[field.name] = new Date(value as string);
    } else if (type === 'date') {
      row[field.name] = new Date(value as string);
    } else if (type === 'int' || type === 'long') {
      row[field.name] = Number(value);
    } else if (type === 'float' || type === 'double') {
      row[field.name] = Number(value);
    } else {
      row[field.name] = value;
    }
  }
  return row;
}

function _getDefaultValue(type: string): unknown {
  switch (type) {
    case 'int':
    case 'long':
    case 'float':
    case 'double':
      return 0;
    case 'boolean':
      return false;
    case 'timestamp':
    case 'timestamptz':
    case 'date':
      return new Date(0);
    default:
      return '';
  }
}

function _findSpec(metadata: IcebergMetadata, keys: string[]): number {
  if (keys.length === 0) {
    return 0;
  }
  for (const spec of metadata['partition-specs']) {
    if (spec.fields.length === keys.length) {
      if (keys.every((key) => spec.fields.find((f) => f.name === key))) {
        return spec['spec-id'];
      }
    }
  }
  throw new Error(`spec not found for keys ${keys.join(', ')}`);
}

function _findSchema(
  metadata: IcebergMetadata,
  manifest: RedshiftManifest
): number {
  const { elements } = manifest.schema;
  for (const schema of metadata.schemas) {
    if (
      schema.fields.every(
        (f) => !f.required || elements.find((e) => e.name === f.name)
      )
    ) {
      return schema['schema-id'];
    }
  }
  throw new Error('schema not found for schema.elements');
}
