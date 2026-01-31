import { randomUUID } from 'node:crypto';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import { ParquetReader } from 'parquetjs';
import { addDataFiles } from './add_data_files';
import { encodeValue } from './avro_transform';
import { parse } from './json';
import { getMetadata } from './metadata';
import { rewriteParquet } from './parquet_tools';
import { parseS3Url, getS3Client } from './s3_tools';

import type { AwsCredentialIdentity } from '@aws-sdk/types';
import type { AddDataFilesResult, AddFileList } from './add_data_files';
import type { AddFile } from './manifest';
import type { IcebergMetadata, IcebergSchema } from './iceberg';

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
  rewriteParquet?: boolean;
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
    const file = url.split('/').pop() ?? '';
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
    const { s3Url, stats } = await _maybeMoveFile({
      credentials,
      region,
      bucket,
      key,
      url,
      schema,
      rewriteParquet: params.rewriteParquet,
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
interface MaybeMoveFileParams {
  credentials?: AwsCredentialIdentity | undefined;
  region: string;
  bucket: string;
  key: string;
  url: string;
  schema: IcebergSchema;
  rewriteParquet?: boolean | undefined;
  partitions?: Record<string, string>;
}
interface MaybeMoveFileResult {
  s3Url: string;
  stats: Omit<AddFile, 'file' | 'partitions'>;
}
async function _maybeMoveFile(
  params: MaybeMoveFileParams
): Promise<MaybeMoveFileResult> {
  const { bucket: sourceBucket, key: sourceKey } = parseS3Url(params.url);
  if (!sourceBucket || !sourceKey) {
    throw new Error(`bad entry url: ${params.url}`);
  }
  const s3_client = getS3Client(params);

  let s3Url: string;
  let stats: Omit<AddFile, 'file' | 'partitions'>;

  if (params.rewriteParquet) {
    const result = await rewriteParquet({
      inputS3: { Bucket: sourceBucket, Key: sourceKey },
      schema: params.schema,
      partitions: params.partitions,
      s3Client: s3_client,
      bucket: params.bucket,
      key: params.key,
    });
    s3Url = `s3://${params.bucket}/${params.key}`;
    stats = { ...result.stats, fileSize: result.fileSize };
  } else {
    const get = new GetObjectCommand({ Bucket: sourceBucket, Key: sourceKey });
    const { Body } = await s3_client.send(get);
    if (!Body) {
      throw new Error(`body missing for file: ${params.url}`);
    }
    const fileBuffer = Buffer.from(await Body.transformToByteArray());
    stats = await _extractStats(fileBuffer, params.schema);
    if (sourceBucket === params.bucket) {
      s3Url = params.url;
    } else {
      const upload = new Upload({
        client: s3_client,
        params: { Bucket: params.bucket, Key: params.key, Body: fileBuffer },
      });
      await upload.done();
      s3Url = `s3://${params.bucket}/${params.key}`;
    }
  }
  return { s3Url, stats };
}
async function _extractStats(
  fileBuffer: Buffer,
  schema: IcebergSchema
): Promise<Omit<AddFile, 'file' | 'partitions'>> {
  const reader = await ParquetReader.openBuffer(fileBuffer);
  const rowGroups = reader.metadata.row_groups;

  const columnSizes: Record<string, bigint> = {};
  const valueCounts: Record<string, bigint> = {};
  const nullValueCounts: Record<string, bigint> = {};
  const lowerBounds: Record<string, Buffer> = {};
  const upperBounds: Record<string, Buffer> = {};
  let recordCount = 0n;

  for (const rg of rowGroups) {
    for (const column of rg.columns) {
      const fieldName = column.meta_data?.path_in_schema?.[0];
      if (fieldName && column.meta_data) {
        const compressedSize = column.meta_data.total_compressed_size;
        if (compressedSize?.buffer) {
          columnSizes[fieldName] =
            (columnSizes[fieldName] ?? 0n) +
            compressedSize.buffer.readBigInt64BE(compressedSize.offset);
        }
        const numValues = column.meta_data.num_values;
        if (numValues?.buffer) {
          const count = numValues.buffer.readBigInt64BE(numValues.offset);
          valueCounts[fieldName] = (valueCounts[fieldName] ?? 0n) + count;
          if (recordCount < count) {
            recordCount = count;
          }
        }
        const stats = column.meta_data.statistics;
        if (stats) {
          const nullCount = stats.null_count;
          if (nullCount?.buffer) {
            nullValueCounts[fieldName] =
              (nullValueCounts[fieldName] ?? 0n) +
              nullCount.buffer.readBigInt64BE(nullCount.offset);
          }
          const field = schema.fields.find((f) => f.name === fieldName);
          const fieldType =
            field && typeof field.type === 'string' ? field.type : null;
          const minBuf = encodeValue(
            stats.min_value ?? null,
            'identity',
            fieldType
          );
          const maxBuf = encodeValue(
            stats.max_value ?? null,
            'identity',
            fieldType
          );
          if (
            minBuf &&
            (!lowerBounds[fieldName] ||
              Buffer.compare(minBuf, lowerBounds[fieldName]) < 0)
          ) {
            lowerBounds[fieldName] = minBuf;
          }
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
  await reader.close();

  return {
    fileSize: BigInt(fileBuffer.length),
    recordCount,
    columnSizes: Object.keys(columnSizes).length > 0 ? columnSizes : null,
    valueCounts: Object.keys(valueCounts).length > 0 ? valueCounts : null,
    nullValueCounts:
      Object.keys(nullValueCounts).length > 0 ? nullValueCounts : null,
    lowerBounds: Object.keys(lowerBounds).length > 0 ? lowerBounds : null,
    upperBounds: Object.keys(upperBounds).length > 0 ? upperBounds : null,
  };
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
