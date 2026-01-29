import { randomUUID } from 'node:crypto';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import { Upload } from '@aws-sdk/lib-storage';
import { addDataFiles } from './add_data_files';
import { parse } from './json';
import { getMetadata } from './metadata';
import { parseS3Url, getS3Client } from './s3_tools';

import type { AwsCredentialIdentity } from '@aws-sdk/types';
import type { AddDataFilesResult, AddFileList } from './add_data_files';
import type { IcebergMetadata } from './iceberg';

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
  const metadata = await getMetadata(params);
  const bucket = metadata.location.split('/').slice(-1)[0];
  if (!bucket) {
    throw new Error('bad manifest location');
  }

  const import_prefix = `data/${randomUUID()}/`;
  const lists: AddFileList[] = [];
  for (const entry of manifest.entries) {
    const { url } = entry;
    const { content_length, record_count } = entry.meta;
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

    let list = lists.find(
      (l) => l.schemaId === schemaId && l.specId === specId
    );
    if (!list) {
      list = { specId, schemaId, files: [] };
      lists.push(list);
    }
    const part_path = parts.length > 0 ? `${parts.join('/')}/` : '';
    const key = import_prefix + part_path + file;
    list.files.push({
      file: await _maybeMoveFile({ credentials, region, bucket, key, url }),
      partitions,
      fileSize: BigInt(content_length),
      recordCount: BigInt(record_count),
    });
  }
  return addDataFiles({
    credentials,
    tableBucketARN: params.tableBucketARN,
    namespace: params.namespace,
    name: params.name,
    lists,
    retryCount: params.retryCount,
  });
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
}
async function _maybeMoveFile(params: MaybeMoveFileParams): Promise<string> {
  const { bucket, key } = parseS3Url(params.url);
  if (!bucket || !key) {
    throw new Error(`bad entry url: ${params.url}`);
  }
  if (bucket === params.bucket) {
    return params.url;
  }
  const s3_client = getS3Client(params);
  const get = new GetObjectCommand({ Bucket: bucket, Key: key });
  const { Body } = await s3_client.send(get);
  if (!Body) {
    throw new Error(`body missing for file: ${params.url}`);
  }
  const upload = new Upload({
    client: s3_client,
    params: { Bucket: params.bucket, Key: params.key, Body },
  });
  await upload.done();
  return `s3://${params.bucket}/${params.key}`;
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
