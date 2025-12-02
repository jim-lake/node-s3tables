import { randomUUID } from 'node:crypto';
import { avroToBuffer } from './avro_helper';
import { makeManifestType } from './avro_schema';
import { makeBounds } from './avro_transform';
import { writeS3File } from './s3_tools';
import { ManifestFileStatus, DataFileContent, ListContent } from './avro_types';

import type { AwsCredentialIdentity } from '@aws-sdk/types';
import type {
  ManifestFileRecord,
  ManifestListRecord,
  PartitionRecord,
  PartitionSummary,
} from './avro_types';
import type { IcebergMetadata, IcebergSchema } from './iceberg';

export interface AddFile {
  file: string;
  partitions: PartitionRecord;
  fileSize: bigint;
  recordCount: bigint;
  columnSizes?: Record<string, bigint> | null | undefined;
  valueCounts?: Record<string, bigint> | null | undefined;
  nullValueCounts?: Record<string, bigint> | null | undefined;
  nanValueCounts?: Record<string, bigint> | null | undefined;
  lowerBounds?: Record<string, Buffer> | null | undefined;
  upperBounds?: Record<string, Buffer> | null | undefined;
  keyMetadata?: Buffer | null | undefined;
  splitOffsets?: bigint[] | null | undefined;
  equalityIds?: number[] | null | undefined;
  sortOrderId?: number | null | undefined;
}
export interface AddManifestParams {
  credentials?: AwsCredentialIdentity | undefined;
  region: string;
  metadata: IcebergMetadata;
  schemaId: number;
  specId: number;
  snapshotId: bigint;
  sequenceNumber: bigint;
  files: AddFile[];
}
export async function addManifest(
  params: AddManifestParams
): Promise<ManifestListRecord> {
  const { credentials, region, metadata } = params;
  const bucket = metadata.location.split('/').slice(-1)[0];
  const schema = metadata.schemas.find(
    (s) => s['schema-id'] === params.schemaId
  );
  const spec = metadata['partition-specs'].find(
    (p) => p['spec-id'] === params.specId
  );
  if (!bucket) {
    throw new Error('bad manifest location');
  }
  if (!schema) {
    throw new Error('schema not found');
  }
  if (!spec) {
    throw new Error('partition spec not found');
  }
  if (!params.files[0]) {
    throw new Error('must have at least 1 file');
  }

  let added_rows_count = 0n;
  const partitions: PartitionSummary[] = spec.fields.map(() => ({
    contains_null: false,
    contains_nan: false,
    upper_bound: null,
    lower_bound: null,
  }));
  const records = params.files.map((file): ManifestFileRecord => {
    added_rows_count += file.recordCount;
    const bounds = makeBounds(file.partitions, spec, schema);
    for (let i = 0; i < partitions.length; i++) {
      const part = partitions[i];
      const bound = bounds[i];
      if (!part) {
        throw new Error('impossible');
      } else if (bound === null) {
        part.contains_null = true;
      } else if (Buffer.isBuffer(bound)) {
        part.upper_bound = _maxBuffer(part.upper_bound ?? null, bound);
        part.lower_bound = _minBuffer(part.lower_bound ?? null, bound);
      } else {
        part.contains_nan = true;
      }
    }
    return {
      status: ManifestFileStatus.ADDED,
      snapshot_id: params.snapshotId,
      sequence_number: params.sequenceNumber,
      file_sequence_number: params.sequenceNumber,
      data_file: {
        content: DataFileContent.DATA,
        file_path: file.file,
        file_format: 'PARQUET',
        record_count: file.recordCount,
        file_size_in_bytes: file.fileSize,
        partition: file.partitions,
        column_sizes: _transformRecord(schema, file.columnSizes),
        value_counts: _transformRecord(schema, file.valueCounts),
        null_value_counts: _transformRecord(schema, file.nullValueCounts),
        nan_value_counts: _transformRecord(schema, file.nanValueCounts),
        lower_bounds: _transformRecord(schema, file.lowerBounds),
        upper_bounds: _transformRecord(schema, file.upperBounds),
        key_metadata: file.keyMetadata ?? null,
        split_offsets: file.splitOffsets ?? null,
        equality_ids: file.equalityIds ?? null,
        sort_order_id: file.sortOrderId ?? null,
      },
    };
  });
  const manifest_type = makeManifestType(spec, schema);
  const manifest_buf = await avroToBuffer({
    type: manifest_type,
    metadata: {
      'partition-spec-id': String(params.specId),
      'partition-spec': JSON.stringify(spec.fields),
    },
    records,
  });
  const manifest_key = `metadata/${randomUUID()}.avro`;
  await writeS3File({
    credentials,
    region,
    bucket,
    key: manifest_key,
    body: manifest_buf,
  });

  const manifest_record = {
    manifest_path: `s3://${bucket}/${manifest_key}`,
    manifest_length: BigInt(manifest_buf.length),
    partition_spec_id: params.specId,
    content: ListContent.DATA,
    sequence_number: params.sequenceNumber,
    min_sequence_number: params.sequenceNumber,
    added_snapshot_id: params.snapshotId,
    added_files_count: params.files.length,
    existing_files_count: 0,
    deleted_files_count: 0,
    added_rows_count,
    existing_rows_count: 0n,
    deleted_rows_count: 0n,
    partitions,
  };
  return manifest_record;
}
function _transformRecord<T>(
  schema: IcebergSchema,
  map: Record<string, T> | null | undefined
): { key: number; value: T }[] | null {
  if (!map) {
    return null;
  }
  const ret: { key: number; value: T }[] = [];
  for (const field of schema.fields) {
    const value = map[field.name];
    if (value !== undefined) {
      ret.push({ key: field.id, value });
    }
  }
  return ret.length > 0 ? ret : null;
}
function _minBuffer(a: Buffer | null, b: Buffer | null): Buffer | null {
  if (!a && !b) {
    return null;
  } else if (!a) {
    return b;
  } else if (!b) {
    return a;
  }
  return Buffer.compare(a, b) <= 0 ? a : b;
}
function _maxBuffer(a: Buffer | null, b: Buffer | null): Buffer | null {
  if (!a && !b) {
    return null;
  } else if (!a) {
    return b;
  } else if (!b) {
    return a;
  }
  return Buffer.compare(a, b) >= 0 ? a : b;
}
