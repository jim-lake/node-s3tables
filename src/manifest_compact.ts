import { randomBytes, randomUUID } from 'node:crypto';

import { asyncIterMap } from './async_iter_map';
import {
  ManifestListSchema,
  ManifestListType,
  makeManifestSchema,
  makeManifestType,
} from './avro_schema';
import { ListContent } from './avro_types';
import { getMetadata } from './metadata';
import { parseS3Url, downloadAvro, streamWriteAvro } from './s3_tools';
import { submitSnapshot } from './snapshot';

import type { AwsCredentialIdentity } from '@aws-sdk/types';
import type { Schema } from 'avsc';
import type { IcebergPartitionSpec, IcebergSchema } from './iceberg';
import type { ManifestListRecord, ManifestFileRecord } from './avro_types';
import type { SubmitSnapshotResult } from './snapshot';

export type CalculateWeightFunction = (group: ManifestListRecord[]) => number;

export interface ManifestCompactParams {
  credentials?: AwsCredentialIdentity;
  tableBucketARN: string;
  namespace: string;
  name: string;
  snapshotId?: bigint;
  targetCount?: number;
  calculateWeight?: CalculateWeightFunction;
  forceRewrite?: boolean;
  retryCount?: number;
  maxSnapshots?: number;
}
export interface ManifestCompactResult extends SubmitSnapshotResult {
  changed: boolean;
  outputManifestCount: number;
}
export async function manifestCompact(
  params: ManifestCompactParams
): Promise<ManifestCompactResult> {
  const { credentials, targetCount, calculateWeight } = params;
  const region = params.tableBucketARN.split(':')[3];
  if (!region) {
    throw new Error('bad tableBucketARN');
  }
  const snapshot_id = params.snapshotId ?? _randomBigInt64();
  const metadata = await getMetadata(params);
  const bucket = metadata.location.split('/').slice(-1)[0];
  const parent_snapshot_id = BigInt(metadata['current-snapshot-id']);
  const snapshot =
    metadata.snapshots.find(
      (s) => BigInt(s['snapshot-id']) === parent_snapshot_id
    ) ?? null;
  if (!bucket) {
    throw new Error('bad manifest location');
  }
  if (!snapshot) {
    return {
      result: {},
      retriesNeeded: 0,
      parentSnapshotId: parent_snapshot_id,
      snapshotId: 0n,
      sequenceNumber: 0n,
      changed: false,
      outputManifestCount: 0,
    };
  }
  if (parent_snapshot_id <= 0n) {
    throw new Error('no old snapshot');
  }
  const old_list_key = parseS3Url(snapshot['manifest-list']).key;
  if (!old_list_key) {
    throw new Error('last snapshot invalid');
  }
  const sequence_number = BigInt(metadata['last-sequence-number']) + 1n;
  let remove_snapshot_id = 0n;
  if (params.maxSnapshots && metadata.snapshots.length >= params.maxSnapshots) {
    let earliest_time = 0;
    for (const snap of metadata.snapshots) {
      const snap_time = snap['timestamp-ms'];
      if (earliest_time === 0 || snap_time < earliest_time) {
        earliest_time = snap_time;
        remove_snapshot_id = BigInt(snap['snapshot-id']);
      }
    }
  }

  const list = await downloadAvro<ManifestListRecord>({
    credentials,
    region,
    bucket,
    key: old_list_key,
    avroSchema: ManifestListSchema,
  });
  const filtered = list.filter(_filterDeletes);
  const groups = _groupList(filtered, (a, b) => {
    if (
      a.content === ListContent.DATA &&
      b.content === ListContent.DATA &&
      a.deleted_files_count === 0 &&
      b.deleted_files_count === 0 &&
      a.partition_spec_id === b.partition_spec_id
    ) {
      return (
        !a.partitions ||
        a.partitions.every((part, i) => {
          const other = b.partitions?.[i];
          return (
            other &&
            (part.upper_bound === other.upper_bound ||
              (part.upper_bound &&
                other.upper_bound &&
                Buffer.compare(part.upper_bound, other.upper_bound) === 0)) &&
            (part.lower_bound === other.lower_bound ||
              (part.lower_bound &&
                other.lower_bound &&
                Buffer.compare(part.lower_bound, other.lower_bound) === 0))
          );
        })
      );
    }
    return false;
  });

  const final_groups =
    targetCount !== undefined &&
    calculateWeight !== undefined &&
    groups.length > targetCount
      ? _combineWeightGroups(groups, targetCount, calculateWeight)
      : groups;

  if (final_groups.length === list.length && !params.forceRewrite) {
    return {
      result: {},
      retriesNeeded: 0,
      parentSnapshotId: parent_snapshot_id,
      snapshotId: 0n,
      sequenceNumber: sequence_number,
      changed: false,
      outputManifestCount: 0,
    };
  }
  const manifest_list_key = `metadata/${randomUUID()}.avro`;
  const iter = asyncIterMap(final_groups, async (group) => {
    if (!group[0]) {
      return [];
    }
    const { partition_spec_id } = group[0];
    const spec = metadata['partition-specs'].find(
      (p) => p['spec-id'] === partition_spec_id
    );
    if (!spec) {
      throw new Error(`Partition spec not found: ${partition_spec_id}`);
    }
    return _combineGroup({
      credentials,
      region,
      bucket,
      group,
      spec,
      snapshotId: snapshot_id,
      schemas: metadata.schemas,
      sequenceNumber: sequence_number,
      forceRewrite: params.forceRewrite ?? false,
    });
  });
  await streamWriteAvro<ManifestListRecord>({
    credentials,
    region,
    bucket,
    key: manifest_list_key,
    metadata: {
      'sequence-number': String(sequence_number),
      'snapshot-id': String(snapshot_id),
      'parent-snapshot-id': String(parent_snapshot_id),
    },
    avroType: ManifestListType,
    iter,
  });

  const summary = {
    operation: 'replace',
    'added-data-files': '0',
    'deleted-data-files': '0',
    'added-records': '0',
    'deleted-records': '0',
    'added-files-size': '0',
    'removed-files-size': '0',
    'changed-partition-count': '0',
  };
  const snap_result = await submitSnapshot({
    credentials,
    tableBucketARN: params.tableBucketARN,
    namespace: params.namespace,
    name: params.name,
    currentSchemaId: metadata['current-schema-id'],
    parentSnapshotId: parent_snapshot_id,
    snapshotId: snapshot_id,
    sequenceNumber: sequence_number,
    manifestListUrl: `s3://${bucket}/${manifest_list_key}`,
    summary,
    removeSnapshotId: remove_snapshot_id,
    retryCount: params.retryCount,
  });
  return {
    ...snap_result,
    changed: true,
    outputManifestCount: final_groups.length,
  };
}
interface CombineGroupParams {
  credentials?: AwsCredentialIdentity | undefined;
  region: string;
  bucket: string;
  snapshotId: bigint;
  sequenceNumber: bigint;
  group: ManifestListRecord[];
  forceRewrite: boolean;
  spec: IcebergPartitionSpec;
  schemas: IcebergSchema[];
}
async function _combineGroup(
  params: CombineGroupParams
): Promise<ManifestListRecord[]> {
  const { credentials, region, bucket, group } = params;
  const record0 = group[0];
  if ((group.length === 1 && !params.forceRewrite) || !record0) {
    return group;
  }
  const key = `metadata/${randomUUID()}.avro`;
  const schema = makeManifestSchema(params.spec, params.schemas);
  const type = makeManifestType(params.spec, params.schemas);
  const iter = asyncIterMap(group, async (record) => {
    return _streamReadManifest({
      credentials,
      region,
      bucket,
      url: record.manifest_path,
      schema,
    });
  });
  const manifest_length = await streamWriteAvro<ManifestFileRecord>({
    credentials,
    region,
    bucket,
    key,
    metadata: {
      'partition-spec-id': String(params.spec['spec-id']),
      'partition-spec': JSON.stringify(params.spec.fields),
    },
    avroType: type,
    iter,
  });
  const ret: ManifestListRecord = {
    manifest_path: `s3://${bucket}/${key}`,
    manifest_length: BigInt(manifest_length),
    partition_spec_id: record0.partition_spec_id,
    content: record0.content,
    sequence_number: params.sequenceNumber,
    min_sequence_number: params.sequenceNumber,
    added_snapshot_id: params.snapshotId,
    added_files_count: 0,
    existing_files_count: 0,
    deleted_files_count: 0,
    added_rows_count: 0n,
    existing_rows_count: 0n,
    deleted_rows_count: 0n,
    partitions: record0.partitions ?? null,
  };
  for (const record of group) {
    ret.added_files_count += record.added_files_count;
    ret.existing_files_count += record.existing_files_count;
    ret.deleted_files_count += record.deleted_files_count;
    ret.added_rows_count += record.added_rows_count;
    ret.existing_rows_count += record.existing_rows_count;
    ret.deleted_rows_count += record.deleted_rows_count;
    ret.min_sequence_number = _bigintMin(
      ret.min_sequence_number,
      record.min_sequence_number
    );
  }
  for (let i = 1; i < group.length; i++) {
    const parts = group[i]?.partitions;
    if (ret.partitions && parts) {
      for (let j = 0; j < parts.length; j++) {
        const part = parts[j];
        const ret_part = ret.partitions[j];
        if (part && ret_part) {
          ret_part.contains_null ||= part.contains_null;
          if (part.contains_nan !== undefined) {
            ret_part.contains_nan =
              (ret_part.contains_nan ?? false) || part.contains_nan;
          }
          if (
            !ret_part.upper_bound ||
            (part.upper_bound &&
              Buffer.compare(part.upper_bound, ret_part.upper_bound) > 0)
          ) {
            ret_part.upper_bound = part.upper_bound ?? null;
          }
          if (
            !ret_part.lower_bound ||
            (part.lower_bound &&
              Buffer.compare(part.lower_bound, ret_part.lower_bound) < 0)
          ) {
            ret_part.lower_bound = part.lower_bound ?? null;
          }
        }
      }
    } else if (parts) {
      ret.partitions = parts;
    }
  }
  return [ret];
}
interface StreamReadManifestParams {
  credentials?: AwsCredentialIdentity | undefined;
  region: string;
  bucket: string;
  url: string;
  schema: Schema;
}
async function _streamReadManifest(
  params: StreamReadManifestParams
): Promise<ManifestFileRecord[]> {
  let bucket: string | undefined = params.bucket;
  let key: string | undefined = params.url;
  if (params.url.startsWith('s3://')) {
    const parsed = parseS3Url(params.url);
    bucket = parsed.bucket;
    key = parsed.key;
  }
  if (!bucket || !key) {
    throw new Error(`invalid manfiest url: ${params.url}`);
  }
  return downloadAvro<ManifestFileRecord>({
    credentials: params.credentials,
    region: params.region,
    bucket,
    key,
    avroSchema: params.schema,
  });
}
function _filterDeletes(record: ManifestListRecord) {
  return (
    record.content === ListContent.DATA &&
    record.added_files_count === 0 &&
    record.existing_files_count === 0
  );
}
function _groupList<T>(list: T[], compare: (a: T, b: T) => boolean): T[][] {
  const ret: T[][] = [];
  for (const item of list) {
    let added = false;
    for (const group of ret) {
      if (group[0] && compare(group[0], item)) {
        group.push(item);
        added = true;
        break;
      }
    }
    if (!added) {
      ret.push([item]);
    }
  }
  return ret;
}
function _combineWeightGroups(
  groups: ManifestListRecord[][],
  targetCount: number,
  calculateWeight: CalculateWeightFunction
): ManifestListRecord[][] {
  const weighted_groups = groups.map((group) => ({
    group,
    weight: calculateWeight(group),
  }));
  weighted_groups.sort(_sortGroup);
  while (weighted_groups.length > targetCount) {
    const remove_item = weighted_groups.shift();
    if (!remove_item) {
      break;
    }
    for (const item of remove_item.group) {
      weighted_groups[0]?.group.push(item);
    }
  }
  return weighted_groups.map((g) => g.group);
}
interface WeightedObject {
  weight: number;
}
function _sortGroup(a: WeightedObject, b: WeightedObject) {
  return a.weight - b.weight;
}
function _randomBigInt64(): bigint {
  const bytes = randomBytes(8);
  let ret = bytes.readBigUInt64BE();
  ret &= BigInt('0x7FFFFFFFFFFFFFFF');
  if (ret === 0n) {
    ret = 1n;
  }
  return ret;
}
function _bigintMin(value0: bigint, ...values: bigint[]): bigint {
  let ret = value0;
  for (const val of values) {
    if (val < ret) {
      ret = val;
    }
  }
  return ret;
}
