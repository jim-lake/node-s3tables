import { randomBytes, randomUUID } from 'node:crypto';
import { avroToBuffer } from './avro_helper';
import { ManifestListType } from './avro_schema';
import { addManifest } from './manifest';
import { getMetadata } from './metadata';
import { parseS3Url, writeS3File, updateManifestList } from './s3_tools';
import { submitSnapshot } from './snapshot';

import type { AwsCredentialIdentity } from '@aws-sdk/types';
import type { AddFile } from './manifest';
import type { IcebergSnapshot } from './iceberg';
import type { JSONObject } from './json';

export default { addDataFiles };

export interface AddFileList {
  specId: number;
  schemaId: number;
  files: AddFile[];
}
export interface AddDataFilesParams {
  credentials?: AwsCredentialIdentity | undefined;
  tableBucketARN: string;
  namespace: string;
  name: string;
  snapshotId?: bigint;
  lists: AddFileList[];
  retryCount?: number | undefined;
  maxSnapshots?: number;
}
export interface AddDataFilesResult {
  result: JSONObject;
  retriesNeeded: number;
  parentSnapshotId: bigint;
  snapshotId: bigint;
  sequenceNumber: bigint;
}
export async function addDataFiles(
  params: AddDataFilesParams
): Promise<AddDataFilesResult> {
  const { credentials } = params;
  const region = params.tableBucketARN.split(':')[3];
  if (!region) {
    throw new Error('bad tableBucketARN');
  }
  const snapshot_id = params.snapshotId ?? _randomBigInt64();
  const metadata = await getMetadata(params);
  const bucket = metadata.location.split('/').slice(-1)[0];
  if (!bucket) {
    throw new Error('bad manifest location');
  }
  const parent_snapshot_id = BigInt(metadata['current-snapshot-id']);
  const snapshot =
    metadata.snapshots.find(
      (s) => BigInt(s['snapshot-id']) === parent_snapshot_id
    ) ?? null;
  if (parent_snapshot_id > 0n && !snapshot) {
    throw new Error('no old snapshot');
  }
  let old_list_key = snapshot ? parseS3Url(snapshot['manifest-list']).key : '';
  if (snapshot && !old_list_key) {
    throw new Error('last snapshot invalid');
  }
  let sequence_number = BigInt(metadata['last-sequence-number']) + 1n;
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

  let added_files = 0;
  let added_records = 0n;
  let added_size = 0n;
  const records = await Promise.all(
    params.lists.map(async (list) => {
      added_files += list.files.length;
      for (const file of list.files) {
        added_records += file.recordCount;
        added_size += file.fileSize;
      }
      const opts = {
        credentials,
        region,
        metadata,
        schemaId: list.schemaId,
        specId: list.specId,
        snapshotId: snapshot_id,
        sequenceNumber: sequence_number,
        files: list.files,
      };
      return addManifest(opts);
    })
  );

  async function createManifestList() {
    if (!bucket) {
      throw new Error('bad manifest location');
    }
    if (!region) {
      throw new Error('bad tableBucketARN');
    }
    const manifest_list_key = `metadata/${randomUUID()}.avro`;
    const url = `s3://${bucket}/${manifest_list_key}`;
    if (old_list_key) {
      await updateManifestList({
        credentials,
        region,
        bucket,
        key: old_list_key,
        outKey: manifest_list_key,
        metadata: {
          'sequence-number': String(sequence_number),
          'snapshot-id': String(snapshot_id),
          'parent-snapshot-id': String(parent_snapshot_id),
        },
        prepend: records,
      });
    } else {
      const manifest_list_buf = await avroToBuffer({
        type: ManifestListType,
        metadata: {
          'sequence-number': String(sequence_number),
          'snapshot-id': String(snapshot_id),
          'parent-snapshot-id': 'null',
        },
        records,
      });
      await writeS3File({
        credentials,
        region,
        bucket,
        key: manifest_list_key,
        body: manifest_list_buf,
      });
    }
    return url;
  }

  const manifest_list_url = await createManifestList();

  async function resolveConflict(conflict_snap: IcebergSnapshot) {
    if (
      conflict_snap.summary.operation === 'append' &&
      BigInt(conflict_snap['sequence-number']) === sequence_number
    ) {
      old_list_key = parseS3Url(conflict_snap['manifest-list']).key;
      if (!old_list_key) {
        throw new Error('conflict');
      }
      added_files += parseInt(
        conflict_snap.summary['added-data-files'] ?? '0',
        10
      );
      added_records += BigInt(conflict_snap.summary['added-records'] ?? '0');
      added_size += BigInt(conflict_snap.summary['added-files-size'] ?? '0');
      sequence_number++;

      const url = await createManifestList();
      return {
        manifestListUrl: url,
        summary: {
          operation: 'append',
          'added-data-files': String(added_files),
          'added-records': String(added_records),
          'added-files-size': String(added_size),
        },
      };
    }
    throw new Error('conflict');
  }

  return submitSnapshot({
    credentials,
    tableBucketARN: params.tableBucketARN,
    namespace: params.namespace,
    name: params.name,
    currentSchemaId: metadata['current-schema-id'],
    parentSnapshotId: parent_snapshot_id,
    snapshotId: snapshot_id,
    sequenceNumber: sequence_number,
    retryCount: params.retryCount,
    removeSnapshotId: remove_snapshot_id,
    manifestListUrl: manifest_list_url,
    summary: {
      operation: 'append',
      'added-data-files': String(added_files),
      'added-records': String(added_records),
      'added-files-size': String(added_size),
    },
    resolveConflict,
  });
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
