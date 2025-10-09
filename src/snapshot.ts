import { randomBytes, randomUUID } from 'node:crypto';
import { avroToBuffer } from './avro_helper';
import { ManifestListType } from './avro_schema';
import { addManifest } from './manifest';
import { getMetadata } from './metadata';
import { icebergRequest, IcebergHttpError } from './request';
import { parseS3Url, writeS3File, updateManifestList } from './s3_tools';

import type { AwsCredentialIdentity } from '@aws-sdk/types';
import type { JSONObject } from './json';
import type { AddFile } from './manifest';

export default { addDataFiles };

const DEFAULT_RETRY_COUNT = 5;

export interface AddFileList {
  specId: number;
  schemaId: number;
  files: AddFile[];
}
export interface AddDataFilesParams {
  credentials?: AwsCredentialIdentity;
  tableBucketARN: string;
  namespace: string;
  name: string;
  lists: AddFileList[];
  retryCount?: number;
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
  const retry_max = params.retryCount ?? DEFAULT_RETRY_COUNT;
  const region = params.tableBucketARN.split(':')[3];
  if (!region) {
    throw new Error('bad tableBucketARN');
  }
  const snapshot_id = _randomBigInt64();
  const metadata = await getMetadata(params);
  const bucket = metadata.location.split('/').slice(-1)[0];
  const parent_snapshot_id = BigInt(metadata['current-snapshot-id']);
  const snapshot =
    metadata.snapshots.find((s) => s['snapshot-id'] === parent_snapshot_id) ??
    null;
  if (!bucket) {
    throw new Error('bad manifest location');
  }
  if (parent_snapshot_id > 0n && !snapshot) {
    throw new Error('no old snapshot');
  }
  let old_list_key = snapshot ? parseS3Url(snapshot['manifest-list']).key : '';
  if (snapshot && !old_list_key) {
    throw new Error('last snapshot invalid');
  }
  let sequence_number =
    BigInt(
      metadata.snapshots.reduce(
        (memo, s) =>
          s['sequence-number'] > memo ? s['sequence-number'] : memo,
        0
      )
    ) + 1n;

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
  let expected_snapshot_id = parent_snapshot_id;

  for (let try_count = 0; ; try_count++) {
    const manifest_list_key = `metadata/${randomUUID()}.avro`;
    const manifest_list_url = `s3://${bucket}/${manifest_list_key}`;
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
    try {
      const result = await icebergRequest({
        credentials: params.credentials,
        tableBucketARN: params.tableBucketARN,
        method: 'POST',
        suffix: `/namespaces/${params.namespace}/tables/${params.name}`,
        body: {
          requirements:
            expected_snapshot_id > 0n
              ? [
                  {
                    type: 'assert-ref-snapshot-id',
                    ref: 'main',
                    'snapshot-id': expected_snapshot_id,
                  },
                ]
              : [],
          updates: [
            {
              action: 'add-snapshot',
              snapshot: {
                'sequence-number': sequence_number,
                'snapshot-id': snapshot_id,
                'parent-snapshot-id': parent_snapshot_id,
                'timestamp-ms': Date.now(),
                summary: {
                  operation: 'append',
                  'added-data-files': String(added_files),
                  'added-records': String(added_records),
                  'added-files-size': String(added_size),
                },
                'manifest-list': manifest_list_url,
                'schema-id': metadata['current-schema-id'],
              },
            },
            {
              action: 'set-snapshot-ref',
              'snapshot-id': snapshot_id,
              type: 'branch',
              'ref-name': 'main',
            },
          ],
        },
      });
      return {
        result,
        retriesNeeded: try_count,
        parentSnapshotId: parent_snapshot_id,
        snapshotId: snapshot_id,
        sequenceNumber: sequence_number,
      };
    } catch (e) {
      if (
        e instanceof IcebergHttpError &&
        e.status === 409 &&
        try_count < retry_max
      ) {
        // retry case
      } else {
        throw e;
      }
    }

    // we do a merge in the append only simultanious case
    const conflict_metadata = await getMetadata(params);
    const conflict_snapshot_id = BigInt(
      conflict_metadata['current-snapshot-id']
    );
    if (conflict_snapshot_id <= 0n) {
      throw new Error('conflict');
    }
    const conflict_snap = conflict_metadata.snapshots.find(
      (s) => s['snapshot-id'] === conflict_snapshot_id
    );
    if (!conflict_snap) {
      throw new Error('conflict');
    }
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

      expected_snapshot_id = conflict_snapshot_id;
      sequence_number++;
    } else {
      throw new Error('conflict');
    }
  }
}
export interface SetCurrentCommitParams {
  credentials?: AwsCredentialIdentity;
  tableBucketARN: string;
  namespace: string;
  name: string;
  snapshotId: bigint;
}
export async function setCurrentCommit(params: SetCurrentCommitParams) {
  const commit_result = await icebergRequest({
    credentials: params.credentials,
    tableBucketARN: params.tableBucketARN,
    method: 'POST',
    suffix: `/namespaces/${params.namespace}/tables/${params.name}`,
    body: {
      updates: [
        {
          action: 'set-snapshot-ref',
          'snapshot-id': params.snapshotId,
          type: 'branch',
          'ref-name': 'main',
        },
      ],
    },
  });
  return commit_result;
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
