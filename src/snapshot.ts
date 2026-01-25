import { getMetadata } from './metadata';
import { icebergRequest, IcebergHttpError } from './request';

import type { AwsCredentialIdentity } from '@aws-sdk/types';
import type { IcebergSnapshot } from './iceberg';
import type { JSONObject, JSONArray } from './json';

const DEFAULT_RETRY_COUNT = 5;

export interface SubmitSnapshotParams {
  credentials?: AwsCredentialIdentity | undefined;
  tableBucketARN: string;
  namespace: string;
  name: string;
  currentSchemaId: number;
  parentSnapshotId: bigint;
  snapshotId: bigint;
  sequenceNumber: bigint;
  retryCount?: number | undefined;
  removeSnapshotId?: bigint | undefined;
  manifestListUrl: string;
  summary: Record<string, string>;
  resolveConflict?: (
    conflictSnapshot: IcebergSnapshot
  ) => Promise<ResolveConflictResult>;
}
export interface ResolveConflictResult {
  manifestListUrl: string;
  summary: Record<string, string>;
}
export interface SubmitSnapshotResult {
  result: JSONObject;
  retriesNeeded: number;
  parentSnapshotId: bigint;
  snapshotId: bigint;
  sequenceNumber: bigint;
}
export async function submitSnapshot(
  params: SubmitSnapshotParams
): Promise<SubmitSnapshotResult> {
  const { snapshotId, parentSnapshotId, resolveConflict } = params;
  let { sequenceNumber, removeSnapshotId, manifestListUrl, summary } = params;
  const retry_max = params.retryCount ?? DEFAULT_RETRY_COUNT;

  let expected_snapshot_id = parentSnapshotId;
  let conflict_snap: IcebergSnapshot | undefined;
  for (let try_count = 0; ; try_count++) {
    if (conflict_snap && resolveConflict) {
      const resolve_result = await resolveConflict(conflict_snap);
      summary = resolve_result.summary;
      manifestListUrl = resolve_result.manifestListUrl;
    } else if (conflict_snap) {
      throw new Error('conflict');
    }

    try {
      const updates: JSONArray = [
        {
          action: 'add-snapshot',
          snapshot: {
            'sequence-number': sequenceNumber,
            'snapshot-id': snapshotId,
            'parent-snapshot-id': parentSnapshotId,
            'timestamp-ms': Date.now(),
            summary,
            'manifest-list': manifestListUrl,
            'schema-id': params.currentSchemaId,
          },
        },
        {
          action: 'set-snapshot-ref',
          'snapshot-id': snapshotId,
          type: 'branch',
          'ref-name': 'main',
        },
      ];
      if (removeSnapshotId && removeSnapshotId > 0n) {
        updates.push({
          action: 'remove-snapshots',
          'snapshot-ids': [removeSnapshotId],
        });
      }
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
          updates,
        },
      });
      return {
        result,
        retriesNeeded: try_count,
        parentSnapshotId,
        snapshotId,
        sequenceNumber,
      };
    } catch (e) {
      if (
        e instanceof IcebergHttpError &&
        e.status === 409 &&
        try_count < retry_max
      ) {
        // retry case
        removeSnapshotId = 0n;
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
    conflict_snap = conflict_metadata.snapshots.find(
      (s) => s['snapshot-id'] === conflict_snapshot_id
    );
    if (!conflict_snap) {
      throw new Error('conflict');
    }
    if (
      conflict_snap.summary.operation === 'append' &&
      BigInt(conflict_snap['sequence-number']) === sequenceNumber
    ) {
      expected_snapshot_id = conflict_snapshot_id;
      sequenceNumber++;
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
      requirements: [],
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
