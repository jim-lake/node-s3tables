import { randomBytes, randomUUID } from 'node:crypto';
import { avroToBuffer } from './avro_helper';
import { makeManifestType, ManifestListType } from './avro_schema';
import { ManifestFileStatus, DataFileContent, ListContent } from './avro_types';
import { makeBounds } from './avro_transform';
import { getMetadata } from './metadata';
import { icebergRequest } from './request';
import { parseS3Url, writeS3File, updateManifestList } from './s3_tools';

import type { AwsCredentialIdentity } from '@aws-sdk/types';
import type { PartitionRecord } from './avro_types';

export default { addDataFiles };

export interface AddDataFilesParams {
  credentials?: AwsCredentialIdentity;
  tableBucketARN: string;
  namespace: string;
  name: string;
  file: string;
  specId: number;
  schemaId: number;
  partitions: PartitionRecord;
  fileSize: bigint;
  recordCount: bigint;
}
export async function addDataFiles(params: AddDataFilesParams) {
  const { credentials } = params;
  const region = params.tableBucketARN.split(':')[3];
  if (!region) {
    throw new Error('bad tableBucketARN');
  }
  const snapshot_id = _randomBigInt64();
  const metadata = await getMetadata(params);
  const parent_snapshot_id = metadata['current-snapshot-id'];
  const bucket = metadata.location.split('/').slice(-1)[0];
  const snapshot =
    parent_snapshot_id !== -1
      ? metadata.snapshots.find((s) => s['snapshot-id'] === parent_snapshot_id)
      : null;
  const schema = metadata.schemas.find(
    (s) => s['schema-id'] === params.schemaId
  );
  const spec = metadata['partition-specs'].find(
    (p) => p['spec-id'] === params.specId
  );
  if (!bucket) {
    throw new Error('bad manifest location');
  }
  if (parent_snapshot_id !== -1 && !snapshot) {
    throw new Error('no old snapshot');
  }
  if (!schema) {
    throw new Error('schema not found');
  }
  if (!spec) {
    throw new Error('partition spec not found');
  }
  const sequence_number =
    BigInt(
      metadata.snapshots.reduce(
        (memo, s) =>
          s['sequence-number'] > memo ? s['sequence-number'] : memo,
        0
      )
    ) + 1n;

  const manifest_type = makeManifestType(spec, schema);
  const manifest_buf = await avroToBuffer({
    type: manifest_type,
    metadata: {
      'partition-spec-id': String(params.specId),
      'partition-spec': JSON.stringify(spec.fields),
    },
    records: [
      {
        status: ManifestFileStatus.ADDED,
        snapshot_id,
        sequence_number,
        file_sequence_number: sequence_number,
        data_file: {
          content: DataFileContent.DATA,
          file_path: params.file,
          file_format: 'PARQUET',
          record_count: params.recordCount,
          file_size_in_bytes: params.fileSize,
          partition: params.partitions,
        },
      },
    ],
  });
  const manifest_key = `metadata/${randomUUID()}.avro`;
  await writeS3File({
    credentials,
    region,
    bucket,
    key: manifest_key,
    body: manifest_buf,
  });

  const manifest_list_key = `metadata/${randomUUID()}.avro`;
  const manifest_list_url = `s3://${bucket}/${manifest_list_key}`;

  const bounds = makeBounds(params.partitions, spec, schema);
  const partitions = bounds.map((bound) => ({
    contains_null: false,
    contains_nan: false,
    upper_bound: bound,
    lower_bound: bound,
  }));

  const manifest_record = {
    manifest_path: `s3://${bucket}/${manifest_key}`,
    manifest_length: BigInt(manifest_buf.length),
    partition_spec_id: params.specId,
    content: ListContent.DATA,
    sequence_number,
    min_sequence_number: sequence_number,
    added_snapshot_id: snapshot_id,
    added_data_files_count: 1,
    existing_data_files_count: 0,
    deleted_data_files_count: 0,
    added_rows_count: params.recordCount,
    existing_rows_count: 0n,
    deleted_rows_count: 0n,
    partitions,
  };

  if (snapshot) {
    // Update existing manifest list
    const { key: old_list_key } = parseS3Url(snapshot['manifest-list']);
    if (!old_list_key) {
      throw new Error('snapshot invalid');
    }

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
      prepend: [manifest_record],
    });
  } else {
    // Create new manifest list directly for first snapshot
    const manifest_list_buf = await avroToBuffer({
      type: ManifestListType,
      metadata: {
        'sequence-number': String(sequence_number),
        'snapshot-id': String(snapshot_id),
        'parent-snapshot-id': String(parent_snapshot_id),
      },
      records: [manifest_record],
    });

    await writeS3File({
      credentials,
      region,
      bucket,
      key: manifest_list_key,
      body: manifest_list_buf,
    });
  }
  const commit_result = await icebergRequest({
    credentials: params.credentials,
    tableBucketARN: params.tableBucketARN,
    method: 'POST',
    suffix: `/namespaces/${params.namespace}/tables/${params.name}`,
    body: {
      requirements:
        parent_snapshot_id === -1
          ? [
              {
                type: 'assert-table-uuid',
                uuid: (metadata as any)['table-uuid'],
              },
            ]
          : [
              {
                type: 'assert-ref-snapshot-id',
                ref: 'main',
                'snapshot-id': parent_snapshot_id,
              },
            ],
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
              'added-data-files': '1',
              'added-records': String(params.recordCount),
              'added-files-size': String(params.fileSize),
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
  return commit_result;
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
