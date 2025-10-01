import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { GetTableCommand, S3TablesClient } from '@aws-sdk/client-s3tables';

import { icebergRequest } from './request';

import type { AwsCredentialIdentity } from '@aws-sdk/types';
import type { S3TablesClientConfig } from '@aws-sdk/client-s3tables';
import type {
  IcebergSchemaField,
  IcebergPartitionField,
  IcebergMetadata,
} from './iceberg';
export type * from './iceberg';

export default { getMetadata, addSchema, addPartitionSpec };

export type TableLocation =
  | { tableArn: string }
  | { tableBucketARN: string; namespace: string; name: string };
export type GetMetadataParams = TableLocation & {
  config?: S3TablesClientConfig;
};
export async function getMetadata(
  params: GetMetadataParams
): Promise<IcebergMetadata> {
  const { config, ...other } = params;
  const client = new S3TablesClient(config ?? {});
  const get_table_cmd = new GetTableCommand(other);
  const response = await client.send(get_table_cmd);
  console.log(response);
  if (!response.metadataLocation) {
    throw new Error('missing metadataLocation');
  }
  const s3_client = new S3Client(config as unknown ?? {});
  const { key, bucket } = _parseS3Url(response.metadataLocation);
  const get_file_cmd = new GetObjectCommand({ Bucket: bucket, Key: key });
  const file_response = await s3_client.send(get_file_cmd);
  const body = await file_response.Body?.transformToString();
  if (!body) {
    throw new Error('missing body');
  }
  return JSON.parse(body);
}
export interface AddSchemaParams {
  credentials?: AwsCredentialIdentity;
  tableBucketARN: string;
  namespace: string;
  name: string;
  schemaId: number;
  fields: IcebergSchemaField[];
}
export async function addSchema(params: AddSchemaParams) {
  return icebergRequest({
    tableBucketARN: params.tableBucketARN,
    method: 'POST',
    suffix: `/namespaces/${params.namespace}/tables/${params.name}`,
    body: {
      requirements: [],
      updates: [
        {
          action: 'add-schema',
          schema: {
            'schema-id': params.schemaId,
            type: 'struct',
            fields: params.fields,
          },
        },
        { action: 'set-current-schema', 'schema-id': params.schemaId },
      ],
    },
  });
}
export interface AddPartitionSpecParams {
  credentials?: AwsCredentialIdentity;
  tableBucketARN: string;
  namespace: string;
  name: string;
  specId: number;
  fields: IcebergPartitionField[];
}
export function addPartitionSpec(params: AddPartitionSpecParams) {
  return icebergRequest({
    tableBucketARN: params.tableBucketARN,
    method: 'POST',
    suffix: `/namespaces/${params.namespace}/tables/${params.name}`,
    body: {
      requirements: [],
      updates: [
        {
          action: 'add-spec',
          spec: {
            'spec-id': params.specId,
            type: 'struct',
            fields: params.fields,
          },
        },
        { action: 'set-default-spec', 'spec-id': params.specId },
      ],
    },
  });
}

function _parseS3Url(url: string) {
  const match = url.match(/^s3:\/\/([^\/]+)\/(.+)$/);
  if (!match) {
    throw new Error('Invalid S3 URL');
  }
  return { bucket: match[1], key: match[2] };
}
