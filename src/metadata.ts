import { GetObjectCommand } from '@aws-sdk/client-s3';
import { GetTableCommand } from '@aws-sdk/client-s3tables';

import { parse } from './json';
import { icebergRequest } from './request';
import { parseS3Url, getS3Client, getS3TablesClient } from './s3_tools';

import type { AwsCredentialIdentity } from '@aws-sdk/types';
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
  region?: string;
  credentials?: AwsCredentialIdentity;
};
export async function getMetadata(
  params: GetMetadataParams
): Promise<IcebergMetadata> {
  if ('tableBucketARN' in params) {
    interface Response {
      metadata?: IcebergMetadata;
    }
    const icebergResponse = await icebergRequest<Response>({
      credentials: params.credentials,
      tableBucketARN: params.tableBucketARN,
      method: 'GET',
      suffix: `/namespaces/${params.namespace}/tables/${params.name}`,
    });
    if (icebergResponse.metadata) {
      return icebergResponse.metadata;
    }
    throw new Error('invalid table metadata');
  }
  const { ...other } = params;
  const client = getS3TablesClient(params);
  const get_table_cmd = new GetTableCommand(other);
  const response = await client.send(get_table_cmd);
  if (!response.metadataLocation) {
    throw new Error('missing metadataLocation');
  }
  const s3_client = getS3Client(params);
  const { key, bucket } = parseS3Url(response.metadataLocation);
  const get_file_cmd = new GetObjectCommand({ Bucket: bucket, Key: key });
  const file_response = await s3_client.send(get_file_cmd);
  const body = await file_response.Body?.transformToString();
  if (!body) {
    throw new Error('missing body');
  }
  return parse(body) as unknown as IcebergMetadata;
}
export interface AddSchemaParams {
  credentials?: AwsCredentialIdentity;
  tableBucketARN: string;
  namespace: string;
  name: string;
  schemaId: number;
  fields: IcebergSchemaField[];
}
export interface IcebergUpdateResponse {
  metadata: IcebergMetadata;
  'metadata-location': string;
}

export async function addSchema(params: AddSchemaParams): Promise<IcebergUpdateResponse> {
  return icebergRequest<IcebergUpdateResponse>({
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
export async function addPartitionSpec(params: AddPartitionSpecParams): Promise<IcebergUpdateResponse> {
  return icebergRequest<IcebergUpdateResponse>({
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
