import { AwsCredentialIdentity } from '@aws-sdk/types';
import { S3TablesClientConfig } from '@aws-sdk/client-s3tables';

type IcebergTransform = 'identity' | 'year' | 'month' | 'day' | 'hour' | `bucket[${number}]` | `truncate[${number}]`;
interface IcebergPartitionField {
    'field-id': number;
    name: string;
    'source-id': number;
    transform: IcebergTransform;
}
type IcebergPrimitiveType = 'boolean' | 'int' | 'long' | 'float' | 'double' | 'date' | 'time' | 'timestamp' | 'timestamptz' | 'string' | 'uuid' | 'binary' | `decimal(${number},${number})` | `fixed[${number}]`;
type IcebergComplexType = {
    type: 'list';
    element: IcebergType;
    'element-required': boolean;
} | {
    type: 'map';
    key: IcebergType;
    value: IcebergType;
    'value-required': boolean;
} | {
    type: 'struct';
    fields: IcebergSchemaField[];
};
type IcebergType = IcebergPrimitiveType | IcebergComplexType;
interface IcebergSchemaField {
    id: number;
    name: string;
    type: IcebergType;
    required: boolean;
    doc?: string;
}
interface IcebergSchema {
    type: 'struct';
    'schema-id': number;
    fields: IcebergSchemaField[];
}
interface IcebergPartitionSpec {
    'spec-id': number;
    fields: IcebergPartitionField[];
}
interface IcebergMetadata {
    'last-column-id': number;
    'current-schema-id': number;
    schemas: IcebergSchema[];
    'default-spec-id': number;
    'partition-specs': IcebergPartitionSpec[];
    'last-partition-id': number;
    'current-snapshot-id': number;
}

type TableLocation = {
    tableArn: string;
} | {
    tableBucketARN: string;
    namespace: string;
    name: string;
};
type GetMetadataParams = TableLocation & {
    config?: S3TablesClientConfig;
};
declare function getMetadata(params: GetMetadataParams): Promise<IcebergMetadata>;
interface AddSchemaParams {
    credentials?: AwsCredentialIdentity;
    tableBucketARN: string;
    namespace: string;
    name: string;
    schemaId: number;
    fields: IcebergSchemaField[];
}
declare function addSchema(params: AddSchemaParams): Promise<string>;
interface AddPartitionSpecParams {
    credentials?: AwsCredentialIdentity;
    tableBucketARN: string;
    namespace: string;
    name: string;
    specId: number;
    fields: IcebergPartitionField[];
}
declare function addPartitionSpec(params: AddPartitionSpecParams): Promise<string>;

declare const _default: {
    getMetadata: typeof getMetadata;
    addSchema: typeof addSchema;
    addPartitionSpec: typeof addPartitionSpec;
};

export { addPartitionSpec, addSchema, _default as default, getMetadata };
export type { AddPartitionSpecParams, AddSchemaParams, GetMetadataParams, IcebergComplexType, IcebergMetadata, IcebergPartitionField, IcebergPartitionSpec, IcebergPrimitiveType, IcebergSchema, IcebergSchemaField, IcebergTransform, IcebergType, TableLocation };
