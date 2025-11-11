import { AwsCredentialIdentity } from '@aws-sdk/types';

type RawValue = string | number | bigint | Buffer | null;
type PartitionRecord = Record<string, RawValue>;
interface PartitionSummary {
    contains_null: boolean;
    contains_nan?: boolean | null;
    lower_bound?: Buffer | null;
    upper_bound?: Buffer | null;
}
declare enum ListContent {
    DATA = 0,
    DELETES = 1
}
interface ManifestListRecord {
    manifest_path: string;
    manifest_length: bigint;
    partition_spec_id: number;
    content: ListContent;
    sequence_number: bigint;
    min_sequence_number: bigint;
    added_snapshot_id: bigint;
    added_data_files_count: number;
    existing_data_files_count: number;
    deleted_data_files_count: number;
    added_rows_count: bigint;
    existing_rows_count: bigint;
    deleted_rows_count: bigint;
    partitions?: PartitionSummary[] | null;
}

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
interface IcebergSnapshotSummary extends Record<string, string> {
    operation: 'append' | 'replace' | 'overwrite' | 'delete';
}
interface IcebergSnapshot {
    'snapshot-id': bigint | number;
    'parent-snapshot-id'?: bigint | number;
    'sequence-number': number;
    'timestamp-ms': number;
    'manifest-list': string;
    summary: IcebergSnapshotSummary;
    'schema-id'?: number;
}
interface IcebergMetadata {
    'last-column-id': number;
    'last-sequence-number': bigint | number;
    'current-schema-id': number;
    schemas: IcebergSchema[];
    snapshots: IcebergSnapshot[];
    'default-spec-id': number;
    'partition-specs': IcebergPartitionSpec[];
    'last-partition-id': number;
    'current-snapshot-id': bigint | number;
    location: string;
}

interface AddFile {
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
interface AddManifestParams {
    credentials?: AwsCredentialIdentity | undefined;
    region: string;
    metadata: IcebergMetadata;
    schemaId: number;
    specId: number;
    snapshotId: bigint;
    sequenceNumber: bigint;
    files: AddFile[];
}
declare function addManifest(params: AddManifestParams): Promise<ManifestListRecord>;

type TableLocation = {
    tableArn: string;
} | {
    tableBucketARN: string;
    namespace: string;
    name: string;
};
type GetMetadataParams = TableLocation & {
    region?: string;
    credentials?: AwsCredentialIdentity;
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
interface IcebergUpdateResponse {
    metadata: IcebergMetadata;
    'metadata-location': string;
}
declare function addSchema(params: AddSchemaParams): Promise<IcebergUpdateResponse>;
interface AddPartitionSpecParams {
    credentials?: AwsCredentialIdentity;
    tableBucketARN: string;
    namespace: string;
    name: string;
    specId: number;
    fields: IcebergPartitionField[];
}
declare function addPartitionSpec(params: AddPartitionSpecParams): Promise<IcebergUpdateResponse>;
interface RemoveSnapshotsParams {
    credentials?: AwsCredentialIdentity;
    tableBucketARN: string;
    namespace: string;
    name: string;
    snapshotIds: bigint[];
}
declare function removeSnapshots(params: RemoveSnapshotsParams): Promise<IcebergUpdateResponse>;

type JSONPrimitive = string | number | boolean | null | bigint | undefined;
type JSONValue = JSONPrimitive | JSONObject | JSONArray;
interface JSONObject {
    [key: string]: JSONValue;
}
type JSONArray = JSONValue[];

interface AddFileList {
    specId: number;
    schemaId: number;
    files: AddFile[];
}
interface AddDataFilesParams {
    credentials?: AwsCredentialIdentity;
    tableBucketARN: string;
    namespace: string;
    name: string;
    snapshotId?: bigint;
    lists: AddFileList[];
    retryCount?: number;
    maxSnapshots?: number;
}
interface AddDataFilesResult {
    result: JSONObject;
    retriesNeeded: number;
    parentSnapshotId: bigint;
    snapshotId: bigint;
    sequenceNumber: bigint;
}
declare function addDataFiles(params: AddDataFilesParams): Promise<AddDataFilesResult>;
interface SetCurrentCommitParams {
    credentials?: AwsCredentialIdentity;
    tableBucketARN: string;
    namespace: string;
    name: string;
    snapshotId: bigint;
}
declare function setCurrentCommit(params: SetCurrentCommitParams): Promise<JSONObject>;

declare class IcebergHttpError extends Error {
    status: number;
    text?: string;
    body?: JSONObject;
    constructor(status: number, body: JSONValue, message: string);
}

declare const _default: {
    IcebergHttpError: typeof IcebergHttpError;
    getMetadata: typeof getMetadata;
    addSchema: typeof addSchema;
    addPartitionSpec: typeof addPartitionSpec;
    addManifest: typeof addManifest;
    addDataFiles: typeof addDataFiles;
    setCurrentCommit: typeof setCurrentCommit;
    removeSnapshots: typeof removeSnapshots;
};

export { IcebergHttpError, addDataFiles, addManifest, addPartitionSpec, addSchema, _default as default, getMetadata, removeSnapshots, setCurrentCommit };
export type { AddDataFilesParams, AddDataFilesResult, AddFile, AddFileList, AddManifestParams, AddPartitionSpecParams, AddSchemaParams, GetMetadataParams, IcebergComplexType, IcebergMetadata, IcebergPartitionField, IcebergPartitionSpec, IcebergPrimitiveType, IcebergSchema, IcebergSchemaField, IcebergSnapshot, IcebergSnapshotSummary, IcebergTransform, IcebergType, IcebergUpdateResponse, RemoveSnapshotsParams, SetCurrentCommitParams, TableLocation };
