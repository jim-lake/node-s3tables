import { S3ClientConfig, S3Client } from '@aws-sdk/client-s3';
export { S3ClientConfig } from '@aws-sdk/client-s3';
import { S3TablesClientConfig, S3TablesClient } from '@aws-sdk/client-s3tables';
export { S3TablesClientConfig } from '@aws-sdk/client-s3tables';
import { AwsCredentialIdentity } from '@aws-sdk/types';
import * as avsc from '@jim-lake/avsc';
import { Schema } from '@jim-lake/avsc';

/**
 * User-facing AWS configuration accepted by every public API in this library.
 *
 * `region` and `credentials` are convenient top-level shortcuts applied to both
 * service configs. `s3ClientConfig` and `s3TablesClientConfig` are spread
 * directly into the corresponding AWS SDK client constructors, so any
 * constructor option is supported (for example `useDualstackEndpoint: true` for
 * IPv6 / public-dual-stack environments, or a custom `endpoint`).
 */
interface AwsConfigParams {
    region?: string | undefined;
    credentials?: AwsCredentialIdentity | undefined;
    s3ClientConfig?: S3ClientConfig | undefined;
    s3TablesClientConfig?: S3TablesClientConfig | undefined;
}
/**
 * Normalized configuration carried internally. Both service config objects
 * always exist (possibly empty) so downstream code can spread them into SDK
 * constructors without null checks. This single object is passed by reference
 * through every internal function; it is never destructured and reassembled.
 */
interface AwsConfig {
    region?: string | undefined;
    s3ClientConfig: S3ClientConfig;
    s3TablesClientConfig: S3TablesClientConfig;
}
/**
 * Resolve the user-facing shortcuts into a single {@link AwsConfig}. Explicit
 * values inside `s3ClientConfig` / `s3TablesClientConfig` always win over the
 * `region` / `credentials` shortcuts. Call this once at a public entry point;
 * pass the result around unchanged.
 */
declare function resolveAwsConfig(params: AwsConfigParams): AwsConfig;

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
    added_files_count: number;
    existing_files_count: number;
    deleted_files_count: number;
    added_rows_count: bigint;
    existing_rows_count: bigint;
    deleted_rows_count: bigint;
    partitions?: PartitionSummary[] | null;
    key_metadata?: Buffer | null;
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
    config: AwsConfig;
    metadata: IcebergMetadata;
    schemaId: number;
    specId: number;
    snapshotId: bigint;
    sequenceNumber: bigint;
    files: AddFile[];
}
declare function addManifest(params: AddManifestParams): Promise<ManifestListRecord>;
declare function minBuffer(a: Buffer | null | undefined, b: Buffer | null | undefined, field: IcebergPartitionField, schema: IcebergSchema): Buffer | null;
declare function maxBuffer(a: Buffer | null | undefined, b: Buffer | null | undefined, field: IcebergPartitionField, schema: IcebergSchema): Buffer | null;

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
interface AddDataFilesParams extends AwsConfigParams {
    tableBucketARN: string;
    namespace: string;
    name: string;
    snapshotId?: bigint;
    lists: AddFileList[];
    retryCount?: number | undefined;
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

interface ImportRedshiftManifestParams extends AwsConfigParams {
    tableBucketARN: string;
    namespace: string;
    name: string;
    redshiftManifestUrl: string;
    schemaId?: number;
    specId?: number;
    retryCount?: number | undefined;
    rewriteParquet?: boolean;
}
interface ImportRedshiftManifestResult extends AddDataFilesResult {
    files: {
        url: string;
        records: bigint;
        fileSize: bigint;
    }[];
}
declare function importRedshiftManifest(params: ImportRedshiftManifestParams): Promise<ImportRedshiftManifestResult>;

type TableLocation = {
    tableArn: string;
} | {
    tableBucketARN: string;
    namespace: string;
    name: string;
};
type GetMetadataParams = TableLocation & AwsConfigParams;
declare function getMetadata(params: GetMetadataParams): Promise<IcebergMetadata>;
/**
 * Internal variant of {@link getMetadata} that takes an already-resolved
 * {@link AwsConfig}. Used by other operations so the config resolved at the
 * public entry point flows through unchanged.
 */
declare function getMetadataWithConfig(location: TableLocation, config: AwsConfig): Promise<IcebergMetadata>;
interface AddSchemaParams extends AwsConfigParams {
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
interface AddPartitionSpecParams extends AwsConfigParams {
    tableBucketARN: string;
    namespace: string;
    name: string;
    specId: number;
    fields: IcebergPartitionField[];
}
declare function addPartitionSpec(params: AddPartitionSpecParams): Promise<IcebergUpdateResponse>;
interface RemoveSnapshotsParams extends AwsConfigParams {
    tableBucketARN: string;
    namespace: string;
    name: string;
    snapshotIds: bigint[];
}
declare function removeSnapshots(params: RemoveSnapshotsParams): Promise<IcebergUpdateResponse>;

declare class IcebergHttpError extends Error {
    status: number;
    text?: string;
    body?: JSONObject;
    constructor(status: number, body: JSONValue, message: string);
}
/**
 * Resolve the hostname for the S3 Tables Iceberg REST endpoint.
 *
 * Always uses the dual-stack endpoint `s3tables.{region}.api.aws`, which
 * resolves to IPv4 or IPv6 depending on the network, so it works everywhere
 * the legacy `amazonaws.com` endpoint did while also supporting IPv6-only /
 * public-dual-stack environments. An explicit `endpoint` in
 * `s3TablesClientConfig` overrides it.
 */
declare function resolveS3TablesHost(region: string, s3TablesClientConfig?: S3TablesClientConfig): string;

interface SubmitSnapshotParams {
    config: AwsConfig;
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
    resolveConflict?: (conflictSnapshot: IcebergSnapshot) => Promise<ResolveConflictResult>;
}
interface ResolveConflictResult {
    manifestListUrl: string;
    summary: Record<string, string>;
}
interface SubmitSnapshotResult {
    result: JSONObject;
    retriesNeeded: number;
    parentSnapshotId: bigint;
    snapshotId: bigint;
    sequenceNumber: bigint;
}
declare function submitSnapshot(params: SubmitSnapshotParams): Promise<SubmitSnapshotResult>;
interface SetCurrentCommitParams extends AwsConfigParams {
    tableBucketARN: string;
    namespace: string;
    name: string;
    snapshotId: bigint;
}
declare function setCurrentCommit(params: SetCurrentCommitParams): Promise<JSONObject>;

declare function parseS3Url(url: string): {
    bucket: string;
    key: string;
};
declare function getS3Client(config: AwsConfig): S3Client;
declare function getS3TablesClient(config: AwsConfig): S3TablesClient;
interface DownloadAvroParams {
    config: AwsConfig;
    bucket: string;
    key: string;
    avroSchema: Schema;
}
declare function downloadAvro<T>(params: DownloadAvroParams): Promise<T[]>;

declare const ManifestListSchema: avsc.Schema;

type CalculateWeightFunction = (group: ManifestListRecord[]) => number;
interface ManifestCompactParams extends AwsConfigParams {
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
interface ManifestCompactResult extends SubmitSnapshotResult {
    changed: boolean;
    inputManifestCount: number;
    outputManifestCount: number;
}
declare function manifestCompact(params: ManifestCompactParams): Promise<ManifestCompactResult>;

declare const _default: {
    IcebergHttpError: typeof IcebergHttpError;
    addSchema: typeof addSchema;
    addPartitionSpec: typeof addPartitionSpec;
    addManifest: typeof addManifest;
    addDataFiles: typeof addDataFiles;
    getMetadata: typeof getMetadata;
    importRedshiftManifest: typeof importRedshiftManifest;
    removeSnapshots: typeof removeSnapshots;
    setCurrentCommit: typeof setCurrentCommit;
};

export { IcebergHttpError, ManifestListSchema, addDataFiles, addManifest, addPartitionSpec, addSchema, _default as default, downloadAvro, getMetadata, getMetadataWithConfig, getS3Client, getS3TablesClient, importRedshiftManifest, manifestCompact, maxBuffer, minBuffer, parseS3Url, removeSnapshots, resolveAwsConfig, resolveS3TablesHost, setCurrentCommit, submitSnapshot };
export type { AddDataFilesParams, AddDataFilesResult, AddFile, AddFileList, AddManifestParams, AddPartitionSpecParams, AddSchemaParams, AwsConfig, AwsConfigParams, CalculateWeightFunction, GetMetadataParams, IcebergComplexType, IcebergMetadata, IcebergPartitionField, IcebergPartitionSpec, IcebergPrimitiveType, IcebergSchema, IcebergSchemaField, IcebergSnapshot, IcebergSnapshotSummary, IcebergTransform, IcebergType, IcebergUpdateResponse, ImportRedshiftManifestParams, ImportRedshiftManifestResult, ManifestCompactParams, ManifestCompactResult, ManifestListRecord, RemoveSnapshotsParams, ResolveConflictResult, SetCurrentCommitParams, SubmitSnapshotParams, SubmitSnapshotResult, TableLocation };
