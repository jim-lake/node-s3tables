import { addDataFiles } from './add_data_files';
import { importRedshiftManifest } from './import_redshift';
import { addManifest } from './manifest';
import {
  getMetadata,
  addSchema,
  addPartitionSpec,
  removeSnapshots,
} from './metadata';
import { IcebergHttpError } from './request';
import { setCurrentCommit } from './snapshot';

export { IcebergHttpError, resolveS3TablesHost } from './request';
export {
  parseS3Url,
  downloadAvro,
  getS3Client,
  getS3TablesClient,
} from './s3_tools';
export { resolveAwsConfig } from './config';
export type { AwsConfig, AwsConfigParams } from './config';
export type { S3ClientConfig, S3TablesClientConfig } from './config';
export { ManifestListSchema } from './avro_schema';

export * from './add_data_files';
export * from './import_redshift';
export * from './manifest';
export * from './manifest_compact';
export * from './metadata';
export * from './snapshot';

export type * from './add_data_files';
export type * from './iceberg';
export type * from './import_redshift';
export type * from './manifest';
export type * from './manifest_compact';
export type * from './metadata';
export type * from './snapshot';

export type { ManifestListRecord } from './avro_types';

export default {
  IcebergHttpError,
  addSchema,
  addPartitionSpec,
  addManifest,
  addDataFiles,
  getMetadata,
  importRedshiftManifest,
  removeSnapshots,
  setCurrentCommit,
};
