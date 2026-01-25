import { addManifest } from './manifest';
import {
  getMetadata,
  addSchema,
  addPartitionSpec,
  removeSnapshots,
} from './metadata';
import { addDataFiles } from './add_data_files';
import { setCurrentCommit } from './snapshot';
import { IcebergHttpError } from './request';

export { IcebergHttpError } from './request';

export * from './add_data_files';
export * from './manifest';
export * from './manifest_compact';
export * from './metadata';
export * from './snapshot';

export type * from './add_data_files';
export type * from './manifest';
export type * from './manifest_compact';
export type * from './metadata';
export type * from './snapshot';
export type * from './iceberg';

export type { ManifestListRecord } from './avro_types';

export default {
  IcebergHttpError,
  getMetadata,
  addSchema,
  addPartitionSpec,
  addManifest,
  addDataFiles,
  setCurrentCommit,
  removeSnapshots,
};
