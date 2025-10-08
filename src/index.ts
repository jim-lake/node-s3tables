import { addManifest } from './manifest';
import { getMetadata, addSchema, addPartitionSpec } from './metadata';
import { addDataFiles, setCurrentCommit } from './snapshot';
import { IcebergHttpError } from './request';

export { IcebergHttpError } from './request';

export * from './manifest';
export * from './metadata';
export * from './snapshot';

export type * from './manifest';
export type * from './metadata';
export type * from './snapshot';
export type * from './iceberg';

export default {
  IcebergHttpError,
  getMetadata,
  addSchema,
  addPartitionSpec,
  addManifest,
  addDataFiles,
  setCurrentCommit,
};
