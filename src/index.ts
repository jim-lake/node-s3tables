import { getMetadata, addSchema, addPartitionSpec } from './metadata';
import { addDataFiles, setCurrentCommit } from './snapshot';
export * from './metadata';
export * from './snapshot';

export type * from './metadata';
export type * from './snapshot';
export type * from './iceberg';

export default {
  getMetadata,
  addSchema,
  addPartitionSpec,
  addDataFiles,
  setCurrentCommit,
};
