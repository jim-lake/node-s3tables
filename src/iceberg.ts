export type IcebergTransform =
  | 'identity'
  | 'year'
  | 'month'
  | 'day'
  | 'hour'
  | `bucket[${number}]`
  | `truncate[${number}]`;
export interface IcebergPartitionField {
  'field-id': number;
  name: string;
  'source-id': number;
  transform: IcebergTransform;
}
export type IcebergPrimitiveType =
  | 'boolean'
  | 'int'
  | 'long'
  | 'float'
  | 'double'
  | 'date'
  | 'time'
  | 'timestamp'
  | 'timestamptz'
  | 'string'
  | 'uuid'
  | 'binary'
  | `decimal(${number},${number})`
  | `fixed[${number}]`;

export type IcebergComplexType =
  | { type: 'list'; element: IcebergType; 'element-required': boolean }
  | {
      type: 'map';
      key: IcebergType;
      value: IcebergType;
      'value-required': boolean;
    }
  | { type: 'struct'; fields: IcebergSchemaField[] };

export type IcebergType = IcebergPrimitiveType | IcebergComplexType;

export interface IcebergSchemaField {
  id: number;
  name: string;
  type: IcebergType;
  required: boolean;
  doc?: string;
}
export interface IcebergSchema {
  type: 'struct';
  'schema-id': number;
  fields: IcebergSchemaField[];
}
export interface IcebergPartitionSpec {
  'spec-id': number;
  fields: IcebergPartitionField[];
}
export interface IcebergSnapshot {
  'snapshot-id': bigint | number;
  'parent-snapshot-id'?: bigint | number;
  'sequence-number': number;
  'timestamp-ms': number;
  'manifest-list': string;
  summary: Record<string, string>;
  'schema-id'?: number;
}
export interface IcebergMetadata {
  'last-column-id': number;
  'current-schema-id': number;
  schemas: IcebergSchema[];
  snapshots: IcebergSnapshot[];
  'default-spec-id': number;
  'partition-specs': IcebergPartitionSpec[];
  'last-partition-id': number;
  'current-snapshot-id': bigint | number;
  location: string;
}
