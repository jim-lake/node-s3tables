import * as avsc from 'avsc';

export enum ManifestFileStatus {
  EXISTING = 0,
  ADDED = 1,
  DELETED = 2,
}
export enum DataFileContent {
  DATA = 0,
  POSITION_DELETES = 1,
  EQUALITY_DELETES = 2,
}
export type RawValue = string | number | bigint | Buffer | null;
export type PartitionRecord = Record<string, RawValue>;
export interface ManifestFileRecord {
  status: ManifestFileStatus;
  snapshot_id: bigint;
  sequence_number?: bigint | null;
  file_sequence_number?: bigint | null;
  data_file: {
    content: DataFileContent;
    file_path: string;
    file_format: 'PARQUET' | 'AVRO' | 'ORC';
    partition: PartitionRecord | null;
    record_count: bigint;
    file_size_in_bytes: bigint;
    column_sizes?: { key: number; value: bigint }[] | null;
    value_counts?: { key: number; value: bigint }[] | null;
    null_value_counts?: { key: number; value: bigint }[] | null;
    nan_value_counts?: { key: number; value: bigint }[] | null;
    lower_bounds?: { key: number; value: Buffer }[] | null;
    upper_bounds?: { key: number; value: Buffer }[] | null;
    key_metadata?: Buffer | null;
    split_offsets?: bigint[] | null;
    equality_ids?: number[] | null;
    sort_order_id?: number | null;
  };
}
export interface PartitionSummary {
  contains_null: boolean;
  contains_nan?: boolean | null;
  lower_bound?: Buffer | null;
  upper_bound?: Buffer | null;
}
export enum ListContent {
  DATA = 0,
  DELETES = 1,
}
export interface ManifestListRecord {
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
export type AvroPrimitiveType = 'string' | 'int' | 'long' | 'double' | 'bytes';

export const BigIntType = avsc.types.LongType.__with({
  fromBuffer(uint_array: Buffer | Uint8Array) {
    return Buffer.from(uint_array).readBigInt64LE();
  },
  toBuffer(n: bigint) {
    const buf = Buffer.alloc(8);
    buf.writeBigInt64LE(n);
    return buf;
  },
  fromJSON: BigInt,
  toJSON: Number,
  isValid: (n: unknown) => typeof n === 'bigint',
  compare(n1: bigint, n2: bigint) {
    return n1 === n2 ? 0 : n1 < n2 ? -1 : 1;
  },
});
export class YearStringType extends avsc.types.LogicalType {
  override _fromValue(val: number) {
    return (1970 + val).toString();
  }

  override _toValue(str: string) {
    return parseInt(str, 10) - 1970;
  }

  override _resolve(type: unknown) {
    if (avsc.Type.isType(type, 'int')) {
      return (val: number) => this._fromValue(val);
    }
    return null;
  }
}
export class MonthStringType extends avsc.types.LogicalType {
  override _fromValue(val: number) {
    const year = 1970 + Math.floor(val / 12);
    const month = (val % 12) + 1;
    return `${year}-${String(month).padStart(2, '0')}`;
  }

  override _toValue(str: string) {
    const [y, m] = str.split('-').map(Number);
    return ((y ?? 1970) - 1970) * 12 + ((m ?? 1) - 1);
  }

  override _resolve(type: unknown) {
    if (avsc.Type.isType(type, 'int')) {
      return (val: number) => this._fromValue(val);
    }
    return null;
  }
}
export class DateStringType extends avsc.types.LogicalType {
  override _fromValue(val: number) {
    const ms = val * 86400000;
    return new Date(ms).toISOString().slice(0, 10);
  }

  override _toValue(str: string) {
    const [year, month, day] = str.split('-').map(Number);
    return Math.floor(
      Date.UTC(year ?? 1970, (month ?? 1) - 1, day ?? 1) / 86400000
    );
  }

  override _resolve(type: unknown) {
    if (avsc.Type.isType(type, 'int')) {
      return (val: number) => this._fromValue(val);
    }
    return null;
  }
}
export class HourStringType extends avsc.types.LogicalType {
  override _fromValue(val: number) {
    const ms = val * 3600000;
    return new Date(ms).toISOString().slice(0, 13);
  }

  override _toValue(str: string) {
    const d = new Date(str);
    return Math.floor(d.getTime() / 3600000);
  }

  override _resolve(type: unknown) {
    if (avsc.Type.isType(type, 'long')) {
      return (val: number) => this._fromValue(val);
    }
    return null;
  }
}
export const AvroRegistry = { long: BigIntType };
export const AvroLogicalTypes = {
  year: YearStringType,
  month: MonthStringType,
  date: DateStringType,
  hour: HourStringType,
};
export type AvroLogicalType = keyof typeof AvroLogicalTypes;
export type AvroType =
  | AvroPrimitiveType
  | { type: AvroPrimitiveType; logicalType: AvroLogicalType };
