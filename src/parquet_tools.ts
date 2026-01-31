import { PassThrough } from 'node:stream';
import { ParquetReader, ParquetWriter, ParquetSchema } from 'parquetjs';
import { encodeValue } from './avro_transform';

import type { IcebergSchema, IcebergType, IcebergSchemaField } from './iceberg';
import type { AddFile } from './manifest';

interface ParquetSchemaField {
  type: string;
  compression?: string;
  optional?: boolean;
}

const JULIAN_UNIX_EPOCH_DIFF = 2440588;
const NANOS_PER_DAY = 86400000000000n;
const MICROS_PER_MILLI = 1000n;

export function icebergToParquetSchema(
  schema: IcebergSchema
): Record<string, ParquetSchemaField> {
  const result: Record<string, ParquetSchemaField> = {};
  for (const field of schema.fields) {
    const pqType = icebergTypeToParquet(field.type);
    if (pqType) {
      result[field.name] = {
        type: pqType,
        compression: 'ZSTD',
        optional: !field.required,
      };
    }
  }
  return result;
}

function icebergTypeToParquet(type: IcebergType): string | null {
  if (typeof type === 'string') {
    switch (type) {
      case 'boolean':
        return 'BOOLEAN';
      case 'int':
        return 'INT32';
      case 'long':
        return 'INT64';
      case 'float':
        return 'FLOAT';
      case 'double':
        return 'DOUBLE';
      case 'date':
        return 'DATE';
      case 'timestamp':
      case 'timestamptz':
        return 'TIMESTAMP_MICROS';
      case 'string':
        return 'UTF8';
      case 'binary':
        return 'BYTE_ARRAY';
      case 'time':
        return 'TIME_MICROS';
      case 'uuid':
        return 'UTF8';
      default:
        if (type.startsWith('decimal(')) {
          return 'BYTE_ARRAY';
        }
        return null;
    }
  }
  return null;
}

function int96ToMicros(buf: Buffer): bigint {
  const nanoOfDay = buf.readBigInt64LE(0);
  const julianDay = buf.readInt32LE(8);
  const daysSinceEpoch = julianDay - JULIAN_UNIX_EPOCH_DIFF;
  const totalNanos = BigInt(daysSinceEpoch) * NANOS_PER_DAY + nanoOfDay;
  return totalNanos / 1000n;
}

function convertValue(value: unknown, icebergType: IcebergType): unknown {
  if (value === null || value === undefined) {
    return value;
  }
  if (Buffer.isBuffer(value) && value.length === 12) {
    if (
      typeof icebergType === 'string' &&
      (icebergType === 'timestamp' || icebergType === 'timestamptz')
    ) {
      return int96ToMicros(value);
    }
  }
  if (value instanceof Date) {
    if (
      typeof icebergType === 'string' &&
      (icebergType === 'timestamp' || icebergType === 'timestamptz')
    ) {
      return BigInt(value.getTime()) * MICROS_PER_MILLI;
    }
  }
  return value;
}

export interface RewriteParquetResult {
  buffer: Buffer;
  stats: Omit<AddFile, 'file' | 'partitions'>;
}

export async function rewriteParquet(
  inputBuffer: Buffer,
  schema: IcebergSchema,
  partitions?: Record<string, string>
): Promise<RewriteParquetResult> {
  const reader = await ParquetReader.openBuffer(inputBuffer, {
    treatInt96AsTimestamp: true,
  });
  const cursor = reader.getCursor();

  const pqSchemaFields = icebergToParquetSchema(schema);
  const pqSchema = new ParquetSchema(pqSchemaFields);

  const stream = new PassThrough();
  const chunks: Buffer[] = [];
  stream.on('data', (chunk: Buffer) => chunks.push(chunk));

  const writer = await ParquetWriter.openStream(pqSchema, stream);

  let row: Record<string, unknown> | null;
  while ((row = await cursor.next())) {
    const converted: Record<string, unknown> = {};
    for (const field of schema.fields) {
      const val = row[field.name] ?? partitions?.[field.name];
      converted[field.name] = convertValue(val, field.type);
    }
    await writer.appendRow(converted);
  }

  const envelopeWriter = writer.envelopeWriter;
  await writer.close();
  await reader.close();

  const buffer = Buffer.concat(chunks);
  const stats = extractWriterStats(envelopeWriter, schema);

  return { buffer, stats };
}

interface ParquetWriterColumnMetadata {
  meta_data?: {
    path_in_schema?: string[];
    total_compressed_size?: number;
    num_values?: number;
    statistics?: {
      null_count?: number;
      min_value?: Buffer;
      max_value?: Buffer;
    };
  };
}

interface ParquetWriterRowGroup {
  columns: ParquetWriterColumnMetadata[];
}

interface ParquetEnvelopeWriter {
  rowGroups: ParquetWriterRowGroup[];
}

function extractWriterStats(
  envelopeWriter: ParquetEnvelopeWriter,
  schema: IcebergSchema
): Omit<AddFile, 'file' | 'partitions'> {
  const columnSizes: Record<string, bigint> = {};
  const valueCounts: Record<string, bigint> = {};
  const nullValueCounts: Record<string, bigint> = {};
  const lowerBounds: Record<string, Buffer> = {};
  const upperBounds: Record<string, Buffer> = {};
  let recordCount = 0n;

  for (const rg of envelopeWriter.rowGroups) {
    for (const column of rg.columns) {
      const fieldName = column.meta_data?.path_in_schema?.[0];
      if (fieldName && column.meta_data) {
        if (column.meta_data.total_compressed_size !== undefined) {
          columnSizes[fieldName] =
            (columnSizes[fieldName] ?? 0n) +
            BigInt(column.meta_data.total_compressed_size);
        }
        if (column.meta_data.num_values !== undefined) {
          const count = BigInt(column.meta_data.num_values);
          valueCounts[fieldName] = (valueCounts[fieldName] ?? 0n) + count;
          if (recordCount < count) {
            recordCount = count;
          }
        }
        if (column.meta_data.statistics) {
          if (column.meta_data.statistics.null_count !== undefined) {
            nullValueCounts[fieldName] =
              (nullValueCounts[fieldName] ?? 0n) +
              BigInt(column.meta_data.statistics.null_count);
          }
          const field = schema.fields.find(
            (f: IcebergSchemaField) => f.name === fieldName
          );
          const fieldType =
            field && typeof field.type === 'string' ? field.type : null;
          const minVal = column.meta_data.statistics.min_value ?? null;
          const maxVal = column.meta_data.statistics.max_value ?? null;
          const minBuf = Buffer.isBuffer(minVal)
            ? minVal
            : encodeValue(minVal, 'identity', fieldType);
          const maxBuf = Buffer.isBuffer(maxVal)
            ? maxVal
            : encodeValue(maxVal, 'identity', fieldType);
          if (
            minBuf &&
            (!lowerBounds[fieldName] ||
              Buffer.compare(minBuf, lowerBounds[fieldName]) < 0)
          ) {
            lowerBounds[fieldName] = minBuf;
          }
          if (
            maxBuf &&
            (!upperBounds[fieldName] ||
              Buffer.compare(maxBuf, upperBounds[fieldName]) > 0)
          ) {
            upperBounds[fieldName] = maxBuf;
          }
        }
      }
    }
  }

  return {
    fileSize: 0n,
    recordCount,
    columnSizes: Object.keys(columnSizes).length > 0 ? columnSizes : null,
    valueCounts: Object.keys(valueCounts).length > 0 ? valueCounts : null,
    nullValueCounts:
      Object.keys(nullValueCounts).length > 0 ? nullValueCounts : null,
    lowerBounds: Object.keys(lowerBounds).length > 0 ? lowerBounds : null,
    upperBounds: Object.keys(upperBounds).length > 0 ? upperBounds : null,
  };
}
