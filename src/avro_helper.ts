import * as avsc from 'avsc';
import * as zlib from 'node:zlib';

import type {
  AvroType,
  ManifestListRecord,
  ManifestFileRecord,
} from './avro_types';
import type {
  IcebergPartitionSpec,
  IcebergPartitionField,
  IcebergSchema,
} from './iceberg';

export function fixupMetadata(
  metadata: Record<string, string | Buffer>
): Record<string, Buffer> {
  const newMetadata: Record<string, Buffer> = {};
  for (const [key, value] of Object.entries(metadata)) {
    if (Buffer.isBuffer(value)) {
      newMetadata[key] = value;
    } else {
      newMetadata[key] = Buffer.from(value, 'utf8');
    }
  }
  return newMetadata;
}
export interface AvroToBufferParams {
  type: avsc.Type;
  metadata?: Record<string, string | Buffer>;
  records: ManifestListRecord[] | ManifestFileRecord[];
}
export async function avroToBuffer(
  params: AvroToBufferParams
): Promise<Buffer> {
  const metadata = params.metadata
    ? fixupMetadata(params.metadata)
    : params.metadata;
  return new Promise((resolve, reject) => {
    try {
      const buffers: Buffer[] = [];
      const opts = {
        writeHeader: true,
        codecs: { deflate: zlib.deflateRaw },
        codec: 'deflate',
        metadata,
      } as unknown as ConstructorParameters<
        typeof avsc.streams.BlockEncoder
      >[1];
      const encoder = new avsc.streams.BlockEncoder(params.type, opts);
      encoder.on('data', (chunk: Buffer) => {
        buffers.push(chunk);
      });
      encoder.on('end', () => {
        resolve(Buffer.concat(buffers));
      });
      encoder.on('error', reject);

      params.records.forEach((record) => {
        encoder.write(record);
      });
      encoder.end();
    } catch (err) {
      if (err instanceof Error) {
        reject(err);
      } else {
        reject(new Error(String(err)));
      }
    }
  });
}
export function icebergToAvroFields(
  spec: IcebergPartitionSpec,
  schema: IcebergSchema
) {
  return spec.fields.map((p) => _icebergToAvroField(p, schema));
}
function _icebergToAvroField(
  field: IcebergPartitionField,
  schema: IcebergSchema
) {
  const source = schema.fields.find((f) => f.id === field['source-id']);
  if (!source) {
    throw new Error(`Source field ${field['source-id']} not found in schema`);
  }
  let avroType: AvroType;
  switch (field.transform) {
    case 'identity':
      if (typeof source.type === 'string') {
        avroType = _mapPrimitiveToAvro(source.type);
        break;
      }
      throw new Error(
        `Unsupported transform: ${field.transform} for complex type`
      );
    case 'year':
      avroType = { type: 'int' as const, logicalType: 'year' as const };
      break;
    case 'month':
      avroType = { type: 'int' as const, logicalType: 'month' as const };
      break;
    case 'day':
      avroType = { type: 'int' as const, logicalType: 'date' as const };
      break;
    case 'hour':
      avroType = { type: 'long' as const, logicalType: 'hour' as const };
      break;
    default:
      if (field.transform.startsWith('bucket[')) {
        avroType = 'int' as const;
        break;
      } else if (field.transform.startsWith('truncate[')) {
        avroType = 'string' as const;
        break;
      }
      throw new Error(`Unsupported transform: ${field.transform} for type`);
  }
  return {
    name: field.name,
    type: ['null', avroType],
    default: null,
    'field-id': field['field-id'],
  };
}
function _mapPrimitiveToAvro(type: string): AvroType {
  switch (type) {
    case 'boolean':
      return 'int';
    case 'int':
      return 'int';
    case 'long':
    case 'time':
    case 'timestamp':
    case 'timestamptz':
      return 'long';
    case 'float':
    case 'double':
      return 'double';
    case 'date':
      return { type: 'int' as const, logicalType: 'date' as const };
    case 'string':
    case 'uuid':
      return 'string';
    case 'binary':
      return 'bytes';
    default:
      throw new Error(`Unsupported primitive: ${type}`);
  }
}
