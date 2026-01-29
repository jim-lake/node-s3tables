import type { PartitionRecord, RawValue } from './avro_types';
import type {
  IcebergPartitionSpec,
  IcebergSchema,
  IcebergTransform,
  IcebergType,
  IcebergPartitionField,
  IcebergPrimitiveType,
} from './iceberg';

function _isPrimitive(t: IcebergType): t is IcebergPrimitiveType {
  return typeof t === 'string';
}
function _outputType(
  transform: IcebergTransform,
  sourceType: IcebergType
): IcebergPrimitiveType | null {
  if (transform === 'identity' || transform.startsWith('truncate[')) {
    if (_isPrimitive(sourceType)) {
      return sourceType;
    }
    return null;
  }
  if (transform.startsWith('bucket[')) {
    return 'int';
  }
  if (
    transform === 'year' ||
    transform === 'month' ||
    transform === 'day' ||
    transform === 'hour'
  ) {
    return 'int';
  }
  return null;
}
function _encodeValue(
  raw: RawValue,
  transform: IcebergTransform | null,
  out_type: IcebergPrimitiveType | null
): Buffer | null {
  if (raw === null || transform === null || out_type === null) {
    return null;
  }
  switch (transform) {
    case 'identity': {
      if (Buffer.isBuffer(raw)) {
        if (
          out_type === 'binary' ||
          out_type.startsWith('decimal(') ||
          out_type.startsWith('fixed[')
        ) {
          return raw;
        }
        throw new Error(
          `Buffer not allowed for identity with type ${out_type}`
        );
      }
      switch (out_type) {
        case 'int': {
          const n = typeof raw === 'number' ? raw : Number(raw);
          const buf = Buffer.alloc(4);
          buf.writeInt32LE(Math.floor(n));
          return buf;
        }
        case 'long': {
          const n = typeof raw === 'bigint' ? raw : BigInt(raw);
          const buf = Buffer.alloc(8);
          buf.writeBigInt64LE(n);
          return buf;
        }
        case 'float': {
          const n = typeof raw === 'number' ? raw : Number(raw);
          const buf = Buffer.alloc(4);
          buf.writeFloatLE(n);
          return buf;
        }
        case 'double': {
          const n = typeof raw === 'number' ? raw : Number(raw);
          const buf = Buffer.alloc(8);
          buf.writeDoubleLE(n);
          return buf;
        }
        case 'string':
        case 'uuid': {
          const s = typeof raw === 'string' ? raw : String(raw);
          return Buffer.from(s, 'utf8');
        }
        case 'boolean': {
          const buf = Buffer.alloc(1);
          buf.writeUInt8(raw ? 1 : 0);
          return buf;
        }
        case 'binary':
        case 'date':
        case 'time':
        case 'timestamp':
        case 'timestamptz':
          throw new Error(`Identity not implemented for type ${out_type}`);
        default:
          throw new Error(`Identity not implemented for type ${out_type}`);
      }
    }
    case 'year':
    case 'month':
    case 'day':
    case 'hour': {
      let n: number;
      if (typeof raw === 'string') {
        const d = new Date(raw);
        if (transform === 'year') {
          n = d.getUTCFullYear();
        } else if (transform === 'month') {
          n = d.getUTCFullYear() * 12 + d.getUTCMonth();
        } else if (transform === 'day') {
          n = Math.floor(d.getTime() / (24 * 3600 * 1000));
        } else {
          n = Math.floor(d.getTime() / (3600 * 1000));
        }
      } else if (typeof raw === 'number' || typeof raw === 'bigint') {
        n = Number(raw);
      } else {
        throw new Error(`${transform} requires string|number|bigint`);
      }
      const buf = Buffer.alloc(4);
      buf.writeInt32LE(n);
      return buf;
    }
    default:
      if (transform.startsWith('bucket[')) {
        if (typeof raw !== 'number') {
          throw new Error('bucket requires number input');
        }
        const buf = Buffer.alloc(4);
        buf.writeInt32LE(raw);
        return buf;
      }
      if (transform.startsWith('truncate[')) {
        if (typeof raw !== 'string') {
          throw new Error('truncate requires string input');
        }
        const width = Number(/\d+/.exec(transform)?.[0]);
        return Buffer.from(raw.substring(0, width), 'utf8');
      }
      throw new Error(`Unsupported transform ${transform}`);
  }
}
type NaNOnly = number & { __nan: true };
const NaNValue: NaNOnly = NaN as NaNOnly;

export function makeBounds(
  partitions: PartitionRecord,
  spec: IcebergPartitionSpec,
  schema: IcebergSchema
): (Buffer | null | NaNOnly)[] {
  return spec.fields.map((f) => {
    const schemaField = schema.fields.find((sf) => sf.id === f['source-id']);
    if (!schemaField) {
      throw new Error(`Schema field not found for source-id ${f['source-id']}`);
    }
    if (!(f.name in partitions)) {
      throw new Error(`partitions missing ${f.name}`);
    }
    const raw = partitions[f.name];
    if (typeof raw === 'number' && isNaN(raw)) {
      return NaNValue;
    }
    if (raw === null || raw === undefined) {
      return null;
    }
    const out_type = _outputType(f.transform, schemaField.type);
    return _encodeValue(raw, f.transform, out_type);
  });
}
export function compareBounds(
  a: Buffer | Uint8Array,
  b: Buffer | Uint8Array,
  field: IcebergPartitionField,
  schema: IcebergSchema
): number {
  const schemaField = schema.fields.find((sf) => sf.id === field['source-id']);
  if (!schemaField) {
    throw new Error(
      `Schema field not found for source-id ${field['source-id']}`
    );
  }
  const out_type = _outputType(field.transform, schemaField.type);
  switch (out_type) {
    case 'boolean':
      return Buffer.from(a).readUInt8() - Buffer.from(b).readUInt8();
    case 'int':
      return Buffer.from(a).readInt32LE() - Buffer.from(b).readInt32LE();
    case 'long': {
      const diff =
        Buffer.from(a).readBigInt64LE() - Buffer.from(b).readBigInt64LE();
      return diff > 0n ? 1 : diff < 0n ? -1 : 0;
    }
    case 'float':
      return Buffer.from(a).readFloatLE() - Buffer.from(b).readFloatLE();
    case 'double':
      return Buffer.from(a).readDoubleLE() - Buffer.from(b).readDoubleLE();
    case null:
    case 'date':
    case 'time':
    case 'timestamp':
    case 'timestamptz':
    case 'string':
    case 'uuid':
    case 'binary':
    default:
      return Buffer.compare(a, b);
  }
}
