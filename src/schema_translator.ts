interface RawAvroField {
  name: string;
  type: unknown;
  'field-id'?: number;
  default?: unknown;
}

interface RawAvroRecordSchema {
  type: 'record';
  name: string;
  fields: RawAvroField[];
}

function isRawRecordSchema(schema: unknown): schema is RawAvroRecordSchema {
  return (
    typeof schema === 'object' &&
    schema !== null &&
    'type' in schema &&
    schema.type === 'record' &&
    'fields' in schema
  );
}

function isRawArraySchema(
  schema: unknown
): schema is { type: 'array'; items: unknown } {
  return (
    typeof schema === 'object' &&
    schema !== null &&
    'type' in schema &&
    schema.type === 'array'
  );
}

function isRawMapSchema(
  schema: unknown
): schema is { type: 'map'; values: unknown } {
  return (
    typeof schema === 'object' &&
    schema !== null &&
    'type' in schema &&
    schema.type === 'map'
  );
}

function isRawUnionSchema(schema: unknown): schema is unknown[] {
  return Array.isArray(schema);
}

export function translateRecord(
  sourceSchema: unknown,
  targetSchema: unknown,
  record: unknown
): unknown {
  return translateValue(sourceSchema, targetSchema, record);
}

function translateValue(
  sourceSchema: unknown,
  targetSchema: unknown,
  value: unknown
): unknown {
  if (value === null || value === undefined) {
    return value;
  }

  // Handle unions
  if (isRawUnionSchema(targetSchema)) {
    for (const targetBranch of targetSchema) {
      if (isRawUnionSchema(sourceSchema)) {
        for (const sourceBranch of sourceSchema) {
          try {
            return translateValue(sourceBranch, targetBranch, value);
          } catch {
            // Try next branch
          }
        }
      } else {
        try {
          return translateValue(sourceSchema, targetBranch, value);
        } catch {
          // Try next branch
        }
      }
    }
    return value;
  }

  if (isRawUnionSchema(sourceSchema)) {
    for (const sourceBranch of sourceSchema) {
      try {
        return translateValue(sourceBranch, targetSchema, value);
      } catch {
        // Try next branch
      }
    }
  }

  // Handle primitives
  if (typeof sourceSchema === 'string' && typeof targetSchema === 'string') {
    return value;
  }

  // Handle records
  if (isRawRecordSchema(sourceSchema) && isRawRecordSchema(targetSchema)) {
    return translateRecordValue(sourceSchema, targetSchema, value);
  }

  // Handle arrays
  if (isRawArraySchema(sourceSchema) && isRawArraySchema(targetSchema)) {
    if (!Array.isArray(value)) {
      return value;
    }
    return value.map((item) =>
      translateValue(sourceSchema.items, targetSchema.items, item)
    );
  }

  // Handle maps
  if (isRawMapSchema(sourceSchema) && isRawMapSchema(targetSchema)) {
    if (typeof value !== 'object') {
      return value;
    }
    const result: Record<string, unknown> = {};
    for (const [key, val] of Object.entries(value)) {
      result[key] = translateValue(
        sourceSchema.values,
        targetSchema.values,
        val
      );
    }
    return result;
  }

  return value;
}

function translateRecordValue(
  sourceSchema: RawAvroRecordSchema,
  targetSchema: RawAvroRecordSchema,
  record: unknown
): unknown {
  if (typeof record !== 'object' || record === null) {
    return record;
  }

  const sourceRecord = record as Record<string, unknown>;
  const result: Record<string, unknown> = {};

  // Build field maps from source
  const sourceFieldById = new Map<number, RawAvroField>();
  const sourceFieldByName = new Map<string, RawAvroField>();

  for (const field of sourceSchema.fields) {
    if (field['field-id'] !== undefined) {
      sourceFieldById.set(field['field-id'], field);
    }
    sourceFieldByName.set(field.name, field);
  }

  // Translate each target field
  for (const targetField of targetSchema.fields) {
    let sourceField: RawAvroField | undefined;
    let sourceValue: unknown;

    // Match by field-id first
    if (targetField['field-id'] !== undefined) {
      sourceField = sourceFieldById.get(targetField['field-id']);
      if (sourceField) {
        sourceValue = sourceRecord[sourceField.name];
      }
    }

    // Fall back to name match
    if (sourceField === undefined) {
      sourceField = sourceFieldByName.get(targetField.name);
      if (sourceField) {
        sourceValue = sourceRecord[sourceField.name];
      }
    }

    // Handle missing source field or value
    if (sourceField === undefined) {
      if ('default' in targetField) {
        result[targetField.name] = targetField.default;
      }
    } else if (sourceValue === undefined) {
      if ('default' in targetField) {
        result[targetField.name] = targetField.default;
      }
    } else {
      // Translate the value
      result[targetField.name] = translateValue(
        sourceField.type,
        targetField.type,
        sourceValue
      );
    }
  }

  return result;
}
