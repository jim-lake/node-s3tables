import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import * as avsc from 'avsc';
import {
  translateRecord,
  translateRecordBySchema,
} from '../src/schema_translator';
import { AvroRegistry } from '../src/avro_types';

void describe('Schema Translator', () => {
  void it('should translate simple record with matching field-ids using raw schemas', () => {
    const sourceSchema = {
      type: 'record',
      name: 'test',
      fields: [
        { name: 'old_name', type: 'int', 'field-id': 1 },
        { name: 'value', type: 'string', 'field-id': 2 },
      ],
    };

    const targetSchema = {
      type: 'record',
      name: 'test',
      fields: [
        { name: 'new_name', type: 'int', 'field-id': 1 },
        { name: 'value', type: 'string', 'field-id': 2 },
      ],
    };

    const source = { old_name: 42, value: 'hello' };
    const result = translateRecordBySchema(sourceSchema, targetSchema, source);

    assert.deepStrictEqual(result, { new_name: 42, value: 'hello' });
  });

  void it('should translate Trino manifest fields to Iceberg fields using raw schemas', () => {
    const trinoSchema = {
      type: 'record',
      name: 'manifest_file',
      fields: [
        { name: 'manifest_path', type: 'string', 'field-id': 500 },
        { name: 'added_data_files_count', type: 'int', 'field-id': 504 },
        { name: 'existing_data_files_count', type: 'int', 'field-id': 505 },
        { name: 'deleted_data_files_count', type: 'int', 'field-id': 506 },
      ],
    };

    const icebergSchema = {
      type: 'record',
      name: 'manifest_file',
      fields: [
        { name: 'manifest_path', type: 'string', 'field-id': 500 },
        { name: 'added_files_count', type: 'int', 'field-id': 504 },
        { name: 'existing_files_count', type: 'int', 'field-id': 505 },
        { name: 'deleted_files_count', type: 'int', 'field-id': 506 },
      ],
    };

    const trinoRecord = {
      manifest_path: 's3://bucket/manifest.avro',
      added_data_files_count: 10,
      existing_data_files_count: 5,
      deleted_data_files_count: 2,
    };

    const result = translateRecordBySchema(
      trinoSchema,
      icebergSchema,
      trinoRecord
    );

    assert.deepStrictEqual(result, {
      manifest_path: 's3://bucket/manifest.avro',
      added_files_count: 10,
      existing_files_count: 5,
      deleted_files_count: 2,
    });
  });

  void it('should translate simple record with matching field-ids', () => {
    const sourceSchema = avsc.Type.forSchema(
      {
        type: 'record',
        name: 'test',
        fields: [
          { name: 'old_name', type: 'int', 'field-id': 1 },
          { name: 'value', type: 'string', 'field-id': 2 },
        ],
      } as avsc.Schema,
      { registry: { ...AvroRegistry } }
    );

    const targetSchema = avsc.Type.forSchema(
      {
        type: 'record',
        name: 'test',
        fields: [
          { name: 'new_name', type: 'int', 'field-id': 1 },
          { name: 'value', type: 'string', 'field-id': 2 },
        ],
      } as avsc.Schema,
      { registry: { ...AvroRegistry } }
    );

    const source = { old_name: 42, value: 'hello' };
    const result = translateRecord(sourceSchema, targetSchema, source);

    assert.deepStrictEqual(result, { new_name: 42, value: 'hello' });
  });

  void it('should translate Trino manifest fields to Iceberg fields', () => {
    const trinoSchema = avsc.Type.forSchema(
      {
        type: 'record',
        name: 'manifest_file',
        fields: [
          { name: 'manifest_path', type: 'string', 'field-id': 500 },
          { name: 'added_data_files_count', type: 'int', 'field-id': 504 },
          { name: 'existing_data_files_count', type: 'int', 'field-id': 505 },
          { name: 'deleted_data_files_count', type: 'int', 'field-id': 506 },
        ],
      } as avsc.Schema,
      { registry: { ...AvroRegistry } }
    );

    const icebergSchema = avsc.Type.forSchema(
      {
        type: 'record',
        name: 'manifest_file',
        fields: [
          { name: 'manifest_path', type: 'string', 'field-id': 500 },
          { name: 'added_files_count', type: 'int', 'field-id': 504 },
          { name: 'existing_files_count', type: 'int', 'field-id': 505 },
          { name: 'deleted_files_count', type: 'int', 'field-id': 506 },
        ],
      } as avsc.Schema,
      { registry: { ...AvroRegistry } }
    );

    const trinoRecord = {
      manifest_path: 's3://bucket/manifest.avro',
      added_data_files_count: 10,
      existing_data_files_count: 5,
      deleted_data_files_count: 2,
    };

    const result = translateRecord(trinoSchema, icebergSchema, trinoRecord);

    assert.deepStrictEqual(result, {
      manifest_path: 's3://bucket/manifest.avro',
      added_files_count: 10,
      existing_files_count: 5,
      deleted_files_count: 2,
    });
  });

  void it('should handle missing fields with defaults', () => {
    const sourceSchema = avsc.Type.forSchema(
      {
        type: 'record',
        name: 'test',
        fields: [{ name: 'id', type: 'int', 'field-id': 1 }],
      } as avsc.Schema,
      { registry: { ...AvroRegistry } }
    );

    const targetSchema = avsc.Type.forSchema(
      {
        type: 'record',
        name: 'test',
        fields: [
          { name: 'id', type: 'int', 'field-id': 1 },
          { name: 'optional', type: 'int', 'field-id': 2, default: 99 },
        ],
      } as avsc.Schema,
      { registry: { ...AvroRegistry } }
    );

    const source = { id: 42 };
    const result = translateRecord(sourceSchema, targetSchema, source);

    assert.deepStrictEqual(result, { id: 42, optional: 99 });
  });

  void it('should handle nested records', () => {
    const sourceSchema = avsc.Type.forSchema(
      {
        type: 'record',
        name: 'outer',
        fields: [
          {
            name: 'inner',
            type: {
              type: 'record',
              name: 'inner_rec',
              fields: [{ name: 'old_field', type: 'int', 'field-id': 10 }],
            },
            'field-id': 1,
          },
        ],
      } as avsc.Schema,
      { registry: { ...AvroRegistry } }
    );

    const targetSchema = avsc.Type.forSchema(
      {
        type: 'record',
        name: 'outer',
        fields: [
          {
            name: 'inner',
            type: {
              type: 'record',
              name: 'inner_rec',
              fields: [{ name: 'new_field', type: 'int', 'field-id': 10 }],
            },
            'field-id': 1,
          },
        ],
      } as avsc.Schema,
      { registry: { ...AvroRegistry } }
    );

    const source = { inner: { old_field: 123 } };
    const result = translateRecord(sourceSchema, targetSchema, source);

    assert.deepStrictEqual(result, { inner: { new_field: 123 } });
  });

  void it('should handle arrays', () => {
    const sourceSchema = avsc.Type.forSchema(
      {
        type: 'record',
        name: 'test',
        fields: [
          {
            name: 'items',
            type: {
              type: 'array',
              items: {
                type: 'record',
                name: 'item',
                fields: [{ name: 'old_name', type: 'int', 'field-id': 1 }],
              },
            },
            'field-id': 1,
          },
        ],
      } as avsc.Schema,
      { registry: { ...AvroRegistry } }
    );

    const targetSchema = avsc.Type.forSchema(
      {
        type: 'record',
        name: 'test',
        fields: [
          {
            name: 'items',
            type: {
              type: 'array',
              items: {
                type: 'record',
                name: 'item',
                fields: [{ name: 'new_name', type: 'int', 'field-id': 1 }],
              },
            },
            'field-id': 1,
          },
        ],
      } as avsc.Schema,
      { registry: { ...AvroRegistry } }
    );

    const source = { items: [{ old_name: 1 }, { old_name: 2 }] };
    const result = translateRecord(sourceSchema, targetSchema, source);

    assert.deepStrictEqual(result, {
      items: [{ new_name: 1 }, { new_name: 2 }],
    });
  });

  void it('should handle unions with null', () => {
    const sourceSchema = avsc.Type.forSchema(
      {
        type: 'record',
        name: 'test',
        fields: [
          { name: 'optional_field', type: ['null', 'int'], 'field-id': 1 },
        ],
      } as avsc.Schema,
      { registry: { ...AvroRegistry } }
    );

    const targetSchema = avsc.Type.forSchema(
      {
        type: 'record',
        name: 'test',
        fields: [
          { name: 'optional_field', type: ['null', 'int'], 'field-id': 1 },
        ],
      } as avsc.Schema,
      { registry: { ...AvroRegistry } }
    );

    const sourceNull = { optional_field: null };
    const resultNull = translateRecord(sourceSchema, targetSchema, sourceNull);
    assert.deepStrictEqual(resultNull, { optional_field: null });

    const sourceValue = { optional_field: 42 };
    const resultValue = translateRecord(
      sourceSchema,
      targetSchema,
      sourceValue
    );
    assert.deepStrictEqual(resultValue, { optional_field: 42 });
  });

  void it('should handle bigint values', () => {
    const sourceSchema = avsc.Type.forSchema(
      {
        type: 'record',
        name: 'test',
        fields: [{ name: 'big_value', type: 'long', 'field-id': 1 }],
      } as avsc.Schema,
      { registry: { ...AvroRegistry } }
    );

    const targetSchema = avsc.Type.forSchema(
      {
        type: 'record',
        name: 'test',
        fields: [{ name: 'big_value', type: 'long', 'field-id': 1 }],
      } as avsc.Schema,
      { registry: { ...AvroRegistry } }
    );

    const source = { big_value: 9007199254740991n };
    const result = translateRecord(sourceSchema, targetSchema, source);

    assert.deepStrictEqual(result, { big_value: 9007199254740991n });
  });

  void it('should fall back to name matching when field-id is missing', () => {
    const sourceSchema = avsc.Type.forSchema(
      {
        type: 'record',
        name: 'test',
        fields: [
          { name: 'id', type: 'int' },
          { name: 'value', type: 'string' },
        ],
      } as avsc.Schema,
      { registry: { ...AvroRegistry } }
    );

    const targetSchema = avsc.Type.forSchema(
      {
        type: 'record',
        name: 'test',
        fields: [
          { name: 'id', type: 'int' },
          { name: 'value', type: 'string' },
        ],
      } as avsc.Schema,
      { registry: { ...AvroRegistry } }
    );

    const source = { id: 42, value: 'test' };
    const result = translateRecord(sourceSchema, targetSchema, source);

    assert.deepStrictEqual(result, { id: 42, value: 'test' });
  });

  void it('should handle complex nested unions in partitions field', () => {
    const sourceSchema = avsc.Type.forSchema(
      {
        type: 'record',
        name: 'manifest_file',
        fields: [
          {
            name: 'partitions',
            type: [
              'null',
              {
                type: 'array',
                items: {
                  type: 'record',
                  name: 'r508',
                  fields: [
                    { name: 'contains_null', type: 'boolean', 'field-id': 509 },
                    {
                      name: 'lower_bound',
                      type: ['null', 'bytes'],
                      'field-id': 510,
                    },
                  ],
                },
              },
            ],
            'field-id': 507,
          },
        ],
      } as avsc.Schema,
      { registry: { ...AvroRegistry } }
    );

    const targetSchema = avsc.Type.forSchema(
      {
        type: 'record',
        name: 'manifest_file',
        fields: [
          {
            name: 'partitions',
            type: [
              'null',
              {
                type: 'array',
                items: {
                  type: 'record',
                  name: 'r508',
                  fields: [
                    { name: 'contains_null', type: 'boolean', 'field-id': 509 },
                    {
                      name: 'lower_bound',
                      type: ['null', 'bytes'],
                      'field-id': 510,
                    },
                  ],
                },
              },
            ],
            'field-id': 507,
          },
        ],
      } as avsc.Schema,
      { registry: { ...AvroRegistry } }
    );

    const source = {
      partitions: [
        { contains_null: true, lower_bound: Buffer.from('test') },
        { contains_null: false, lower_bound: null },
      ],
    };
    const result = translateRecord(sourceSchema, targetSchema, source);

    assert.deepStrictEqual(result, source);
  });
});
