import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import { translateRecord } from '../src/schema_translator';

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
    const result = translateRecord(sourceSchema, targetSchema, source);

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

    const result = translateRecord(trinoSchema, icebergSchema, trinoRecord);

    assert.deepStrictEqual(result, {
      manifest_path: 's3://bucket/manifest.avro',
      added_files_count: 10,
      existing_files_count: 5,
      deleted_files_count: 2,
    });
  });
});
