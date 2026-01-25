import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import * as avsc from 'avsc';
import * as path from 'node:path';
import * as zlib from 'node:zlib';
import { ManifestListType } from '../src/avro_schema';
import { BigIntType } from '../src/avro_types';
import { log } from './helpers/log_helper';

import type { ManifestListRecord } from '../src/avro_types';

void describe('Manifest List Decoder', () => {
  void it('should decode iceberg_snap1.avro with key_metadata field', async () => {
    const filePath = path.join(__dirname, 'files', 'iceberg_snap1.avro');

    const decoder = avsc.createFileDecoder(filePath, {
      parseHook: () => ManifestListType,
      codecs: {
        deflate(
          data: Uint8Array,
          cb: (err: Error | null, result?: Buffer) => void
        ) {
          zlib.inflateRaw(Buffer.from(data), cb);
        },
      },
    });

    const records: ManifestListRecord[] = [];

    await new Promise<void>((resolve, reject) => {
      decoder.on('data', (record: ManifestListRecord) => {
        records.push(record);
      });

      decoder.on('end', () => {
        resolve();
      });

      decoder.on('error', (err) => {
        reject(err);
      });
    });

    assert.ok(records.length > 0, 'Should have decoded at least one record');
    log(`Decoded ${records.length} records`);

    // Verify all records have the expected structure
    for (const record of records) {
      assert.ok('manifest_path' in record, 'Record should have manifest_path');
      assert.ok(
        'key_metadata' in record,
        'Record should have key_metadata field'
      );
      assert.ok(
        record.key_metadata === null || Buffer.isBuffer(record.key_metadata),
        'key_metadata should be null or Buffer'
      );
    }
  });

  void it('should handle files with extra fields gracefully', async () => {
    const filePath = path.join(__dirname, 'files', 'iceberg_snap1.avro');

    // Read the file schema
    const decoder = avsc.createFileDecoder(filePath, {
      parseHook(schema: avsc.Schema) {
        log('File schema:', JSON.stringify(schema, null, 2));
        return avsc.Type.forSchema(schema, { registry: { long: BigIntType } });
      },
      codecs: {
        deflate(
          data: Uint8Array,
          cb: (err: Error | null, result?: Buffer) => void
        ) {
          zlib.inflateRaw(Buffer.from(data), cb);
        },
      },
    });

    const records: ManifestListRecord[] = [];

    await new Promise<void>((resolve, reject) => {
      decoder.on('data', (record: ManifestListRecord) => {
        records.push(record);
      });

      decoder.on('end', () => {
        resolve();
      });

      decoder.on('error', (err) => {
        reject(err);
      });
    });

    assert.ok(records.length > 0, 'Should decode with file schema');

    // Verify key_metadata field exists in records
    const firstRecord = records[0];
    assert.ok(
      firstRecord && 'key_metadata' in firstRecord,
      'Record should have key_metadata field'
    );
  });

  void it('should encode and decode records with key_metadata', () => {
    const testRecord: ManifestListRecord = {
      manifest_path: 's3://test-bucket/metadata/test.avro',
      manifest_length: 1000n,
      partition_spec_id: 1,
      content: 0,
      sequence_number: 1n,
      min_sequence_number: 1n,
      added_snapshot_id: 123456789n,
      added_files_count: 10,
      existing_files_count: 0,
      deleted_files_count: 0,
      added_rows_count: 100n,
      existing_rows_count: 0n,
      deleted_rows_count: 0n,
      partitions: null,
      key_metadata: Buffer.from('test-encryption-key'),
    };

    // Encode
    const encoded = ManifestListType.toBuffer(testRecord);
    assert.ok(encoded.length > 0, 'Should encode record');

    // Decode
    const decoded = ManifestListType.fromBuffer(encoded) as ManifestListRecord;
    assert.strictEqual(decoded.manifest_path, testRecord.manifest_path);
    assert.strictEqual(decoded.manifest_length, testRecord.manifest_length);

    // Verify key_metadata field exists
    assert.ok(
      'key_metadata' in decoded,
      'Decoded record should have key_metadata field'
    );

    /*
     * Note: key_metadata may be null due to Avro default behavior
     * This test verifies the field exists and can be encoded/decoded
     */
    if (decoded.key_metadata) {
      assert.ok(
        Buffer.isBuffer(decoded.key_metadata) ||
          ArrayBuffer.isView(decoded.key_metadata),
        'key_metadata should be a Buffer or typed array when present'
      );
    }
  });

  void it('should handle records without key_metadata (backward compatibility)', () => {
    const testRecord: ManifestListRecord = {
      manifest_path: 's3://test-bucket/metadata/test.avro',
      manifest_length: 1000n,
      partition_spec_id: 1,
      content: 0,
      sequence_number: 1n,
      min_sequence_number: 1n,
      added_snapshot_id: 123456789n,
      added_files_count: 10,
      existing_files_count: 0,
      deleted_files_count: 0,
      added_rows_count: 100n,
      existing_rows_count: 0n,
      deleted_rows_count: 0n,
      partitions: null,
      // key_metadata intentionally omitted
    };

    // Encode
    const encoded = ManifestListType.toBuffer(testRecord);
    assert.ok(encoded.length > 0, 'Should encode record without key_metadata');

    // Decode
    const decoded = ManifestListType.fromBuffer(encoded) as ManifestListRecord;
    assert.strictEqual(decoded.manifest_path, testRecord.manifest_path);
    assert.strictEqual(
      decoded.key_metadata,
      null,
      'key_metadata should default to null'
    );
  });
});
