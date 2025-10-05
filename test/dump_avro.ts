#!/usr/bin/env node

import * as avsc from 'avsc';

const filename = process.argv[2];
const outputFile = process.argv[3];

if (!filename) {
  console.error('Usage: tsx dump_avro.ts <avro-file> [output-file]');
  process.exit(1);
}

const BigIntType = avsc.types.LongType.__with({
  fromBuffer: (buf: Buffer) => buf.readBigInt64LE(),
  toBuffer(n: bigint) {
    const buf = Buffer.allocUnsafe(8);
    buf.writeBigInt64LE(n);
    return buf;
  },
  fromJSON: BigInt,
  toJSON: Number,
  isValid: (n: unknown): n is bigint => typeof n === 'bigint',
  compare: (a: bigint, b: bigint) => (a < b ? -1 : a > b ? 1 : 0),
});

const decoder = avsc.createFileDecoder(filename, {
  parseHook: (schema: avsc.Schema) =>
    avsc.Type.forSchema(schema, { registry: { long: BigIntType } }),
});

decoder.on(
  'metadata',
  (_type, _codec, header: { meta: Record<string, unknown> }) => {
    const schemaBuffer = header.meta['avro.schema'];
    const fullSchema = Buffer.isBuffer(schemaBuffer)
      ? schemaBuffer.toString()
      : String(schemaBuffer);
    const schema = JSON.parse(fullSchema) as unknown;

    console.log('Schema:');
    console.log(JSON.stringify(schema, null, 2));

    console.log('\nMeta:');
    for (const [subKey, subValue] of Object.entries(header.meta)) {
      if (Buffer.isBuffer(subValue) || subValue instanceof Uint8Array) {
        console.log(`${subKey}: ${Buffer.from(subValue).toString()}`);
      } else {
        console.log(`${subKey}: ${String(subValue)}`);
      }
    }
    decoder.on('data', (record) => {
      console.log('record:', record);
    });

    if (outputFile) {
      const newType = avsc.Type.forSchema(schema as avsc.Schema, {
        registry: { long: BigIntType },
      });
      const encoder = avsc.createFileEncoder(outputFile, newType);
      decoder.on('data', (record) => {
        encoder.write(record);
      });
      decoder.on('end', () => {
        encoder.end();
      });
    }
  }
);

decoder.on('error', (err) => {
  console.error('Error:', err.message);
  process.exit(1);
});
