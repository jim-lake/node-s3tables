import { test } from './helpers/test_helper';
import { log } from './helpers/log_helper';
import { strict as assert } from 'node:assert';
import { config } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';
import { queryRows, executeQuery } from './helpers/athena_helper';
import { createSimpleParquetFile } from './helpers/parquet_helper';

import { getMetadata, addDataFiles } from '../src';

/*
 * createSimpleParquetFile writes 10 physical rows per file. These tests examine
 * what the AddFile.recordCount value does: it is a required field (a
 * non-optional bigint on the manifest entry), but the library does not validate
 * it against the parquet file. The two Athena query paths treat it differently,
 * which is exactly why a wrong value is dangerous:
 *
 *   - SELECT *  physically scans the parquet data, so it returns the real rows
 *               regardless of the claimed record_count.
 *   - count(*)  is answered from the Iceberg manifest metadata, so it believes
 *               the claimed record_count.
 *
 * A wrong record_count therefore leaves the table internally inconsistent:
 * count(*) and SELECT * disagree. 0 and 1 are the "something is wrong" values --
 * neither is rejected, both silently corrupt the metadata count.
 */
const FILE_ROWS = 10;

/*
 * Generate a normal 10-row parquet file, append it as one snapshot claiming the
 * given recordCount, and return both Athena counts afterward.
 */
async function appendWithRecordCount(
  namespace: string,
  name: string,
  fileIndex: number,
  recordCount: bigint
): Promise<{ selectRows: number; metadataCount: number }> {
  const metadata = await getMetadata({
    tableBucketARN: config.tableBucketARN,
    namespace,
    name,
  });
  const tableBucket = metadata.location.split('/').slice(-1)[0];
  assert(tableBucket, 'Could not extract table bucket');

  const { key, size } = await createSimpleParquetFile(tableBucket, fileIndex);

  await addDataFiles({
    tableBucketARN: config.tableBucketARN,
    namespace,
    name,
    lists: [
      {
        specId: 0,
        schemaId: 0,
        files: [
          {
            file: `s3://${tableBucket}/${key}`,
            partitions: { app: 'test-app' },
            recordCount,
            fileSize: BigInt(size),
          },
        ],
      },
    ],
  });

  const selectRows = (await queryRows(namespace, name)).length;
  const metadataCount = await countStar(namespace, name);
  return { selectRows, metadataCount };
}

/* Run `SELECT count(*)` via Athena and return the scalar. */
async function countStar(namespace: string, name: string): Promise<number> {
  const result = await executeQuery(
    namespace,
    `SELECT count(*) AS c FROM ${name}`
  );
  const value = result.ResultSet?.Rows?.[1]?.Data?.[0]?.VarCharValue;
  assert(value !== undefined, 'count(*) returned no value');
  return Number(value);
}

void test('record_count behavior test', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_record_count',
    'test_table_record_count',
    [
      { name: 'app', type: 'string', required: true },
      { name: 'event_datetime', type: 'timestamp', required: false },
    ]
  );

  await t.test('correct record_count keeps both counts in sync', async () => {
    const { selectRows, metadataCount } = await appendWithRecordCount(
      namespace,
      name,
      1,
      BigInt(FILE_ROWS)
    );
    log('correct recordCount: SELECT *', selectRows, 'count(*)', metadataCount);
    assert.strictEqual(
      selectRows,
      FILE_ROWS,
      `SELECT * should be ${FILE_ROWS}`
    );
    assert.strictEqual(
      metadataCount,
      FILE_ROWS,
      `count(*) should be ${FILE_ROWS}`
    );
  });

  await t.test('record_count=0 desyncs count(*) from SELECT *', async () => {
    /* Wrong on purpose: the file really has 10 rows. */
    const { selectRows, metadataCount } = await appendWithRecordCount(
      namespace,
      name,
      2,
      0n
    );
    log('recordCount=0: SELECT *', selectRows, 'count(*)', metadataCount);

    /* SELECT * scans the real rows: 10 existing + 10 physical = 20. */
    assert.strictEqual(
      selectRows,
      FILE_ROWS * 2,
      `SELECT * should scan all physical rows; expected ${FILE_ROWS * 2}, got ${selectRows}`
    );
    /* count(*) trusts the manifest: 10 + 0 claimed = 10, so it is now wrong. */
    assert.strictEqual(
      metadataCount,
      FILE_ROWS,
      `count(*) should trust the claimed 0 rows; expected ${FILE_ROWS}, got ${metadataCount}`
    );
    assert.notStrictEqual(
      metadataCount,
      selectRows,
      'recordCount=0 should leave count(*) and SELECT * inconsistent'
    );
  });

  await t.test('record_count=1 desyncs count(*) from SELECT *', async () => {
    /* Wrong on purpose: the file really has 10 rows. */
    const { selectRows, metadataCount } = await appendWithRecordCount(
      namespace,
      name,
      3,
      1n
    );
    log('recordCount=1: SELECT *', selectRows, 'count(*)', metadataCount);

    /* SELECT * scans the real rows: 20 existing + 10 physical = 30. */
    assert.strictEqual(
      selectRows,
      FILE_ROWS * 3,
      `SELECT * should scan all physical rows; expected ${FILE_ROWS * 3}, got ${selectRows}`
    );
    /* count(*) trusts the manifest: previous 10 + 1 claimed = 11, still wrong. */
    assert.strictEqual(
      metadataCount,
      FILE_ROWS + 1,
      `count(*) should trust the claimed 1 row; expected ${FILE_ROWS + 1}, got ${metadataCount}`
    );
    assert.notStrictEqual(
      metadataCount,
      selectRows,
      'recordCount=1 should leave count(*) and SELECT * inconsistent'
    );
  });
});
