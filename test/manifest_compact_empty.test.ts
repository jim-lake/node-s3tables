import { test } from './helpers/test_helper';
import { strict as assert } from 'node:assert';
import { config } from './helpers/aws_clients';
import { setupTable } from './helpers/table_lifecycle';

import { manifestCompact } from '../src/manifest_compact';

void test('manifest compact empty table', async (t) => {
  const { namespace, name } = await setupTable(
    t,
    'test_ns_compact_empty',
    'test_table_compact_empty',
    [
      { name: 'id', type: 'long', required: true },
      { name: 'value', type: 'string', required: false },
    ]
  );

  await t.test('compact empty table returns unchanged', async () => {
    const result = await manifestCompact({
      tableBucketARN: config.tableBucketARN,
      namespace,
      name,
    });
    assert.strictEqual(result.changed, false);
    assert.strictEqual(result.outputManifestCount, 0);
    assert.strictEqual(result.snapshotId, 0n);
  });
});
