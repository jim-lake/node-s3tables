import { test } from './helpers/test_helper';
import { strict as assert } from 'node:assert';
import { config } from './helpers/aws_clients';

import { getMetadata, addSchema, addPartitionSpec } from '../src';

void test('metadata error scenarios', async (t) => {
  await t.test('getMetadata with missing namespace', async () => {
    await assert.rejects(
      async () => {
        await getMetadata({
          tableBucketARN: config.tableBucketARN,
          namespace: 'nonexistent_namespace',
          name: 'nonexistent_table',
        });
      },
      (error: any) => {
        assert(error.message.includes('does not exist'), 'Error should mention table does not exist');
        return true;
      }
    );
  });

  await t.test('getMetadata with missing table', async () => {
    await assert.rejects(
      async () => {
        await getMetadata({
          tableBucketARN: config.tableBucketARN,
          namespace: 'default',
          name: 'nonexistent_table',
        });
      },
      (error: any) => {
        assert(error.message.includes('does not exist'), 'Error should mention table does not exist');
        return true;
      }
    );
  });

  await t.test('getMetadata with invalid table ARN', async () => {
    await assert.rejects(
      async () => {
        await getMetadata({
          tableArn: 'arn:aws:s3tables:us-west-2:123456789012:bucket/invalid/table/nonexistent',
        });
      },
      (error: any) => {
        assert(error.message.includes('not valid'), 'Error should mention ARN is not valid');
        return true;
      }
    );
  });

  await t.test('addSchema with missing namespace', async () => {
    await assert.rejects(
      async () => {
        await addSchema({
          tableBucketARN: config.tableBucketARN,
          namespace: 'nonexistent_namespace',
          name: 'nonexistent_table',
          schemaId: 1,
          fields: [
            { id: 1, name: 'test_field', type: 'string', required: true },
          ],
        });
      },
      (error: any) => {
        assert(error.message.includes('does not exist'), 'Error should mention table does not exist');
        return true;
      }
    );
  });

  await t.test('addSchema with missing table', async () => {
    await assert.rejects(
      async () => {
        await addSchema({
          tableBucketARN: config.tableBucketARN,
          namespace: 'default',
          name: 'nonexistent_table',
          schemaId: 1,
          fields: [
            { id: 1, name: 'test_field', type: 'string', required: true },
          ],
        });
      },
      (error: any) => {
        assert(error.message.includes('does not exist'), 'Error should mention table does not exist');
        return true;
      }
    );
  });

  await t.test('addPartitionSpec with missing namespace', async () => {
    await assert.rejects(
      async () => {
        await addPartitionSpec({
          tableBucketARN: config.tableBucketARN,
          namespace: 'nonexistent_namespace',
          name: 'nonexistent_table',
          specId: 1,
          fields: [
            {
              'field-id': 1000,
              name: 'test_partition',
              'source-id': 1,
              transform: 'identity',
            },
          ],
        });
      },
      (error: any) => {
        assert(error.message.includes('does not exist'), 'Error should mention table does not exist');
        return true;
      }
    );
  });

  await t.test('addPartitionSpec with missing table', async () => {
    await assert.rejects(
      async () => {
        await addPartitionSpec({
          tableBucketARN: config.tableBucketARN,
          namespace: 'default',
          name: 'nonexistent_table',
          specId: 1,
          fields: [
            {
              'field-id': 1000,
              name: 'test_partition',
              'source-id': 1,
              transform: 'identity',
            },
          ],
        });
      },
      (error: any) => {
        assert(error.message.includes('does not exist'), 'Error should mention table does not exist');
        return true;
      }
    );
  });

  await t.test('addSchema with invalid field type', async () => {
    await assert.rejects(
      async () => {
        await addSchema({
          tableBucketARN: config.tableBucketARN,
          namespace: 'default',
          name: 'nonexistent_table',
          schemaId: 1,
          fields: [
            { id: 1, name: 'test_field', type: 'invalid_type' as any, required: true },
          ],
        });
      },
      (error: any) => {
        assert(error instanceof Error, 'Should throw an error for invalid field type');
        return true;
      }
    );
  });

  await t.test('addSchema with duplicate field ids', async () => {
    await assert.rejects(
      async () => {
        await addSchema({
          tableBucketARN: config.tableBucketARN,
          namespace: 'default',
          name: 'nonexistent_table',
          schemaId: 1,
          fields: [
            { id: 1, name: 'field1', type: 'string', required: true },
            { id: 1, name: 'field2', type: 'string', required: true },
          ],
        });
      },
      (error: any) => {
        assert(error instanceof Error, 'Should throw an error for duplicate field ids');
        return true;
      }
    );
  });

  await t.test('addPartitionSpec with invalid transform', async () => {
    await assert.rejects(
      async () => {
        await addPartitionSpec({
          tableBucketARN: config.tableBucketARN,
          namespace: 'default',
          name: 'nonexistent_table',
          specId: 1,
          fields: [
            {
              'field-id': 1000,
              name: 'test_partition',
              'source-id': 1,
              transform: 'invalid_transform' as any,
            },
          ],
        });
      },
      (error: any) => {
        assert(error instanceof Error, 'Should throw an error for invalid transform');
        return true;
      }
    );
  });

  await t.test('addPartitionSpec with duplicate field ids', async () => {
    await assert.rejects(
      async () => {
        await addPartitionSpec({
          tableBucketARN: config.tableBucketARN,
          namespace: 'default',
          name: 'nonexistent_table',
          specId: 1,
          fields: [
            {
              'field-id': 1000,
              name: 'partition1',
              'source-id': 1,
              transform: 'identity',
            },
            {
              'field-id': 1000,
              name: 'partition2',
              'source-id': 2,
              transform: 'identity',
            },
          ],
        });
      },
      (error: any) => {
        assert(error instanceof Error, 'Should throw an error for duplicate partition field ids');
        return true;
      }
    );
  });
});
