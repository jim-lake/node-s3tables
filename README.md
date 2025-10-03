# node-s3tables

A Node.js library for interacting with AWS S3 Tables using the Iceberg REST API.

## Installation

```bash
npm install node-s3tables
```

## Quick Start

```javascript
import {
  getMetadata,
  addSchema,
  addPartitionSpec,
  addDataFiles,
  setCurrentCommit,
} from 'node-s3tables';

// Get table metadata
const metadata = await getMetadata({
  tableArn:
    'arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket/table/my-table-id',
});

// Add a new schema
await addSchema({
  tableBucketARN: 'arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket',
  namespace: 'my_namespace',
  name: 'my_table',
  schemaId: 2,
  fields: [
    { id: 1, name: 'id', required: true, type: 'long' },
    { id: 2, name: 'name', required: false, type: 'string' },
  ],
});
```

## API Reference

### getMetadata(params)

Retrieves Iceberg metadata for an S3 table.

**Parameters:**

- `params.tableArn` (string) - The ARN of the table
- OR `params.tableBucketARN` (string) + `params.namespace` (string) + `params.name` (string)
- `params.config` (S3TablesClientConfig, optional) - AWS SDK configuration

**Returns:** Promise<IcebergMetadata>

```javascript
// Using table ARN
const metadata = await getMetadata({
  tableArn:
    'arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket/table/my-table-id',
});

// Using bucket ARN + namespace + name
const metadata = await getMetadata({
  tableBucketARN: 'arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket',
  namespace: 'my_namespace',
  name: 'my_table',
});
```

### addSchema(params)

Adds a new schema to an S3 table and sets it as current.

**Parameters:**

- `params.tableBucketARN` (string) - The ARN of the table bucket
- `params.namespace` (string) - The namespace name
- `params.name` (string) - The table name
- `params.schemaId` (number) - The new schema ID
- `params.fields` (IcebergSchemaField[]) - Array of schema fields
- `params.credentials` (AwsCredentialIdentity, optional) - AWS credentials

**Returns:** Promise<string>

```javascript
await addSchema({
  tableBucketARN: 'arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket',
  namespace: 'sales',
  name: 'daily_sales',
  schemaId: 2,
  fields: [
    { id: 1, name: 'sale_date', required: false, type: 'date' },
    { id: 2, name: 'product_category', required: false, type: 'string' },
    { id: 3, name: 'sales_amount', required: false, type: 'double' },
  ],
});
```

### addPartitionSpec(params)

Adds a new partition specification to an S3 table and sets it as default.

**Parameters:**

- `params.tableBucketARN` (string) - The ARN of the table bucket
- `params.namespace` (string) - The namespace name
- `params.name` (string) - The table name
- `params.specId` (number) - The new partition spec ID
- `params.fields` (IcebergPartitionField[]) - Array of partition fields
- `params.credentials` (AwsCredentialIdentity, optional) - AWS credentials

**Returns:** Promise<string>

```javascript
await addPartitionSpec({
  tableBucketARN: 'arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket',
  namespace: 'sales',
  name: 'daily_sales',
  specId: 1,
  fields: [
    {
      'field-id': 1000,
      name: 'sale_date_day',
      'source-id': 1,
      transform: 'day',
    },
    {
      'field-id': 1001,
      name: 'product_category',
      'source-id': 2,
      transform: 'identity',
    },
  ],
});
```

### addDataFiles(params)

Adds data files to an S3 table by creating a new snapshot.

**Parameters:**

- `params.tableBucketARN` (string) - The ARN of the table bucket
- `params.namespace` (string) - The namespace name
- `params.name` (string) - The table name
- `params.file` (string) - S3 URL of the data file to add
- `params.specId` (number) - The partition spec ID to use
- `params.schemaId` (number) - The schema ID to use
- `params.partitions` (PartitionRecord) - Partition values for the data file
- `params.fileSize` (bigint) - Size of the data file in bytes
- `params.recordCount` (bigint) - Number of records in the data file
- `params.credentials` (AwsCredentialIdentity, optional) - AWS credentials

**Returns:** Promise<string>

```javascript
await addDataFiles({
  tableBucketARN: 'arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket',
  namespace: 'sales',
  name: 'daily_sales',
  file: 's3://my-bucket/data/sales-2024-01-01.parquet',
  schemaId: 2,
  specId: 1,
  partitions: { sale_date_day: '2024-01-01' },
  recordCount: 1000n,
  fileSize: 52428n,
});
```

### setCurrentCommit(params)

Sets the current commit/snapshot for an S3 table.

**Parameters:**

- `params.tableBucketARN` (string) - The ARN of the table bucket
- `params.namespace` (string) - The namespace name
- `params.name` (string) - The table name
- `params.snapshotId` (bigint) - The snapshot ID to set as current
- `params.credentials` (AwsCredentialIdentity, optional) - AWS credentials

**Returns:** Promise<string>

```javascript
await setCurrentCommit({
  tableBucketARN: 'arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket',
  namespace: 'sales',
  name: 'daily_sales',
  snapshotId: 4183020680887155442n,
});
```

## Type Definitions

### IcebergSchemaField

```typescript
interface IcebergSchemaField {
  id: number;
  name: string;
  type: IcebergType;
  required: boolean;
  doc?: string;
}
```

### IcebergPartitionField

```typescript
interface IcebergPartitionField {
  'field-id': number;
  name: string;
  'source-id': number;
  transform: IcebergTransform;
}
```

### IcebergType

Supported primitive types:

- `'boolean'`, `'int'`, `'long'`, `'float'`, `'double'`
- `'date'`, `'time'`, `'timestamp'`, `'timestamptz'`
- `'string'`, `'uuid'`, `'binary'`
- `'decimal(precision,scale)'`, `'fixed[length]'`

Complex types:

- List: `{ type: 'list', element: IcebergType, 'element-required': boolean }`
- Map: `{ type: 'map', key: IcebergType, value: IcebergType, 'value-required': boolean }`
- Struct: `{ type: 'struct', fields: IcebergSchemaField[] }`

### IcebergTransform

Supported partition transforms:

- `'identity'` - Use the field value as-is
- `'year'`, `'month'`, `'day'`, `'hour'` - Date/time transforms
- `'bucket[N]'` - Hash bucket with N buckets
- `'truncate[N]'` - Truncate strings to N characters

## Testing

Run the test suite:

```bash
npm test
```

Run tests with coverage:

```bash
npm run test:cover
```

The tests require environment variables to be set:

- `TABLE_ARN` - The ARN of an S3 table for testing
- `TABLE_BUCKET_ARN` - The ARN of the table bucket
- `TABLE_NAMESPACE` - The namespace name

## Configuration

The library uses the AWS SDK for authentication. Configure credentials using:

- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- AWS credentials file (`~/.aws/credentials`)
- IAM roles (when running on EC2/Lambda)
- Or pass credentials directly to functions

## License

MIT
