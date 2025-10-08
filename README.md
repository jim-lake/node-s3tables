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
  addManifest,
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

**Returns:** Promise<IcebergUpdateResponse>

```javascript
const result = await addSchema({
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

// result.metadata contains the updated table metadata
// result['metadata-location'] contains the S3 path to the new metadata file
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

**Returns:** Promise<IcebergUpdateResponse>

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

### addManifest(params)

Creates a manifest file for data files and returns a manifest list record.

**Parameters:**

- `params.credentials` (AwsCredentialIdentity, optional) - AWS credentials
- `params.region` (string) - AWS region
- `params.metadata` (IcebergMetadata) - Table metadata
- `params.schemaId` (number) - Schema ID to use
- `params.specId` (number) - Partition spec ID to use
- `params.snapshotId` (bigint) - Snapshot ID
- `params.sequenceNumber` (bigint) - Sequence number
- `params.files` (AddFile[]) - Array of data files

**Returns:** Promise<ManifestListRecord>

```javascript
const manifestRecord = await addManifest({
  region: 'us-west-2',
  metadata: tableMetadata,
  schemaId: 2,
  specId: 1,
  snapshotId: 4183020680887155442n,
  sequenceNumber: 1n,
  files: [
    {
      file: 's3://my-bucket/data/sales-2024-01-01.parquet',
      partitions: { sale_date_day: '2024-01-01' },
      recordCount: 1000n,
      fileSize: 52428n,
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
- `params.lists` (AddFileList[]) - Array of file lists to add
- `params.credentials` (AwsCredentialIdentity, optional) - AWS credentials

**Returns:** Promise<string>

```javascript
await addDataFiles({
  tableBucketARN: 'arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket',
  namespace: 'sales',
  name: 'daily_sales',
  lists: [
    {
      specId: 1,
      schemaId: 2,
      files: [
        {
          file: 's3://my-bucket/data/sales-2024-01-01.parquet',
          partitions: { sale_date_day: '2024-01-01' },
          recordCount: 1000n,
          fileSize: 52428n,
        },
      ],
    },
  ],
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

### IcebergUpdateResponse

```typescript
interface IcebergUpdateResponse {
  metadata: IcebergMetadata;
  'metadata-location': string;
}
```

### AddFileList

```typescript
interface AddFileList {
  specId: number;
  schemaId: number;
  files: AddFile[];
}
```

### AddFile

```typescript
interface AddFile {
  file: string;
  partitions: PartitionRecord;
  fileSize: bigint;
  recordCount: bigint;
  columnSizes?: Record<string, bigint> | null;
  valueCounts?: Record<string, bigint> | null;
  nullValueCounts?: Record<string, bigint> | null;
  nanValueCounts?: Record<string, bigint> | null;
  lowerBounds?: Record<string, Buffer> | null;
  upperBounds?: Record<string, Buffer> | null;
  keyMetadata?: Buffer | null;
  splitOffsets?: bigint[] | null;
  equalityIds?: number[] | null;
  sortOrderId?: number | null;
}
```

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

## AWS API Calls and Required Permissions

The library makes calls to multiple AWS services and requires specific IAM permissions:

### S3 Tables Service

**API Calls:**

- `GetTable` - Used by `getMetadata()` when called with `tableArn`
- Iceberg REST API calls via HTTPS to `s3tables.{region}.amazonaws.com`

**Required Permissions:**

- `s3tables:GetTable` - For retrieving table information
- `s3tables:GetTableData` - For reading table metadata and data objects (includes GetObject, HeadObject, ListParts)
- `s3tables:PutTableData` - For writing table metadata and data objects (includes PutObject, multipart upload operations)
- `s3tables:UpdateTableMetadataLocation` - For updating table root pointer during metadata operations

### Function-Specific Permission Requirements

**`getMetadata()`:**

- When using `tableArn`: `s3tables:GetTable`, `s3tables:GetTableData`
- When using `tableBucketARN` + `namespace` + `name`: `s3tables:GetTableData`

**`addSchema()`:**

- `s3tables:PutTableData`, `s3tables:UpdateTableMetadataLocation`

**`addPartitionSpec()`:**

- `s3tables:PutTableData`, `s3tables:UpdateTableMetadataLocation`

**`addManifest()`:**

- `s3tables:PutTableData` (for writing manifest files)

**`addDataFiles()`:**

- `s3tables:GetTableData` (to get current metadata and read existing manifest lists)
- `s3tables:PutTableData` (to write new manifest files and lists)
- `s3tables:UpdateTableMetadataLocation` (to add snapshots)

**`setCurrentCommit()`:**

- `s3tables:PutTableData`, `s3tables:UpdateTableMetadataLocation`

### Example IAM Policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3tables:GetTable",
        "s3tables:GetTableData",
        "s3tables:PutTableData",
        "s3tables:UpdateTableMetadataLocation"
      ],
      "Resource": "arn:aws:s3tables:*:*:bucket/*/table/*"
    }
  ]
}
```

## Configuration

The library uses the AWS SDK for authentication. Configure credentials using:

- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- AWS credentials file (`~/.aws/credentials`)
- IAM roles (when running on EC2/Lambda)
- Or pass credentials directly to functions

## Testing

### Prerequisites

The tests require AWS credentials and S3 Tables resources. Set up the following environment variables in a `.env` file:

```bash
TABLE_BUCKET_ARN=arn:aws:s3tables:us-west-2:123456789012:bucket/your-test-bucket
CATALOG_ID=123456789012:s3tablescatalog/your-test-bucket
OUTPUT_BUCKET=your-output-bucket
```

### AWS Service Calls and Permissions

The tests make calls to multiple AWS services and require the following permissions:

**S3 Tables:**

- `s3tables:CreateNamespace`
- `s3tables:DeleteNamespace`
- `s3tables:CreateTable`
- `s3tables:DeleteTable`
- `s3tables:GetTableMetadata`
- `s3tables:UpdateTableMetadata`

**S3:**

- `s3:PutObject` (for uploading test Parquet files)
- `s3:GetObject` (for reading manifest files)

**Lake Formation:**

- `lakeformation:AddLFTagsToResource` (adds `AccessLevel: Public` tag to namespaces)

**Athena:**

- `athena:StartQueryExecution`
- `athena:GetQueryExecution`
- `athena:GetQueryResults`

**Lake Formation Setup:**
The tests expect a Lake Formation tag with key `AccessLevel` and value `Public` to exist in your account. This tag is automatically applied to test namespaces to allow Athena query permissions.

### Test Dependencies

The test suite uses additional dependencies for creating test data:

- `@aws-sdk/client-athena` - For running Athena queries in tests
- `@aws-sdk/client-lakeformation` - For Lake Formation permissions
- `parquetjs` - For creating test Parquet files
- `dotenv-cli` - For loading environment variables

### Running Tests

Run the test suite:

```bash
npm run test
```

Run tests with coverage:

```bash
npm run test:cover
```

Run a single test file:

```bash
npm run test:single test/create.test.ts
```

## License

MIT
