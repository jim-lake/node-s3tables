import {
  S3TablesClient,
  CreateNamespaceCommand,
  CreateTableCommand,
  DeleteTableCommand,
  DeleteNamespaceCommand,
} from '@aws-sdk/client-s3tables';
import { S3Client } from '@aws-sdk/client-s3';
import { LakeFormationClient } from '@aws-sdk/client-lakeformation';
import { AthenaClient } from '@aws-sdk/client-athena';

const tableBucketARN = process.env['TABLE_BUCKET_ARN'];
const catalogId = process.env['CATALOG_ID'];
const outputBucket = process.env['OUTPUT_BUCKET'];

if (!tableBucketARN) {
  throw new Error('environment requires TABLE_BUCKET_ARN');
}
if (!catalogId) {
  throw new Error('environment requires CATALOG_ID');
}
if (!outputBucket) {
  throw new Error('environment requires OUTPUT_BUCKET');
}

const region = tableBucketARN.split(':')[3];

export const clients = {
  s3Tables: new S3TablesClient(),
  s3: new S3Client(region ? { region } : {}),
  lakeFormation: new LakeFormationClient(region ? { region } : {}),
  athena: new AthenaClient(region ? { region } : {}),
};

export const config = { tableBucketARN, catalogId, outputBucket, region };

export {
  CreateNamespaceCommand,
  CreateTableCommand,
  DeleteTableCommand,
  DeleteNamespaceCommand,
};
