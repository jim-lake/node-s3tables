/* eslint-disable no-console */
import { parseArgs } from 'node:util';
import {
  manifestCompact,
  addDataFiles,
  importRedshiftManifest,
} from 'node-s3tables';
import type { AddFile, AddDataFilesParams } from 'node-s3tables';

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
  process.exit(1);
});

const { positionals, values } = parseArgs({
  allowPositionals: true,
  options: {
    'force-rewrite': { type: 'boolean' },
    'spec-id': { type: 'string' },
    'schema-id': { type: 'string' },
    files: { type: 'string' },
    'max-snapshots': { type: 'string' },
    'redshift-manifest-url': { type: 'string' },
    'rewrite-parquet': { type: 'boolean' },
  },
});

const [command, tableBucketARN, namespace, name] = positionals;

if (!command || !tableBucketARN || !namespace || !name) {
  console.error(
    'Usage: node-s3tables <command> <tableBucketARN> <namespace> <name> [options]\n'
  );
  console.error('Commands:');
  console.error('  compact      Compact manifest files');
  console.error('    Options: [--force-rewrite]');
  console.error('');
  console.error('  add_files    Add data files to table');
  console.error(
    '    Options: --spec-id <id> --schema-id <id> --files <json> [--max-snapshots <n>]'
  );
  console.error(
    '    Example: --files \'[{"file":"s3://bucket/data.parquet","partitions":{},"recordCount":"1000","fileSize":"52428"}]\''
  );
  console.error('');
  console.error(
    '  import_redshift    Import redshift manifest created by UNLOAD'
  );
  console.error(
    '    Options: --redshift-manifest-url <s3url> [--rewrite-parquet]'
  );
  console.error('');
  process.exit(-1);
}

if (command === 'compact') {
  console.log(
    'Compact:',
    tableBucketARN,
    namespace,
    name,
    'forceRewrite:',
    Boolean(values['force-rewrite'])
  );
  manifestCompact({
    tableBucketARN,
    namespace,
    name,
    forceRewrite: Boolean(values['force-rewrite']),
  })
    .then((result: unknown) => {
      console.log('Compact result:', result);
      process.exit(0);
    })
    .catch((error: unknown) => {
      console.error('Error:', error);
      process.exit(1);
    });
} else if (command === 'add_files') {
  const specId = values['spec-id'];
  const schemaId = values['schema-id'];
  const filesJson = values.files;

  if (!specId || !schemaId || !filesJson) {
    console.error('Error: Missing required options for add_files command\n');
    console.error(
      'Usage: node-s3tables add_files <tableBucketARN> <namespace> <name> --spec-id <id> --schema-id <id> --files <json> [--max-snapshots <n>]\n'
    );
    console.error('Example:');
    console.error(
      '  --spec-id 1 --schema-id 2 --files \'[{"file":"s3://bucket/data.parquet","partitions":{"date":"2024-01-01"},"recordCount":"1000","fileSize":"52428"}]\''
    );
    process.exit(-1);
  }

  const files = JSON.parse(filesJson) as AddFile[];
  const maxSnapshots = values['max-snapshots']
    ? parseInt(values['max-snapshots'], 10)
    : undefined;

  console.log('Adding files:', tableBucketARN, namespace, name);
  const params: AddDataFilesParams = {
    tableBucketARN,
    namespace,
    name,
    lists: [
      { specId: parseInt(specId, 10), schemaId: parseInt(schemaId, 10), files },
    ],
  };
  if (maxSnapshots !== undefined) {
    params.maxSnapshots = maxSnapshots;
  }
  addDataFiles(params)
    .then((result: unknown) => {
      console.log('Add files result:', result);
      process.exit(0);
    })
    .catch((error: unknown) => {
      console.error('Error:', error);
      process.exit(1);
    });
} else if (command === 'import_redshift') {
  const redshiftManifestUrl = values['redshift-manifest-url'];
  if (!redshiftManifestUrl) {
    console.error(
      'Error: Missing required options for import_redshift command\n'
    );
    console.error(
      'Usage: node-s3tables import_redshift <tableBucketARN> <namespace> <name> --redshift-manfiest-url <s3url>\n'
    );
    console.error('Example:');
    console.error(
      '  --redshift-manifest-url s3://s3table-bucket/exported_manfiest.json'
    );
    process.exit(-1);
  }
  const rewriteParquet = Boolean(values['rewrite-parquet']);
  console.log(
    'Importing file:',
    redshiftManifestUrl,
    'to:',
    tableBucketARN,
    namespace,
    name,
    'rewrite parquet:',
    rewriteParquet
  );
  importRedshiftManifest({
    tableBucketARN,
    namespace,
    name,
    redshiftManifestUrl,
    rewriteParquet,
  })
    .then((result: unknown) => {
      console.log('Import result:', result);
      process.exit(0);
    })
    .catch((error: unknown) => {
      console.error('Error:', error);
      process.exit(1);
    });
} else {
  console.error('Unknown command:', command);
  process.exit(-1);
}
