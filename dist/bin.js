#!/usr/bin/env node
'use strict';

var nodeS3tables = require('node-s3tables');

/* eslint-disable no-console */
const [tableBucketARN, namespace, name] = process.argv.slice(2);
if (!tableBucketARN || !namespace || !name) {
    console.error('Usage: node-s3tables compact <tableBucketARN> <namespace> <name>');
    process.exit(-1);
}
nodeS3tables.manifestCompact({ tableBucketARN, namespace, name })
    .then((result) => {
    console.log('Compact result:', result);
    process.exit(0);
})
    .catch((error) => {
    if (error instanceof Error) {
        console.error('Error:', error.message);
    }
    else {
        console.error('Error:', error);
    }
    process.exit(1);
});
