import { test } from 'node:test';
import { strict as assert } from 'node:assert';

import { resolveS3TablesHost } from '../src/request';
import { resolveAwsConfig } from '../src/config';

void test('resolveS3TablesHost - always uses dual-stack endpoint', () => {
  assert.equal(resolveS3TablesHost('us-west-2'), 's3tables.us-west-2.api.aws');
  assert.equal(
    resolveS3TablesHost('us-west-2', {}),
    's3tables.us-west-2.api.aws'
  );
  assert.equal(
    resolveS3TablesHost('eu-central-1', { useDualstackEndpoint: true }),
    's3tables.eu-central-1.api.aws'
  );
});

void test('resolveS3TablesHost - explicit endpoint URL wins', () => {
  assert.equal(
    resolveS3TablesHost('us-west-2', {
      endpoint: 'https://custom.example.com:8443/iceberg',
    }),
    'custom.example.com:8443'
  );
});

void test('resolveS3TablesHost - explicit bare host endpoint', () => {
  assert.equal(
    resolveS3TablesHost('us-west-2', { endpoint: 'localhost:9000' }),
    'localhost:9000'
  );
});

void test('resolveAwsConfig - folds region/credentials into both configs', () => {
  const creds = { accessKeyId: 'a', secretAccessKey: 'b' };
  const config = resolveAwsConfig({ region: 'us-west-2', credentials: creds });
  assert.equal(config.region, 'us-west-2');
  assert.equal(config.s3ClientConfig.region, 'us-west-2');
  assert.equal(config.s3TablesClientConfig.region, 'us-west-2');
  assert.equal(config.s3ClientConfig.credentials, creds);
  assert.equal(config.s3TablesClientConfig.credentials, creds);
});

void test('resolveAwsConfig - per-service config wins over shortcuts', () => {
  const config = resolveAwsConfig({
    region: 'us-west-2',
    s3TablesClientConfig: { region: 'eu-west-1', useDualstackEndpoint: true },
  });
  // s3TablesClientConfig keeps its explicit region
  assert.equal(config.s3TablesClientConfig.region, 'eu-west-1');
  assert.equal(config.s3TablesClientConfig.useDualstackEndpoint, true);
  // s3 client still gets the shortcut region
  assert.equal(config.s3ClientConfig.region, 'us-west-2');
  // resolved region prefers the tables client region
  assert.equal(config.region, 'eu-west-1');
});

void test('resolveAwsConfig - dual-stack flag propagates to both when set directly', () => {
  const config = resolveAwsConfig({
    region: 'us-west-2',
    s3ClientConfig: { useDualstackEndpoint: true },
    s3TablesClientConfig: { useDualstackEndpoint: true },
  });
  assert.equal(config.s3ClientConfig.useDualstackEndpoint, true);
  assert.equal(config.s3TablesClientConfig.useDualstackEndpoint, true);
});

void test('resolveAwsConfig - empty params produce empty service configs', () => {
  const config = resolveAwsConfig({});
  assert.deepEqual(config.s3ClientConfig, {});
  assert.deepEqual(config.s3TablesClientConfig, {});
  assert.equal(config.region, undefined);
});
