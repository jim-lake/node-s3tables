import { SignatureV4 } from '@smithy/signature-v4';
import { Sha256 } from '@aws-crypto/sha256-js';
import { HttpRequest } from '@smithy/protocol-http';
import { defaultProvider } from '@aws-sdk/credential-provider-node';

import type { AwsCredentialIdentity } from '@aws-sdk/types';

export default { icebergRequest };

interface GetParams {
  credentials?: AwsCredentialIdentity;
  tableBucketARN: string;
  method?: string;
  suffix: string;
  body?: unknown;
}
export async function icebergRequest(params: GetParams): Promise<string> {
  const region = params.tableBucketARN.split(':')[3];
  if (!region) {
    throw new Error('bad tableBucketARN');
  }
  console.log('region:', region);
  const arn = encodeURIComponent(params.tableBucketARN);
  const hostname = `s3tables.${region}.amazonaws.com`;
  const full_path = `/iceberg/v1/${arn}${params.suffix}`;

  const body = params.body ? JSON.stringify(params.body) : null;
  const req_opts: ConstructorParameters<typeof HttpRequest>[0] = {
    method: params.method ?? 'GET',
    protocol: 'https:',
    path: full_path,
    hostname,
    headers: { host: hostname },
  };
  if (body && req_opts.headers) {
    req_opts.body = body;
    req_opts.headers['content-type'] = 'application/json';
    req_opts.headers['content-length'] = String(Buffer.byteLength(body));
  }

  const request = new HttpRequest(req_opts);
  console.log('request:', request);
  const signer = new SignatureV4({
    credentials: params.credentials ?? defaultProvider(),
    region,
    service: 's3tables',
    sha256: Sha256,
  });
  const signed = await signer.sign(request);
  console.log('signed:', signed);
  const url = `https://${hostname}${signed.path}`;
  console.log('url:', url);
  const fetch_opts: Parameters<typeof fetch>[1] = {
    method: signed.method,
    headers: signed.headers as Record<string, string>,
  };
  if (signed.body) {
    fetch_opts.body = signed.body;
  }
  const res = await fetch(url, fetch_opts);
  if (!res.ok) {
    console.log('body:', await res.text());
    throw new Error(`request failed: ${res.status} ${res.statusText}`);
  }
  return (await res.json()) as string;
}
