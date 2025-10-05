import { SignatureV4 } from '@smithy/signature-v4';
import { Sha256 } from '@aws-crypto/sha256-js';
import { HttpRequest } from '@smithy/protocol-http';
import { defaultProvider } from '@aws-sdk/credential-provider-node';

import { parse, stringify } from './json';

import type { JSONObject } from './json';
import type { AwsCredentialIdentity } from '@aws-sdk/types';

export default { icebergRequest };

interface GetParams {
  credentials?: AwsCredentialIdentity | undefined;
  tableBucketARN: string;
  method?: string;
  suffix: string;
  body?: unknown;
}
export async function icebergRequest<T = JSONObject>(
  params: GetParams
): Promise<T> {
  const region = params.tableBucketARN.split(':')[3];
  if (!region) {
    throw new Error('bad tableBucketARN');
  }
  const arn = encodeURIComponent(params.tableBucketARN);
  const hostname = `s3tables.${region}.amazonaws.com`;
  const full_path = `/iceberg/v1/${arn}${params.suffix}`;

  const body = params.body ? stringify(params.body) : null;
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
  const signer = new SignatureV4({
    credentials: params.credentials ?? defaultProvider(),
    region,
    service: 's3tables',
    sha256: Sha256,
  });
  const signed = await signer.sign(request);
  const url = `https://${hostname}${signed.path}`;
  const fetch_opts: Parameters<typeof fetch>[1] = {
    method: signed.method,
    headers: signed.headers as Record<string, string>,
  };
  if (signed.body) {
    fetch_opts.body = signed.body as string;
  }
  const res = await fetch(url, fetch_opts);
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`request failed: ${res.status} ${res.statusText} ${text}`);
  }
  try {
    return parse(text) as T;
  } catch {
    return text as T;
  }
}
