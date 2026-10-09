import type { S3ClientConfig } from '@aws-sdk/client-s3';
import type { S3TablesClientConfig } from '@aws-sdk/client-s3tables';
import type { AwsCredentialIdentity } from '@aws-sdk/types';

export type { S3ClientConfig, S3TablesClientConfig };

/**
 * User-facing AWS configuration accepted by every public API in this library.
 *
 * `region` and `credentials` are convenient top-level shortcuts applied to both
 * service configs. `s3ClientConfig` and `s3TablesClientConfig` are spread
 * directly into the corresponding AWS SDK client constructors, so any
 * constructor option is supported (for example `useDualstackEndpoint: true` for
 * IPv6 / public-dual-stack environments, or a custom `endpoint`).
 */
export interface AwsConfigParams {
  region?: string | undefined;
  credentials?: AwsCredentialIdentity | undefined;
  s3ClientConfig?: S3ClientConfig | undefined;
  s3TablesClientConfig?: S3TablesClientConfig | undefined;
}

/**
 * Normalized configuration carried internally. Both service config objects
 * always exist (possibly empty) so downstream code can spread them into SDK
 * constructors without null checks. This single object is passed by reference
 * through every internal function; it is never destructured and reassembled.
 */
export interface AwsConfig {
  region?: string | undefined;
  s3ClientConfig: S3ClientConfig;
  s3TablesClientConfig: S3TablesClientConfig;
}

/**
 * Resolve the user-facing shortcuts into a single {@link AwsConfig}. Explicit
 * values inside `s3ClientConfig` / `s3TablesClientConfig` always win over the
 * `region` / `credentials` shortcuts. Call this once at a public entry point;
 * pass the result around unchanged.
 */
export function resolveAwsConfig(params: AwsConfigParams): AwsConfig {
  const { region, credentials } = params;
  const s3ClientConfig: S3ClientConfig = { ...params.s3ClientConfig };
  const s3TablesClientConfig: S3TablesClientConfig = {
    ...params.s3TablesClientConfig,
  };

  if (region !== undefined) {
    s3ClientConfig.region ??= region;
    s3TablesClientConfig.region ??= region;
  }
  if (credentials !== undefined) {
    s3ClientConfig.credentials ??= credentials;
    s3TablesClientConfig.credentials ??= credentials;
  }

  const resolvedRegion =
    (typeof s3TablesClientConfig.region === 'string'
      ? s3TablesClientConfig.region
      : undefined) ??
    (typeof s3ClientConfig.region === 'string'
      ? s3ClientConfig.region
      : undefined) ??
    region;

  return { region: resolvedRegion, s3ClientConfig, s3TablesClientConfig };
}
