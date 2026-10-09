# AGENTS.md

Operational reference for working on **node-s3tables**.

## Project

Node.js/TypeScript library for AWS S3 Tables via the Iceberg REST API
(`s3tables.{region}.amazonaws.com/iceberg/v1/...`). Source in `src/`, tests in
`test/`. Public API is documented in `README.md`.

## Toolchain

- Node `v24.x`, npm `11.x`. Dependencies are installed (`node_modules/`).
- Test runner: `tsx --test` wrapped by `dotenv-cli`.
- Build: `rollup -c` → `dist/`. `dist/` is tracked in git; discard incidental
  rebuilds with `git checkout -- dist/` unless a dist change is intended.

```bash
npm run ts:check                       # tsc --noEmit
npm run build                          # rollup -> dist/
npm test                               # dotenv -- tsx --test test/*.test.ts
npm run test:single test/add.test.ts   # one or more files
npm run test:cover                     # with coverage
npx eslint src test                    # lint
```

Lint: `npm run lint` runs eslint with `--fix`. `src/s3_tools.ts`,
`test/avro_transform.test.ts`, and `test/manifest_list_decoder.test.ts` contain
pre-existing lint errors unrelated to tests; leave them unless asked.

## Test environment

Two groups:

1. **Unit tests** (no AWS): `avro_transform`, `bigint_serialization`,
   `manifest_list_decoder`, `random_bigint`, `schema_translator`. Run with no
   configuration.
2. **AWS integration tests** (all other files): import
   `test/helpers/aws_clients.ts`, which throws at import time if
   `TABLE_BUCKET_ARN`, `CATALOG_ID`, or `OUTPUT_BUCKET` are unset. One
   unconfigured file aborts the whole `npm test` run with no summary.

### Required `.env` (gitignored via `/.env*`)

```bash
TABLE_BUCKET_ARN=arn:aws:s3tables:us-west-2:540280374342:bucket/ares-staging-dc-report-test-table
CATALOG_ID=540280374342:s3tablescatalog/ares-staging-dc-report-test-table
OUTPUT_BUCKET=ares-staging-test-build
AWS_REGION=us-west-2
```

- Table bucket `ares-staging-dc-report-test-table` is in **us-west-2** (account
  `540280374342`), the scratch table bucket.
- `OUTPUT_BUCKET=ares-staging-test-build` is the Athena output bucket. The
  active role can `PutObject` but not `DeleteObject` there; tests only write.
- Credentials come from the EC2 instance role `build-box-role`. No static keys.

### Lake Formation

The active role is **not** a Lake Formation admin and cannot create or apply
LF-tags (the sole data-lake admin is `user/jlake`). Namespaces inherit
`IAM_ALLOWED_PRINCIPALS -> ALL` from the LF data-lake default settings, so
Athena queries work without any LF-tag.

`test/helpers/table_lifecycle.ts#setupTable` adds an `AccessLevel: Public`
LF-tag but treats failure as non-fatal and honors `SKIP_LF_TAG=1` to skip the
call. Keep this behavior: the tag is not required for tests to pass in this
account.

## Iceberg schema constraint

Adding a new **required** (non-nullable) column to an existing table is invalid
in Iceberg; the S3 Tables endpoint rejects it with
`400 "The specified metadata is not valid"`, which cascades into
`schema not found` / `spec not found` in dependent subtests. Any field
introduced by `addSchema` beyond the table's initial `CreateTable` schema must
be `required: false`. Fields that already exist in the current schema may stay
required. Data/snapshot operations (`addDataFiles`, append snapshots) are
unaffected by this constraint.

## Running AWS tests

- `setupTable` creates a randomly-named namespace (`test_ns_*`) + table and
  cleans up in a `t.after` hook. Set `NO_CLEANUP=1` to retain resources.
- A crash mid-`setupTable` can orphan a namespace. List and remove:

```bash
aws s3tables list-namespaces --table-bucket-arn "$TABLE_BUCKET_ARN" --region us-west-2
aws s3tables delete-table --table-bucket-arn "$TABLE_BUCKET_ARN" --namespace NS --name NAME --region us-west-2
aws s3tables delete-namespace --table-bucket-arn "$TABLE_BUCKET_ARN" --namespace NS --region us-west-2
```

- Do not leave non-test `.ts` files in `test/`; `test/*.test.ts` is the run
  glob, but any `.ts` under `test/` is still type-checked.

## Verification

```bash
# unit only (no AWS):
npx tsx --test test/avro_transform.test.ts test/bigint_serialization.test.ts \
  test/manifest_list_decoder.test.ts test/random_bigint.test.ts \
  test/schema_translator.test.ts

# full suite (needs .env): all files pass
npm test
```
