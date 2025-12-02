#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 2 ]; then
  echo "Usage: $0 <table-bucket-arn> <namespace1> [namespace2 ...]"
  exit 1
fi

BUCKET_ARN="$1"
shift

for NS in "$@"; do
  echo "Processing namespace: $NS"

  #
  # 1. List tables in namespace
  #
  TABLES=$(aws s3tables list-tables \
    --table-bucket-arn "$BUCKET_ARN" \
    --namespace "$NS" \
    --query 'tables[].name' \
    --output text \
    2>/dev/null || true)

  if [ -z "$TABLES" ]; then
    echo "  No tables found."
  else
    echo "  Deleting tables..."
    for T in $TABLES; do
      echo "    - Deleting table: $T"
      aws s3tables delete-table \
        --table-bucket-arn "$BUCKET_ARN" \
        --namespace "$NS" \
        --name "$T"
    done
  fi

  #
  # 2. Delete the namespace
  #
  echo "  Deleting namespace: $NS"
  aws s3tables delete-namespace \
    --table-bucket-arn "$BUCKET_ARN" \
    --name "$NS"
done

echo "Done."
