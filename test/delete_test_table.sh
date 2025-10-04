#!/bin/bash

# Usage: delete_test_table.sh <table> <namespace>
# Example: ./delete_test_table.sh my_table my_namespace

set -euo pipefail

TABLE_BUCKET_ARN="$1"
TABLE_NAME="$2"
NAMESPACE="$3"

if [[ -z "$TABLE_BUCKET_ARN" || -z "$TABLE_NAME" || -z "$NAMESPACE" ]]; then
  echo "Usage: $0 <table_bucket_arn> <table> <namespace>"
  exit 1
fi

echo "🧹 Deleting table '$TABLE_NAME' in namespace '$NAMESPACE'..."

# Delete the table
if aws s3tables delete-table \
    --table-bucket-arn "$TABLE_BUCKET_ARN" \
    --name "$TABLE_NAME" \
    --namespace "$NAMESPACE"; then
  echo "✅ Table '$TABLE_NAME' deleted."
else
  echo "⚠️ Failed to delete table '$TABLE_NAME'."
  exit 1
fi

echo "🧹 Deleting namespace '$NAMESPACE'..."

# Delete the namespace
if aws s3tables delete-namespace \
    --table-bucket-arn "$TABLE_BUCKET_ARN" \
    --namespace "$NAMESPACE"; then
  echo "✅ Namespace '$NAMESPACE' deleted."
else
  echo "⚠️ Failed to delete namespace '$NAMESPACE'."
  exit 1
fi

echo "🎯 Done."
