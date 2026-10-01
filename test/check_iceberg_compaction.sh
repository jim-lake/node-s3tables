#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <table-bucket-arn>" >&2
  exit 1
fi

BUCKET_ARN="$1"

# The ARN always contains the region in field 4:
# arn:aws:s3tables:<region>:<account>:bucket/<name>
REGION="$(cut -d: -f4 <<< "$BUCKET_ARN")"
if [[ -z "$REGION" ]]; then
  echo "error: could not parse region from ARN: $BUCKET_ARN" >&2
  exit 1
fi
export AWS_DEFAULT_REGION="$REGION"

aws s3tables list-namespaces \
  --table-bucket-arn "$BUCKET_ARN" \
  --output json |
jq -r '.namespaces[].namespace | join(".")' |
while read -r NAMESPACE; do

  [[ -z "$NAMESPACE" ]] && continue

  if ! TABLES_JSON=$(aws s3tables list-tables \
      --table-bucket-arn "$BUCKET_ARN" \
      --namespace "$NAMESPACE" \
      --output json 2>/dev/null); then
    continue
  fi

  jq -r '.tables[].name' <<< "$TABLES_JSON" |
  while read -r TABLE; do

    STATUS_JSON=$(aws s3tables get-table-maintenance-job-status \
      --table-bucket-arn "$BUCKET_ARN" \
      --namespace "$NAMESPACE" \
      --name "$TABLE" \
      --output json)

    COMPACTION_STATUS=$(jq -r '.status.icebergCompaction.status // "UNKNOWN"' <<< "$STATUS_JSON")

    case "$COMPACTION_STATUS" in
      Successful|Not_Yet_Run)
        ;; # ignore
      *)
        echo "$NAMESPACE/$TABLE - icebergCompaction.status=$COMPACTION_STATUS"
        ;;
    esac

  done
done
