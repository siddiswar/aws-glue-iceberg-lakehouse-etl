#!/usr/bin/env bash
set -euo pipefail

# ----------------------------
# Config (override via env vars)
# ----------------------------
REGION="${REGION:-eu-west-2}"
STACK_NAME="${STACK_NAME:-e2e-aws-glue-iceberg-lakehouse-stack}"

# Optional: override these if you want different values than template defaults.
# If you leave them empty, CloudFormation will use template defaults.
BUCKET_NAME="${BUCKET_NAME:-}"          # e.g. siddu-lakehouse-glue-iceberg-bucket-081139831514
WAREHOUSE_PREFIX="${WAREHOUSE_PREFIX:-}" # e.g. warehouse/
GLUE_SCRIPTS_PREFIX="${GLUE_SCRIPTS_PREFIX:-}" # e.g. glue-scripts/
TIMEZONE="${TIMEZONE:-}"                # e.g. Europe/London

# ----------------------------
# Helpers
# ----------------------------
need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "ERROR: '$1' not found on PATH."
    exit 1
  }
}

# CloudFormation Output helper (no jq required)
get_stack_output() {
  local output_key="$1"
  aws cloudformation describe-stacks \
    --region "${REGION}" \
    --stack-name "${STACK_NAME}" \
    --query "Stacks[0].Outputs[?OutputKey=='${output_key}'].OutputValue | [0]" \
    --output text
}

# Build parameter overrides string
build_parameter_overrides() {
  local params=()
  [[ -n "${BUCKET_NAME}" ]] && params+=("BucketName=${BUCKET_NAME}")
  [[ -n "${WAREHOUSE_PREFIX}" ]] && params+=("WarehousePrefix=${WAREHOUSE_PREFIX}")
  [[ -n "${GLUE_SCRIPTS_PREFIX}" ]] && params+=("GlueScriptsPrefix=${GLUE_SCRIPTS_PREFIX}")
  [[ -n "${TIMEZONE}" ]] && params+=("Timezone=${TIMEZONE}")

  if [[ ${#params[@]} -gt 0 ]]; then
    echo "--parameter-overrides ${params[*]}"
  else
    echo ""
  fi
}

# ----------------------------
# Main
# ----------------------------
need aws
need sam

echo "==> SAM build (region=${REGION}, stack=${STACK_NAME})"
sam build

PARAM_OVERRIDES="$(build_parameter_overrides)"

echo "==> SAM deploy (non-interactive)"
# Notes:
# - CAPABILITY_NAMED_IAM is required because the template sets RoleName for IAM roles.
# - --no-confirm-changeset avoids interactive prompt.
# - --no-fail-on-empty-changeset makes re-runs painless.
sam deploy \
  --region "${REGION}" \
  --stack-name "${STACK_NAME}" \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
  --no-confirm-changeset \
  --no-fail-on-empty-changeset \
  ${PARAM_OVERRIDES}

echo "==> Reading deployed outputs"
DEPLOYED_BUCKET="$(get_stack_output BucketName)"
STATE_MACHINE_ARN="$(get_stack_output StateMachineArn)"

if [[ -z "${DEPLOYED_BUCKET}" || "${DEPLOYED_BUCKET}" == "None" ]]; then
  echo "ERROR: Could not read BucketName output from stack '${STACK_NAME}'."
  echo "Check CloudFormation Outputs and ensure OutputKey 'BucketName' exists."
  exit 1
fi

# If GlueScriptsPrefix param wasn't provided, fall back to template default (glue-scripts/)
PREFIX="${GLUE_SCRIPTS_PREFIX:-glue-scripts/}"

echo "==> Syncing Glue scripts to s3://${DEPLOYED_BUCKET}/${PREFIX}"
aws s3 sync glue/ "s3://${DEPLOYED_BUCKET}/${PREFIX}" --region "${REGION}"

echo ""
echo "✅ Deployed stack: ${STACK_NAME}"
echo "✅ Bucket: ${DEPLOYED_BUCKET}"
echo "✅ State machine: ${STATE_MACHINE_ARN}"
echo ""
echo "Next:"
echo " - Run on-demand: Start execution of state machine 'etl_daily_pipeline' with input {}"
