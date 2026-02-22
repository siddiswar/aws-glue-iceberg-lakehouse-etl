#!/usr/bin/env bash
set -euo pipefail

REGION="${REGION:-eu-west-2}"
STACK_NAME="${STACK_NAME:-e2e-aws-glue-iceberg-lakehouse-stack}"

# Optional: also delete SAM CLI managed default stack/bucket
DELETE_SAM_MANAGED_DEFAULT="${DELETE_SAM_MANAGED_DEFAULT:-false}"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "ERROR: '$1' not found on PATH."
    exit 1
  }
}

stack_exists() {
  aws cloudformation describe-stacks \
    --region "${REGION}" \
    --stack-name "${STACK_NAME}" >/dev/null 2>&1
}

get_stack_output() {
  local output_key="$1"
  aws cloudformation describe-stacks \
    --region "${REGION}" \
    --stack-name "${STACK_NAME}" \
    --query "Stacks[0].Outputs[?OutputKey=='${output_key}'].OutputValue | [0]" \
    --output text
}

# Try to discover schedules created by this stack using CloudFormation resource list.
# Then disable them via scheduler API.
disable_schedules_from_stack() {
  echo "==> Disabling Scheduler schedules created by stack (if any)"
  local schedule_names
  schedule_names="$(aws cloudformation list-stack-resources \
    --region "${REGION}" \
    --stack-name "${STACK_NAME}" \
    --query "StackResourceSummaries[?ResourceType=='AWS::Scheduler::Schedule'].PhysicalResourceId" \
    --output text 2>/dev/null || true)"

  if [[ -z "${schedule_names}" || "${schedule_names}" == "None" ]]; then
    echo "    (No AWS::Scheduler::Schedule resources found in stack)"
    return 0
  fi

  # list-stack-resources can return one or more names separated by whitespace
  for sched in ${schedule_names}; do
    echo "    Disabling schedule: ${sched}"
    aws scheduler update-schedule \
      --region "${REGION}" \
      --name "${sched}" \
      --state DISABLED \
      --flexible-time-window '{"Mode":"OFF"}' \
      --schedule-expression "cron(0 6 * * ? *)" \
      --schedule-expression-timezone "Europe/London" \
      --target "$(aws scheduler get-schedule --region "${REGION}" --name "${sched}" --query 'Target' --output json)" \
      >/dev/null
  done
}

empty_bucket() {
  local bucket="$1"
  echo "==> Emptying bucket: s3://${bucket}"
  aws s3 rm "s3://${bucket}" --recursive --region "${REGION}" || true
}

delete_stack() {
  echo "==> Deleting stack: ${STACK_NAME}"
  aws cloudformation delete-stack --stack-name "${STACK_NAME}" --region "${REGION}"
  echo "==> Waiting for stack delete to complete..."
  aws cloudformation wait stack-delete-complete --stack-name "${STACK_NAME}" --region "${REGION}"
  echo "✅ Stack deleted: ${STACK_NAME}"
}

delete_sam_managed_default() {
  local sam_stack="aws-sam-cli-managed-default"
  echo "==> Attempting to delete SAM managed default stack: ${sam_stack}"

  # Find the managed bucket name from that stack outputs/resources.
  # Usually it includes a bucket like aws-sam-cli-managed-default-samclisourcebucket-...
  local bucket
  bucket="$(aws cloudformation list-stack-resources \
    --region "${REGION}" \
    --stack-name "${sam_stack}" \
    --query "StackResourceSummaries[?ResourceType=='AWS::S3::Bucket'].PhysicalResourceId | [0]" \
    --output text 2>/dev/null || true)"

  if [[ -n "${bucket}" && "${bucket}" != "None" ]]; then
    empty_bucket "${bucket}"
  else
    echo "    (Could not find SAM managed bucket resource; skipping bucket empty)"
  fi

  aws cloudformation delete-stack --stack-name "${sam_stack}" --region "${REGION}" || true
  aws cloudformation wait stack-delete-complete --stack-name "${sam_stack}" --region "${REGION}" || true
  echo "✅ SAM managed default stack deleted (if it existed): ${sam_stack}"
}

main() {
  need aws

  if ! stack_exists; then
    echo "Stack '${STACK_NAME}' not found in ${REGION}. Nothing to delete."
    exit 0
  fi

  # Best effort: disable schedules so nothing triggers mid-delete.
  disable_schedules_from_stack || true

  # Empty the main lake bucket so CloudFormation can delete it.
  local lake_bucket
  lake_bucket="$(get_stack_output BucketName)"

  if [[ -n "${lake_bucket}" && "${lake_bucket}" != "None" ]]; then
    empty_bucket "${lake_bucket}"
  else
    echo "WARNING: Could not read 'BucketName' output from stack. Skipping bucket empty step."
    echo "         If stack deletion fails with BucketNotEmpty, empty the bucket manually and retry."
  fi

  delete_stack

  if [[ "${DELETE_SAM_MANAGED_DEFAULT}" == "true" ]]; then
    delete_sam_managed_default
  else
    echo "==> Skipping deletion of aws-sam-cli-managed-default (set DELETE_SAM_MANAGED_DEFAULT=true to delete it)."
  fi
}

main "$@"