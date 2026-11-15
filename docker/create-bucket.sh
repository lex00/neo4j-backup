#!/bin/sh
# One-shot init for the local S3 emulator (floci): create the demo KMS key alias and the
# backup bucket, and set the bucket's default encryption to that key. Idempotent.
# Runs in the amazon/aws-cli image with AWS_ENDPOINT_URL pointing at the emulator.
set -eu

if ! aws kms describe-key --key-id "alias/$KMS_KEY_NAME" >/dev/null 2>&1; then
  key_id=$(aws kms create-key --description "$KMS_KEY_NAME" --query KeyMetadata.KeyId --output text)
  aws kms create-alias --alias-name "alias/$KMS_KEY_NAME" --target-key-id "$key_id"
fi

if ! aws s3api head-bucket --bucket "$BACKUP_BUCKET" >/dev/null 2>&1; then
  aws s3api create-bucket --bucket "$BACKUP_BUCKET" >/dev/null
fi

# floci records this but does not apply it to objects (see docker/compose.yaml).
aws s3api put-bucket-encryption --bucket "$BACKUP_BUCKET" \
  --server-side-encryption-configuration \
  "{\"Rules\":[{\"ApplyServerSideEncryptionByDefault\":{\"SSEAlgorithm\":\"aws:kms\",\"KMSMasterKeyID\":\"alias/$KMS_KEY_NAME\"}}]}"

echo "bucket $BACKUP_BUCKET ready (KMS key alias/$KMS_KEY_NAME)"
