# gcloud spanner databases create (DATABASE : --instance=INSTANCE) [--async]
#         [--ddl=DDL] [--ddl-file=DDL_FILE]
#         [--kms-key=KMS_KEY : --kms-keyring=KMS_KEYRING
#           --kms-location=KMS_LOCATION --kms-project=KMS_PROJECT]
#         [GCLOUD_WIDE_FLAG ...]
gcloud spanner databases create $2 \
--instance=$1
