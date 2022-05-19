#gcloud spanner databases ddl update (DATABASE : --instance=INSTANCE)
#        [--async] [--ddl=DDL] [--ddl-file=DDL_FILE] [GCLOUD_WIDE_FLAG ...]

gcloud spanner databases ddl update $2 \
--instance=$1 \
--ddl-file=./$3
