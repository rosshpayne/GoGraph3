aws dynamodb create-table  --table-name  "GoGraph02" --cli-input-json file://tbl-graph.json
#aws dynamodb create-table --cli-input-json file://dgraph-table.7.json
#aws dynamodb create-table --cli-input-json file://dgraph-table-noIndex.7.json
#aws dynamodb create-table --cli-input-json file://dgraph-table.Types.json
#aws dynamodb create-table --cli-input-json file://dgraph-table.event.json
aws dynamodb create-table --cli-input-json file://tbl-runstats.json
# aws dynamodb create-table --table-name "Edge_Relationship" --cli-input-json file://tbl-Edge_.json
# aws dynamodb create-table --table-name "EdgeChild_Relationship" --cli-input-json file://tbl-EdgeChild_.json
aws dynamodb create-table --table-name "Edge_Movies" --cli-input-json file://tbl-Edge_.json
aws dynamodb create-table --table-name "EdgeChild_Movies" --cli-input-json file://tbl-EdgeChild_.json
aws dynamodb create-table --cli-input-json file://tbl-eslog.json
# aws dynamodb create-table --cli-input-json file://tbl-eventlog.json
aws dynamodb create-table --table-name  "pgState"  --cli-input-json file://tbl-pgstate.json
aws dynamodb create-table --cli-input-json file://tbl-state.json
