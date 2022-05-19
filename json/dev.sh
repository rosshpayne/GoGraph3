aws dynamodb delete-table --table-name "GoGraphDev2"
#aws dynamodb delete-table --table-name "state"
aws dynamodb delete-table --table-name "Edge_Relationship"
aws dynamodb delete-table --table-name "EdgeChild_Relationship"
# aws dynamodb delete-table --table-name "Eventlog2"
sleep 13
aws dynamodb create-table  --table-name  "GoGraphDev2" --cli-input-json file://tbl-graph.json
aws dynamodb create-table --table-name "Edge_Relationship" --cli-input-json file://tbl-Edge_.json
aws dynamodb create-table --table-name "EdgeChild_Relationship" --cli-input-json file://tbl-EdgeChild_.json
#aws dynamodb create-table --table-name "state" --cli-input-json file://tbl-state.json
# aws dynamodb create-table --table-name "EventLog2" --cli-input-json file://tbl-eventlog.json
