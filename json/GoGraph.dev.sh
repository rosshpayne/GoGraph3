aws dynamodb delete-table --table-name "GoGraph.Dev"
sleep 10
aws dynamodb create-table  --table-name  "GoGraph.Dev" --cli-input-json file://tbl-graph.json
