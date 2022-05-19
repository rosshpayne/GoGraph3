aws dynamodb delete-table --table-name "GoGraph.dev"
sleep 10
aws dynamodb create-table  --table-name  "GoGraph.dev" --cli-input-json file://../json/tbl-graph.json
