aws dynamodb delete-table --table-name "GoGraph.prd.2"
sleep 10
aws dynamodb create-table  --table-name  "GoGraph.prd.2" --cli-input-json file://../json/tbl-graph.json
