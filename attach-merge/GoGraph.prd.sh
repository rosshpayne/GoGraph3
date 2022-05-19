aws dynamodb delete-table --table-name "GoGraph.prd"
sleep 10
aws dynamodb create-table  --table-name  "GoGraph.prd" --cli-input-json file://../json/tbl-graph.json
